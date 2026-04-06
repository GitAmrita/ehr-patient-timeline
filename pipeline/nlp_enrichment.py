"""
nlp_enrichment.py

Runs Claude over clinical notes to extract structured medical entities.
Reads from raw/parsed/notes.parquet, writes to raw/parsed/nlp_entities.parquet.

Design decisions:
- Uses Claude tool use to enforce a strict JSON output schema
- Checkpointed: tracks processed doc ids in a local cache file so re-runs
  skip already-processed notes (safe to interrupt and resume)
- One note per API call — simple to debug, easy to see per-note cost
- Haiku model: fast and cheap for bulk extraction over 600+ documents

Output schema (one row per entity per note):
    folder_id, patient_id, note_date, note_type,
    entity_type, entity_value, confidence

Usage:
    python pipeline/nlp_enrichment.py
    python pipeline/nlp_enrichment.py --limit 10   # process first 10 notes only (for testing)
    python pipeline/nlp_enrichment.py --reset       # clear cache and reprocess everything
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path

import anthropic
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

NOTES_FILE = Path(__file__).parent.parent / "raw" / "parsed" / "notes.parquet"
OUTPUT_FILE = Path(__file__).parent.parent / "raw" / "parsed" / "nlp_entities.parquet"
CACHE_FILE = Path(__file__).parent.parent / "raw" / "parsed" / ".nlp_cache.json"

MODEL = "claude-haiku-4-5-20251001"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tool definition — enforces structured JSON output from Claude
# ---------------------------------------------------------------------------

EXTRACTION_TOOL = {
    "name": "extract_clinical_entities",
    "description": "Extract structured clinical entities from a medical document.",
    "input_schema": {
        "type": "object",
        "properties": {
            "diagnoses": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Medical conditions and diagnoses mentioned (e.g. 'Type 2 Diabetes', 'Heart Failure')",
            },
            "medications": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Medications with dosages if present (e.g. 'Furosemide 40mg daily')",
            },
            "procedures": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Tests, procedures, and imaging performed (e.g. 'Echocardiogram', 'Chest X-ray')",
            },
            "key_findings": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Notable clinical findings and results (e.g. 'EF 35%', 'bilateral pleural effusion')",
            },
            "follow_up": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Recommended follow-up actions and next steps",
            },
        },
        "required": ["diagnoses", "medications", "procedures", "key_findings", "follow_up"],
    },
}

SYSTEM_PROMPT = """You are a clinical NLP system. Extract structured medical entities from the following clinical document.

Be precise and concise. Only extract entities explicitly mentioned in the text.
Do not infer or add information not present in the document."""

USER_TEMPLATE = """<document>
{note_text}
</document>"""


# ---------------------------------------------------------------------------
# Cache — tracks which doc_ids have already been processed
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text())
    return {}


def save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache))


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------

def extract_entities(client: anthropic.Anthropic, note_text: str) -> dict:
    """Call Claude with tool use to extract clinical entities from a note."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        tools=[{**EXTRACTION_TOOL, "cache_control": {"type": "ephemeral"}}],
        tool_choice={"type": "tool", "name": "extract_clinical_entities"},
        messages=[
            {"role": "user", "content": USER_TEMPLATE.format(note_text=note_text[:3000])}
        ],
    )

    # Tool use response — input is the structured JSON we want
    for block in response.content:
        if block.type == "tool_use":
            return block.input

    return {}


# ---------------------------------------------------------------------------
# Flatten entities into rows (one row per entity value)
# ---------------------------------------------------------------------------

def flatten_entities(row: pd.Series, entities: dict) -> list[dict]:
    """Convert extracted entity dict into one row per entity value."""
    base = {
        "folder_id":  row["folder_id"],
        "patient_id": row["patient_id"],
        "note_date":  row["note_date"],
        "note_type":  row["note_type"],
    }
    rows = []
    for entity_type, values in entities.items():
        for value in values:
            if value and value.strip():
                rows.append({**base, "entity_type": entity_type, "entity_value": value.strip()})
    return rows


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run(limit: int | None = None, reset: bool = False) -> None:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set — check your .env file")

    client = anthropic.Anthropic(api_key=api_key)

    notes_df = pd.read_parquet(NOTES_FILE)
    if limit:
        notes_df = notes_df.head(limit)

    log.info("Loaded %d notes to process", len(notes_df))

    cache = {} if reset else load_cache()
    if reset and CACHE_FILE.exists():
        CACHE_FILE.unlink()
        log.info("Cache cleared — reprocessing all notes")

    all_entities: list[dict] = []

    # Load existing output to preserve already-processed rows
    if OUTPUT_FILE.exists() and not reset:
        all_entities = pd.read_parquet(OUTPUT_FILE).to_dict("records")
        log.info("Loaded %d existing entity rows from previous run", len(all_entities))

    processed = skipped = failed = 0

    for i, row in notes_df.iterrows():
        # Use folder_id + note_type as cache key (unique per document)
        cache_key = f"{row['folder_id']}:{row['note_type']}"

        if cache_key in cache:
            skipped += 1
            continue

        note_text = row.get("note_text", "")
        if not note_text or len(str(note_text).strip()) < 50:
            cache[cache_key] = "skipped_empty"
            skipped += 1
            continue

        try:
            entities = extract_entities(client, str(note_text))
            rows = flatten_entities(row, entities)
            all_entities.extend(rows)
            cache[cache_key] = "done"
            processed += 1

            if processed % 10 == 0:
                # Checkpoint — save progress every 10 notes
                pd.DataFrame(all_entities).to_parquet(OUTPUT_FILE, index=False)
                save_cache(cache)
                log.info("Progress: %d processed, %d skipped, %d failed", processed, skipped, failed)

            # Polite rate limiting
            time.sleep(0.2)

        except Exception as e:
            log.warning("Failed on %s: %s", cache_key, e)
            cache[cache_key] = f"error:{e}"
            failed += 1

    # Final write
    if all_entities:
        df = pd.DataFrame(all_entities)
        df.to_parquet(OUTPUT_FILE, index=False)
        save_cache(cache)
        log.info("Done. Wrote %d entity rows to %s", len(df), OUTPUT_FILE)
        log.info("Summary: %d processed, %d skipped, %d failed", processed, skipped, failed)

        print("\nEntity type breakdown:")
        print(df["entity_type"].value_counts().to_string())

        print("\nSample entities:")
        print(df[["folder_id", "patient_id", "entity_type", "entity_value"]].head(10).to_string(index=False))
    else:
        log.warning("No entities extracted — check notes.parquet has content")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract clinical entities from notes using Claude")
    parser.add_argument("--limit", type=int, default=None, help="Process only first N notes (for testing)")
    parser.add_argument("--reset", action="store_true", help="Clear cache and reprocess everything")
    args = parser.parse_args()

    run(limit=args.limit, reset=args.reset)
