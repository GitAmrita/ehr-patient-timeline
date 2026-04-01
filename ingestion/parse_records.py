"""
parse_records.py

Parses raw EHR text rows into structured Parquet files per entity type.

Dataset structure (per patient record, separated by '===' lines):
  [title chunk]    — single line, document type (only at dataset start)
  [content chunk]  — starts with 'PATIENT INFORMATION', the actual clinical data
  [footer chunk]   — starts with 'END OF REPORT', contains JSON with folder/file metadata

Sections present in content chunks:
  PATIENT INFORMATION, BASELINE VITALS, EXERCISE DATA,
  SYMPTOMS DURING TEST, ECG FINDINGS, INTERPRETATION,
  STRESS ECHOCARDIOGRAPHY FINDINGS, PERFUSION IMAGING, RECOMMENDATIONS

Output (raw/parsed/):
  patients.parquet        — one row per report (demographics + metadata)
  vitals.parquet          — baseline vitals per report
  exercise.parquet        — exercise test metrics per report
  ecg_findings.parquet    — ECG findings per report
  interpretations.parquet — interpretation / clinical conclusion per report
  symptoms.parquet        — symptom flags per report

Usage:
    python ingestion/parse_records.py
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

RAW_FILE = Path(__file__).parent.parent / "raw" / "ehr_raw.parquet"
PARSED_DIR = Path(__file__).parent.parent / "raw" / "parsed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DOC_SEP = re.compile(r"^={10,}$")
SEC_SEP = re.compile(r"^-{10,}$")


# ---------------------------------------------------------------------------
# Step 1: split raw rows into chunks at '===' separators
# ---------------------------------------------------------------------------

def split_chunks(rows: list[str]) -> list[list[str]]:
    chunks, cur = [], []
    for row in rows:
        if DOC_SEP.match(row.strip()):
            if cur:
                chunks.append(cur)
                cur = []
        else:
            cur.append(row)
    if cur:
        chunks.append(cur)
    return [c for c in chunks if any(l.strip() for l in c)]


# ---------------------------------------------------------------------------
# Step 2: pair content + footer chunks into records
# ---------------------------------------------------------------------------

def pair_into_records(chunks: list[list[str]]) -> list[dict]:
    """
    Chunks come in groups: (optional title) | content | footer | content | footer ...
    Content chunks start with 'PATIENT INFORMATION'.
    Footer chunks start with 'END OF REPORT' and contain JSON metadata.
    """
    records = []
    i = 0
    while i < len(chunks):
        non_empty = [l.strip() for l in chunks[i] if l.strip()]
        if not non_empty:
            i += 1
            continue

        first = non_empty[0]

        if first == "PATIENT INFORMATION":
            content = chunks[i]
            footer = chunks[i + 1] if i + 1 < len(chunks) else []
            records.append({"content": content, "footer": footer})
            i += 2  # consume both
        else:
            i += 1  # skip title or unrecognised chunk

    return records


# ---------------------------------------------------------------------------
# Step 3: parse sections within a content chunk
# ---------------------------------------------------------------------------

def parse_sections(lines: list[str]) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current = "_preamble"
    sections[current] = []

    for line in lines:
        s = line.strip()
        if (
            s
            and s == s.upper()
            and len(s) > 3
            and not DOC_SEP.match(s)
            and not SEC_SEP.match(s)
            and ":" not in s
        ):
            current = s
            sections.setdefault(current, [])
        else:
            sections.setdefault(current, []).append(s)

    return sections


def extract_field(lines: list[str], key: str) -> str | None:
    pattern = re.compile(rf"^{re.escape(key)}\s*:\s*(.+)$", re.IGNORECASE)
    for line in lines:
        m = pattern.match(line.strip())
        if m:
            return m.group(1).strip()
    return None


def extract_footer_meta(footer_lines: list[str]) -> dict:
    """Extract folder ID and file type from the JSON block in the footer."""
    text = " ".join(l.strip() for l in footer_lines)
    meta = {"folder_id": None, "file_type": None}
    try:
        json_match = re.search(r"\{.*\}", text)
        if json_match:
            data = json.loads(json_match.group())
            meta["folder_id"] = data.get("folder")
            meta["file_type"] = data.get("file")
    except (json.JSONDecodeError, AttributeError):
        pass
    return meta


# ---------------------------------------------------------------------------
# Step 4: extract entity rows from a parsed record
# ---------------------------------------------------------------------------

def extract_record(record: dict) -> dict:
    doc_id = str(uuid.uuid4())
    sections = parse_sections(record["content"])
    meta = extract_footer_meta(record["footer"])

    pi = sections.get("PATIENT INFORMATION", [])
    vitals = sections.get("BASELINE VITALS", sections.get("BASELINE DATA", []))
    exercise = sections.get("EXERCISE DATA", sections.get("STRESS DATA", sections.get("PHYSIOLOGIC DATA", [])))
    ecg = sections.get(
        "ECG FINDINGS",
        sections.get("ELECTROCARDIOGRAPHIC FINDINGS", sections.get("ECG ANALYSIS", [])),
    )
    interp = sections.get(
        "INTERPRETATION",
        sections.get("IMPRESSION", sections.get("IMPRESSION / DIAGNOSIS", [])),
    )
    symptoms = sections.get("SYMPTOMS DURING TEST", sections.get("SYMPTOMS", []))

    patient_id = extract_field(pi, "Patient ID")
    study_date = extract_field(pi, "Study Date")

    return {
        "doc_id": doc_id,
        "folder_id": meta["folder_id"],
        "file_type": meta["file_type"],
        "patient_id": patient_id,
        "patient_name": extract_field(pi, "Patient Name"),
        "study_date": study_date,
        "study_type": extract_field(pi, "Study Type"),
        "protocol": extract_field(pi, "Protocol"),
        # vitals
        "heart_rate_bpm": extract_field(vitals, "Heart Rate"),
        "blood_pressure": extract_field(vitals, "Blood Pressure"),
        "o2_saturation": extract_field(vitals, "Oxygen Saturation"),
        # exercise
        "exercise_time": extract_field(exercise, "Total Exercise Time"),
        "max_hr_achieved": extract_field(exercise, "Max Heart Rate Achieved"),
        "target_hr": extract_field(exercise, "Target Heart Rate"),
        "mets_achieved": extract_field(exercise, "METs Achieved"),
        "reason_stopped": extract_field(exercise, "Reason for Stopping"),
        # ecg
        "ecg_baseline": extract_field(ecg, "Baseline"),
        "ecg_during_exercise": extract_field(ecg, "During Exercise"),
        "ecg_st_changes": extract_field(ecg, "ST Changes"),
        "ecg_arrhythmias": extract_field(ecg, "Arrhythmias"),
        # interpretation
        "test_result": extract_field(interp, "Test Result"),
        "functional_capacity": extract_field(interp, "Functional Capacity"),
        "interpretation_text": " ".join(l for l in interp if l),
        # symptoms (flagged as present/not present)
        "chest_pain": extract_field(symptoms, "Chest Pain"),
        "shortness_of_breath": extract_field(symptoms, "Shortness of Breath"),
        "dizziness": extract_field(symptoms, "Dizziness"),
    }


# ---------------------------------------------------------------------------
# Step 5: split into entity-specific DataFrames and write Parquet
# ---------------------------------------------------------------------------

PATIENT_COLS = ["doc_id", "folder_id", "file_type", "patient_id", "patient_name",
                "study_date", "study_type", "protocol"]

VITALS_COLS = ["doc_id", "patient_id", "study_date",
               "heart_rate_bpm", "blood_pressure", "o2_saturation"]

EXERCISE_COLS = ["doc_id", "patient_id", "study_date",
                 "exercise_time", "max_hr_achieved", "target_hr",
                 "mets_achieved", "reason_stopped"]

ECG_COLS = ["doc_id", "patient_id", "study_date",
            "ecg_baseline", "ecg_during_exercise", "ecg_st_changes", "ecg_arrhythmias"]

INTERP_COLS = ["doc_id", "patient_id", "study_date",
               "test_result", "functional_capacity", "interpretation_text"]

SYMPTOM_COLS = ["doc_id", "patient_id", "study_date",
                "chest_pain", "shortness_of_breath", "dizziness"]


def run(raw_file: Path = RAW_FILE, parsed_dir: Path = PARSED_DIR) -> None:
    parsed_dir.mkdir(parents=True, exist_ok=True)

    log.info("Reading raw file: %s", raw_file)
    table = pq.read_table(raw_file)
    rows = table.to_pydict()["text"]

    log.info("Splitting %d rows into chunks...", len(rows))
    chunks = split_chunks(rows)
    log.info("Found %d chunks", len(chunks))

    records = pair_into_records(chunks)
    log.info("Paired into %d records", len(records))

    parsed = [extract_record(r) for r in records]
    df = pd.DataFrame(parsed)

    entities = {
        "patients": PATIENT_COLS,
        "vitals": VITALS_COLS,
        "exercise": EXERCISE_COLS,
        "ecg_findings": ECG_COLS,
        "interpretations": INTERP_COLS,
        "symptoms": SYMPTOM_COLS,
    }

    for name, cols in entities.items():
        out = parsed_dir / f"{name}.parquet"
        entity_df = df[cols].dropna(how="all", subset=[c for c in cols if c not in ("doc_id", "patient_id", "study_date")])
        entity_df.to_parquet(out, index=False)
        log.info("Wrote %-20s — %3d rows → %s", name, len(entity_df), out)

    # Sanity check: show a sample patient record
    sample = df[["patient_id", "folder_id", "study_date", "test_result", "mets_achieved"]].head(3)
    print("\nSample records:")
    print(sample.to_string(index=False))


if __name__ == "__main__":
    run()
