"""
parse_records.py

Walks raw/patient-samples/ and parses each patient's files into
entity-specific Parquet files under raw/parsed/.

File classification uses keyword matching on filenames since names vary per patient.
Content parsing handles markdown format: **Key:** Value fields and | table | rows.

Output (raw/parsed/):
    patients.parquet    — demographics, one row per patient (from patient_summary files)
    encounters.parquet  — ED visits, discharges, ICU admissions (one row per document)
    labs.parquet        — individual lab test results (one row per test)
    notes.parquet       — clinical notes, consults, imaging reports (one row per document)

Usage:
    python ingestion/parse_records.py
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

SAMPLES_DIR = Path(__file__).parent.parent / "raw" / "patient-samples"
PARSED_DIR = Path(__file__).parent.parent / "raw" / "parsed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File classification — keyword sets per entity type
# ---------------------------------------------------------------------------

ENTITY_KEYWORDS: dict[str, list[str]] = {
    "summary":   ["patient_summary", "patient_profile"],
    "labs":      ["lab_result", "lab_report", "sepsis_lab", "cardiac_lab",
                  "chemistry_panel", "metabolic_panel", "comprehensive_lab",
                  "comprehensive_metabolic", "urinalysis", "blood_panel"],
    "imaging":   ["chest_xray", "chest_x_ray", "echocardiogram", "echo_report",
                  "renal_ultrasound", "imaging", "radiology", "xray"],
    "ecg":       ["ecg_report", "ecg", "ekg"],
    "cardiology":["cardiology_consult", "cardiac_cath", "cardiology_eval"],
    "encounter": ["ed_note", "ed_notes", "ed_clinical_note", "ed_admission",
                  "ed_triage", "discharge_summary", "discharge_instruction",
                  "icu_admission", "admission_note", "instructions"],
    "consult":   ["consult", "report", "ophthalmology", "nephrology",
                  "nutrition", "surgical", "endo", "endocrine", "endocrinology"],
}


def classify_file(filename: str) -> str:
    """Return entity type for a given filename based on keyword matching."""
    stem = Path(filename).stem.lower()
    for entity, keywords in ENTITY_KEYWORDS.items():
        if any(kw in stem for kw in keywords):
            return entity
    return "other"


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------

# Matches **Key:** Value or **Key:** Value (bold markdown fields)
BOLD_FIELD = re.compile(r"\*\*([^*]+?)\*\*[:\s]+(.+)")

# Matches markdown table rows: | cell | cell | ...
TABLE_ROW = re.compile(r"^\|(.+)\|$")

# Date patterns
DATE_PATTERN = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4}"
    r"|\d{4}-\d{2}-\d{2}",
    re.IGNORECASE,
)


def extract_bold_field(text: str, *keys: str) -> str | None:
    """Extract the first matching **Key:** value for any of the given keys."""
    for line in text.splitlines():
        m = BOLD_FIELD.search(line)
        if m:
            field_name = m.group(1).strip().lower()
            for key in keys:
                if key.lower() in field_name:
                    return m.group(2).strip()
    return None


def extract_date(text: str) -> str | None:
    """Extract the first date-like string from the text."""
    m = DATE_PATTERN.search(text)
    return m.group(0).strip() if m else None


def extract_patient_id(text: str) -> str | None:
    """Extract patient MRN / Patient ID from markdown text.
    Matches specific ID fields only — not plain 'Patient:' which holds the name."""
    return extract_bold_field(text, "mrn", "patient id", "medical record number", "medical record")


def extract_patient_name(text: str) -> str | None:
    """Extract patient name — matches 'Name:' or 'Patient:' but not 'Patient ID:'."""
    for line in text.splitlines():
        m = BOLD_FIELD.search(line)
        if m:
            field = m.group(1).strip().lower().rstrip(":")
            # 'name' alone, or 'patient' alone (not 'patient id' / 'patient mrn')
            if field == "name" or field == "patient":
                return m.group(2).strip()
    return None


# ---------------------------------------------------------------------------
# Lab table parser
# ---------------------------------------------------------------------------

def parse_lab_tables(text: str) -> list[dict]:
    """
    Parse markdown tables into individual lab test rows.
    Expects tables with a 'Parameter' column and a 'Result' column.
    Returns list of dicts with keys: test_name, result, reference_range, status.
    """
    rows = []
    lines = text.splitlines()
    header: list[str] | None = None

    for line in lines:
        m = TABLE_ROW.match(line.strip())
        if not m:
            header = None
            continue

        cells = [c.strip() for c in m.group(1).split("|")]

        # Detect header row
        if any(c.lower() in ("parameter", "test", "analyte") for c in cells):
            header = [c.lower() for c in cells]
            continue

        # Skip separator rows (---|---|---)
        if all(re.match(r"^-+$", c) or c == "" for c in cells):
            continue

        if header is None or len(cells) < 2:
            continue

        def get_col(names: list[str]) -> str:
            for name in names:
                for i, h in enumerate(header):
                    if name in h and i < len(cells):
                        return re.sub(r"\*+", "", cells[i]).strip()
            return ""

        test_name = get_col(["parameter", "test", "analyte"])
        result = get_col(["result", "value"])
        ref_range = get_col(["reference", "range", "normal"])
        status = get_col(["status", "flag", "interpretation"])

        if test_name and result:
            rows.append({
                "test_name": test_name,
                "result": result,
                "reference_range": ref_range or None,
                "status": status or None,
            })

    return rows


# ---------------------------------------------------------------------------
# Per-entity parsers
# ---------------------------------------------------------------------------

def parse_summary(folder_id: str, text: str) -> dict:
    return {
        "folder_id": folder_id,
        "patient_id": extract_patient_id(text),
        "patient_name": extract_patient_name(text),
        "age": extract_bold_field(text, "age"),
        "gender": extract_bold_field(text, "gender", "sex"),
        "dob": extract_bold_field(text, "dob", "date of birth"),
        "address": extract_bold_field(text, "address"),
        "insurance": extract_bold_field(text, "primary", "insurance"),
        "allergies": extract_bold_field(text, "allerg"),
        "presenting_complaint": _extract_section(text, "PRESENTING COMPLAINT", "CURRENT SYMPTOMS"),
    }


def parse_encounter(folder_id: str, text: str, file_type: str) -> dict:
    return {
        "folder_id": folder_id,
        "patient_id": extract_patient_id(text),
        "encounter_date": extract_date(text),
        "encounter_type": file_type,
        "attending": extract_bold_field(text, "attending", "physician", "provider"),
        "chief_complaint": extract_bold_field(text, "chief complaint"),
        "disposition": extract_bold_field(text, "disposition", "case status"),
        "note_text": text[:2000],  # first 2000 chars for searchability
    }


def parse_labs(folder_id: str, text: str) -> list[dict]:
    patient_id = extract_patient_id(text)
    collection_date = extract_date(text)
    lab_rows = parse_lab_tables(text)
    return [
        {
            "folder_id": folder_id,
            "patient_id": patient_id,
            "collection_date": collection_date,
            **row,
        }
        for row in lab_rows
    ]


def parse_note(folder_id: str, text: str, file_type: str) -> dict:
    return {
        "folder_id": folder_id,
        "patient_id": extract_patient_id(text),
        "note_date": extract_date(text),
        "note_type": file_type,
        "note_text": text[:3000],
    }


# ---------------------------------------------------------------------------
# Section extraction helper
# ---------------------------------------------------------------------------

def _extract_section(text: str, start_heading: str, end_heading: str | None = None) -> str | None:
    """Extract text between two markdown section headings."""
    lines = text.splitlines()
    capturing = False
    captured = []
    for line in lines:
        clean = line.strip().lstrip("#").strip()
        if start_heading.lower() in clean.lower():
            capturing = True
            continue
        if capturing:
            if end_heading and end_heading.lower() in clean.lower():
                break
            if re.match(r"^#+\s", line) and captured:
                break
            captured.append(line)
    return " ".join(captured).strip() or None


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run(samples_dir: Path = SAMPLES_DIR, parsed_dir: Path = PARSED_DIR) -> None:
    parsed_dir.mkdir(parents=True, exist_ok=True)

    patient_folders = sorted(p for p in samples_dir.iterdir() if p.is_dir())
    log.info("Processing %d patient folders...", len(patient_folders))

    summaries, encounters, all_labs, notes = [], [], [], []
    skipped = 0

    for folder in patient_folders:
        folder_id = folder.name
        for f in sorted(folder.iterdir()):
            if not f.is_file() or f.suffix not in (".md", ".txt") or f.name.endswith(".metadata"):
                continue

            text = f.read_text(encoding="utf-8", errors="replace")
            entity = classify_file(f.name)

            if entity == "summary":
                summaries.append(parse_summary(folder_id, text))

            elif entity == "encounter":
                encounters.append(parse_encounter(folder_id, text, f.stem))

            elif entity == "labs":
                lab_rows = parse_labs(folder_id, text)
                all_labs.extend(lab_rows)

            elif entity in ("imaging", "ecg", "cardiology", "consult", "other"):
                notes.append(parse_note(folder_id, text, f.stem))

            else:
                skipped += 1

    # Write Parquet files
    results = {
        "patients":   pd.DataFrame(summaries),
        "encounters": pd.DataFrame(encounters),
        "labs":       pd.DataFrame(all_labs),
        "notes":      pd.DataFrame(notes),
    }

    for name, df in results.items():
        out = parsed_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        log.info("Wrote %-15s — %4d rows → %s", name, len(df), out)

    if skipped:
        log.warning("Skipped %d files with unhandled entity type", skipped)

    # Sanity check
    if not summaries:
        log.warning("No patient_summary files found — patients.parquet will be empty")
        return

    sample = results["patients"][["folder_id", "patient_id", "patient_name", "age", "gender"]].head(5)
    print("\nSample patients:")
    print(sample.to_string(index=False))

    if not all_labs:
        log.warning("No lab rows extracted — check lab file parsing")
    else:
        sample_labs = results["labs"][["folder_id", "patient_id", "test_name", "result", "status"]].head(5)
        print("\nSample labs:")
        print(sample_labs.to_string(index=False))


if __name__ == "__main__":
    run()
