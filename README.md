# EHR Patient Timeline

A full data pipeline and REST API over synthetic multi-file EHR data, built as a backend/data engineering portfolio project.

**Stack:** Python · DuckDB · dbt · FastAPI · Claude AI · React

---

## Architecture

```
HuggingFace Dataset (synthetic EHR, 1000 patients)
        │
        ▼
ingestion/load_raw.py          — downloads patient folders
ingestion/parse_records.py     — classifies files, parses fields → Parquet
        │
        ▼
pipeline/nlp_enrichment.py     — Claude Haiku (offline, pipeline step)
        │
        ▼
scripts/setup_db.py            — registers Parquet files as DuckDB views
        │
        ▼
dbt (staging → marts)          — normalise, aggregate, build timeline
        │
        ▼
FastAPI + React                — patient search + per-folder timeline UI
```

---

## Claude API Integration

Clinical notes are unstructured text. The NLP enrichment pipeline uses **Claude Haiku** to extract structured clinical entities before the data enters the warehouse.

### How it works

`pipeline/nlp_enrichment.py` runs as an **offline pipeline step** — it is called once during data preparation, not on API requests or page load. This keeps the API fast and deterministic, and means Claude's output is persisted and version-controlled alongside the rest of the data.

Claude is called using **tool use** (structured output mode), which forces a defined JSON schema rather than free-form text. The extraction tool schema captures:

- `diagnoses` — active conditions mentioned in the note
- `medications` — drugs with dosage where available
- `procedures` — treatments or interventions performed
- `key_findings` — abnormal values or clinical observations
- `follow_up` — next steps or referrals documented

### Guarding against hallucination — Grounded Generation

LLM output in a clinical context requires verifiable, auditable extraction. This pipeline implements a **grounded generation** pattern: Claude is required to cite the exact source text that supports each entity it extracts. If it cannot find supporting text in the document, it must return `source_text: null` — it is explicitly instructed not to fabricate a citation.

This means hallucination detection is built into the output schema itself:

```json
{
  "value": "Type 2 Diabetes",
  "is_inferred": false,
  "source_text": "Patient has a longstanding history of Type 2 Diabetes mellitus."
}
```

| Field | Purpose |
|---|---|
| `source_text` | Exact quote from the note. **Null = Claude could not ground the entity in the document.** |
| `is_inferred` | True if the entity is implied (e.g. inferred from a medication) rather than explicitly stated |

A dbt singular test (`dbt/tests/assert_nlp_entities_grounded.sql`) runs after every pipeline execution and **fails if any non-inferred entity has a null `source_text`**. Any failures are surfaced as rows identifying the folder, note, and entity for manual review.

Full validation stack:

| Measure | Status |
|---|---|
| **Tool use** — Claude must return structured JSON; cannot invent fields outside the schema | Implemented |
| **Grounded extraction** — system prompt requires a source citation for every entity | Implemented |
| **`source_text` null check** — dbt test flags entities Claude could not ground in the document | Implemented |
| **`is_inferred` flag** — distinguishes directly stated vs. implied entities for downstream consumers | Implemented |
| **Checkpointed pipeline** — `.nlp_cache.json` makes runs idempotent and outputs auditable | Implemented |

The core principle: **Claude reads and extracts, it does not reason or generate.** All clinical judgement stays in the source documents.

---

## Setup

### 1. Clone and create a virtual environment

```bash
git clone https://github.com/GitAmrita/ehr-patient-timeline.git
cd ehr-patient-timeline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Download and parse patient data

```bash
python -m ingestion.load_raw          # 30 sample patients
python -m ingestion.load_raw --full   # all 1000
python -m ingestion.parse_records
```

### 3. Run NLP enrichment (requires Anthropic API key)

```bash
export ANTHROPIC_API_KEY=your_key_here
python -m pipeline.nlp_enrichment --limit 20
```

This step is optional — the rest of the pipeline runs without it.

### 4. Build the warehouse

```bash
python scripts/setup_db.py
dbt run
```

### 5. Start the API

```bash
uvicorn api.main:app --reload
# Docs at http://localhost:8000/docs
```

### 6. Start the UI

```bash
cd ui && npm install && npm run dev
# UI at http://localhost:5173
```

---

## API Endpoints

### `GET /patients/{patient_id}`
Returns all patient records matching the MRN. Multiple results mean the same ID appears across different source folders.

### `GET /patients/{patient_id}/timeline`
Returns events grouped by folder, each with its own `event_count`. Supports `event_type`, `from_date`, and `to_date` query filters.

---

## Key Design Decisions

- **Claude called offline, not at request time** — enrichment runs once in the pipeline; the API serves pre-extracted, validated data
- **Tool use for structured extraction** — eliminates free-form hallucination risk by constraining output to a defined schema
- **DuckDB + dbt** — serverless local warehouse with testable, documented SQL transformations
- **Per-folder timeline grouping** — each folder is a distinct patient record; event counts are scoped accordingly
- **Duplicate MRN handling** — the API returns all matching folders rather than arbitrarily picking one
