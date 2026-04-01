"""
load_raw.py

Downloads the HuggingFace EHR dataset and lands it as Parquet files
in the raw/ directory. This layer is append-only — no transformations.

Usage:
    python ingestion/load_raw.py
"""

import logging
from pathlib import Path

from datasets import load_dataset
import pyarrow.parquet as pq

RAW_DIR = Path(__file__).parent.parent / "raw"
DATASET_NAME = "dataframer/ehr-multi-file-patient-samples"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def download_and_land(raw_dir: Path = RAW_DIR) -> None:
    raw_dir.mkdir(exist_ok=True)
    out_path = raw_dir / "ehr_raw.parquet"

    if out_path.exists():
        log.info("Raw file already exists at %s — skipping download. Delete to re-fetch.", out_path)
        return

    log.info("Downloading dataset '%s' from HuggingFace...", DATASET_NAME)
    dataset = load_dataset(DATASET_NAME, split="test")

    log.info("Downloaded %d rows. Writing to %s...", len(dataset), out_path)
    dataset.to_parquet(str(out_path))

    log.info("Done. Raw file: %s (%.1f KB)", out_path, out_path.stat().st_size / 1024)


def inspect(raw_dir: Path = RAW_DIR) -> None:
    """Print schema and a sample row for a quick sanity check."""
    out_path = raw_dir / "ehr_raw.parquet"
    if not out_path.exists():
        log.error("Raw file not found. Run download_and_land() first.")
        return

    table = pq.read_table(out_path)
    print("\n--- Schema ---")
    print(table.schema)
    print(f"\n--- Row count: {table.num_rows} ---")
    print("\n--- Sample row ---")
    print(table.slice(0, 1).to_pydict())


if __name__ == "__main__":
    download_and_land()
    inspect()
