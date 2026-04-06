"""
load_raw.py

Downloads patient folders from the HuggingFace EHR dataset into raw/patient-samples/.
Each patient is a folder containing multiple .txt and .md files whose names vary per patient.

Usage:
    python ingestion/load_raw.py            # download 30 patients (default, for development)
    python ingestion/load_raw.py --full     # download all 1000 patients
    python ingestion/load_raw.py --sample 50
"""

import argparse
import logging
from collections import defaultdict
from pathlib import Path

from huggingface_hub import hf_hub_download, list_repo_tree, snapshot_download

DATASET_NAME = "dataframer/ehr-multi-file-patient-samples"
RAW_DIR = Path(__file__).parent.parent / "raw"
SAMPLES_DIR = RAW_DIR / "patient-samples"
DEFAULT_SAMPLE = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def list_patient_folders() -> list[str]:
    """Return all patient folder names from the repo (e.g. ['folder_1', 'folder_2', ...])."""
    items = list_repo_tree(
        repo_id=DATASET_NAME,
        repo_type="dataset",
        path_in_repo="patient-samples",
    )
    return sorted(
        item.path.split("/")[-1]
        for item in items
        if item.path != "patient-samples" and item.path.split("/")[-1].startswith("folder_")
    )


def download_sample(n: int, samples_dir: Path = SAMPLES_DIR) -> None:
    """Download the first n patient folders by fetching files individually."""
    samples_dir.mkdir(parents=True, exist_ok=True)

    log.info("Listing patient folders from HuggingFace...")
    all_folders = list_patient_folders()
    selected = all_folders[:n]
    log.info("Downloading %d of %d patient folders...", n, len(all_folders))

    for i, folder_name in enumerate(selected, 1):
        folder_dir = samples_dir / folder_name
        if folder_dir.exists() and folder_dir.is_dir() and any(folder_dir.iterdir()):
            log.info("[%d/%d] %s — already exists, skipping", i, n, folder_name)
            continue

        folder_dir.mkdir(exist_ok=True)
        # List files within this patient folder
        files = list_repo_tree(
            repo_id=DATASET_NAME,
            repo_type="dataset",
            path_in_repo=f"patient-samples/{folder_name}",
        )
        for file_item in files:
            file_path = file_item.path  # e.g. patient-samples/folder_1/demographics.md
            file_name = file_path.split("/")[-1]
            if file_name.endswith(".metadata"):
                continue
            hf_hub_download(
                repo_id=DATASET_NAME,
                repo_type="dataset",
                filename=file_path,
                local_dir=str(RAW_DIR),
            )
        log.info("[%d/%d] %s — done", i, n, folder_name)

    log.info("Download complete → %s", samples_dir)


def download_full(samples_dir: Path = SAMPLES_DIR) -> None:
    """Download all patient folders using snapshot_download (faster for large pulls)."""
    if samples_dir.exists() and any(samples_dir.iterdir()):
        log.info("patient-samples/ already exists — skipping. Delete folder to re-fetch.")
        return

    log.info("Downloading ALL patient folders from '%s'...", DATASET_NAME)
    snapshot_download(
        repo_id=DATASET_NAME,
        repo_type="dataset",
        local_dir=str(RAW_DIR),
        allow_patterns="patient-samples/**",
        ignore_patterns="*.metadata",
        max_workers=8,
    )
    log.info("Download complete → %s", samples_dir)


def inspect(samples_dir: Path = SAMPLES_DIR) -> None:
    """Print a summary: patient count, file extensions, unique file names, sample inventories.

    Runs automatically after every download as a sanity check — lets you see what was
    fetched without manually browsing the folders. Useful for spotting missing files or
    unexpected structural variation across patients.
    """
    patient_folders = sorted(p for p in samples_dir.iterdir() if p.is_dir())

    file_name_counts: dict[str, int] = defaultdict(int)
    ext_counts: dict[str, int] = defaultdict(int)

    for folder in patient_folders:
        for f in folder.iterdir():
            if f.is_file() and not f.name.endswith(".metadata"):
                file_name_counts[f.name] += 1
                ext_counts[f.suffix] += 1

    print(f"\nPatients downloaded: {len(patient_folders)}")

    print("\nFile extensions:")
    for ext, count in sorted(ext_counts.items(), key=lambda x: -x[1]):
        print(f"  {ext or '(none)':10s}  {count:5d} files")

    print(f"\nUnique file names ({len(file_name_counts)} total), by frequency:")
    for name, count in sorted(file_name_counts.items(), key=lambda x: -x[1])[:40]:
        print(f"  {count:4d}x  {name}")

    print("\nSample patient inventories:")
    for folder in patient_folders[:5]:
        files = sorted(f.name for f in folder.iterdir()
                       if f.is_file() and not f.name.endswith(".metadata"))
        print(f"  {folder.name}: {files}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download EHR patient folders from HuggingFace")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--full", action="store_true", help="Download all 1000 patients")
    group.add_argument("--sample", type=int, default=DEFAULT_SAMPLE,
                       help=f"Number of patients to download (default: {DEFAULT_SAMPLE})")
    args = parser.parse_args()

    if args.full:
        download_full()
    else:
        download_sample(args.sample)

    inspect()
