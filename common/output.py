"""
output.py — JSONL writer, run_id generation, timestamp helper.

Output path convention: scraper/output/{source}_{run_id}.jsonl
"""

import json
import uuid
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


# Output directory relative to project root.
# Override by passing an explicit path to write_jsonl().
_DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def make_run_id() -> str:
    """
    Generate a run ID: 8-char UUID4 hex prefix + compact UTC timestamp.
    Example: "a3f2c91b_20260417T143022"
    Unique enough for single-node use; not guaranteed globally unique.
    """
    hex_prefix = uuid.uuid4().hex[:8]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{hex_prefix}_{ts}"


def now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def write_jsonl(
    jobs: Iterator[dict],
    source: str,
    run_id: str,
    output_dir: Path | None = None,
    progress_every: int = 10,
) -> tuple[int, Path]:
    """
    Consume a job iterator and write each job as a newline-delimited JSON record.

    Args:
        jobs:           iterator of Job dicts (from any scraper)
        source:         source site name, e.g. "dice", "jobright", "linkedin"
        run_id:         run identifier from make_run_id()
        output_dir:     directory to write into (default: scraper/output/)
        progress_every: print progress to stderr every N jobs (0 = silent)

    Returns:
        (count_written, output_path)

    Side effects:
        - Creates output_dir if it doesn't exist
        - Writes one JSON object per line to the output file
        - Prints progress + final summary to stderr
    """
    if output_dir is None:
        output_dir = _DEFAULT_OUTPUT_DIR

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{source}_{run_id}.jsonl"

    count = 0
    with out_path.open("w", encoding="utf-8") as fh:
        for job in jobs:
            fh.write(json.dumps(job, ensure_ascii=False) + "\n")
            count += 1
            if progress_every > 0 and count % progress_every == 0:
                _progress(f"[{source}] {count} jobs written...")

    _progress(f"[{source}] Done. {count} jobs → {out_path}")
    return count, out_path


def read_jsonl(path: Path) -> list[dict]:
    """
    Read a JSONL file and return a list of dicts.
    Used by the Node pipeline script and fixtures loader.
    """
    jobs = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                jobs.append(json.loads(line))
    return jobs


def _progress(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)