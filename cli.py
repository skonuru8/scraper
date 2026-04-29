"""
cli.py — scraper CLI entry point.

Usage:
    python -m scraper --source dice --max 50
    python -m scraper --source dice --max 50 --posted-within ONE
    python -m scraper --source jobright --max 50 --headed
    python -m scraper --source jobright_api --max 50
    python -m scraper --source linkedin --max 90 --hours-old 24

Exit codes:
    0  success
    1  adapter error (scrape failed)
    2  cookie file missing

v4.1 changes:
    - Added --posted-within for Dice. Maps to filters.postedDate query param.
      Values: ONE (24h), THREE (3d), SEVEN (7d). Other sources ignore it.
"""

import argparse
import sys
from pathlib import Path

from scraper.common.output import make_run_id, write_jsonl

# Default cookies directory — relative to project root
_COOKIES_DIR = Path("config/cookies")

# Dice's postedDate filter values. Mirrors POSTED_WITHIN_VALUES in dice.py.
_POSTED_WITHIN_CHOICES = ["ONE", "THREE", "SEVEN"]


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scraper",
        description="Scrape job listings from Dice, Jobright, or LinkedIn.",
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=["dice", "jobright", "jobright_api", "linkedin"],
        help="Which source to scrape.",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=50,
        dest="max_jobs",
        help="Max jobs to collect (default: 50).",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        default=False,
        help="Show browser window (Playwright sources only).",
    )
    parser.add_argument(
        "--cookies",
        type=Path,
        default=None,
        help="Override cookie file path (default: config/cookies/{source}.json).",
    )
    parser.add_argument(
        "--hours-old",
        type=int,
        default=72,
        dest="hours_old",
        help="LinkedIn only: max age of postings in hours (default: 72).",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="java developer",
        help="Dice only: search query string (default: 'java developer').",
    )
    parser.add_argument(
        "--posted-within",
        type=str,
        default=None,
        choices=_POSTED_WITHIN_CHOICES,
        dest="posted_within",
        help=(
            "Dice only: server-side recency filter. "
            "ONE=last 24h, THREE=last 3 days, SEVEN=last 7 days. "
            "Default (omitted): no filter, scrape all listings. "
            "Use ONE for cron runs, SEVEN for backfill."
        ),
    )

    args = parser.parse_args()

    # Resolve cookies path
    cookies_path: Path = args.cookies or (_COOKIES_DIR / f"{args.source}.json")

    # Only Jobright sources need cookies now; Dice uses public search, LinkedIn uses JobSpy
    if args.source in ("jobright", "jobright_api"):
        if not cookies_path.exists():
            print(
                f"Error: Cookie file not found at {cookies_path}\n"
                f"Export cookies from your browser and save to {cookies_path}.\n"
                "See instructions.md Step 0 for details.\n"
                "NEVER commit cookie files to git.",
                file=sys.stderr,
            )
            return 2

    # Generate run ID
    run_id = make_run_id()
    print(f"[scraper] run_id={run_id} source={args.source} max={args.max_jobs}",
          file=sys.stderr)

    # Load adapter
    try:
        jobs_iter = _get_adapter(args, cookies_path, run_id)
    except Exception as e:
        print(f"[scraper] Failed to initialize adapter: {e}", file=sys.stderr)
        return 1

    # Stream to JSONL
    try:
        count, out_path = write_jsonl(
            jobs=jobs_iter,
            source=args.source,
            run_id=run_id,
            progress_every=10,
        )
    except FileNotFoundError as e:
        # Cookie file disappeared between check and use
        print(f"[scraper] Cookie error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"[scraper] Adapter error: {e}", file=sys.stderr)
        return 1

    print(f"[scraper] {count} jobs → {out_path}", file=sys.stderr)
    return 0


def _get_adapter(args, cookies_path: Path, run_id: str):
    """Return a jobs iterator from the appropriate adapter."""
    if args.source == "dice":
        from scraper.dice import scrape
        return scrape(
            max_jobs=args.max_jobs,
            run_id=run_id,
            query=args.query,
            headless=not args.headed,
            posted_within=args.posted_within,
        )

    if args.source == "jobright":
        from scraper.jobright import scrape
        return scrape(
            max_jobs=args.max_jobs,
            run_id=run_id,
            cookies_path=cookies_path,
            headless=not args.headed,
        )

    if args.source == "jobright_api":
        from scraper.jobright_api import scrape
        return scrape(
            max_jobs=args.max_jobs,
            run_id=run_id,
            cookies_path=cookies_path,
            sort_condition=1,
        )

    if args.source == "linkedin":
        from scraper.jobspy_adapter import scrape
        return scrape(
            max_jobs=args.max_jobs,
            run_id=run_id,
            cookies_path=cookies_path,
            hours_old=args.hours_old,
        )

    raise ValueError(f"Unknown source: {args.source}")


if __name__ == "__main__":
    sys.exit(main())