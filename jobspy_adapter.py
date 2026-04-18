"""
jobspy_adapter.py — LinkedIn scraper via the python-jobspy library.

JobSpy handles LinkedIn's scraping internally (no Playwright, no cookies needed).
We run 3 sequential searches, dedup by source_url, cap at max_jobs.

Install: pip install python-jobspy
"""

import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from scraper.common.normalize import (
    guess_seniority,
    parse_employment_type,
    parse_location,
)
from scraper.common.output import now_iso
from scraper.common.schema import FLAGS, add_flag, make_empty_job

DEFAULT_SEARCHES = [
    "senior full stack developer",
    "senior java engineer",
    "senior backend engineer",
]

DEFAULT_PARAMS = {
    "site_name": ["linkedin"],
    "location": "New York, NY",
    "results_wanted": 30,
    "hours_old": 72,
    "is_remote": None,
    "distance": 50,
    "linkedin_fetch_description": False,  # listing only; fetcher fills description_raw later
}


def scrape(
    max_jobs: int,
    run_id: str,
    cookies_path: Path | None = None,    # ignored — JobSpy handles its own session
    search_terms: list[str] | None = None,
    location: str = "New York, NY",
    hours_old: int = 72,
) -> Iterator[dict]:
    """
    Scrape LinkedIn via JobSpy for senior engineering roles.

    Runs search_terms sequentially (not parallel — rate limit safety).
    Deduplicates across searches by source_url.
    Yields Job dicts one at a time up to max_jobs.

    Args:
        max_jobs:      stop after this many unique jobs
        run_id:        from output.make_run_id()
        cookies_path:  ignored (kept for CLI uniformity with Playwright scrapers)
        search_terms:  override default 3 searches
        location:      JobSpy location string
        hours_old:     only include jobs posted within this many hours

    Raises:
        ImportError: if python-jobspy is not installed
    """
    try:
        from jobspy import scrape_jobs
    except ImportError:
        raise ImportError(
            "python-jobspy not installed. Run: pip install python-jobspy"
        )

    terms = search_terms or DEFAULT_SEARCHES
    scraped_at = now_iso()
    seen_urls: set[str] = set()
    yielded = 0

    params = {
        **DEFAULT_PARAMS,
        "location": location,
        "hours_old": hours_old,
    }

    for term in terms:
        if yielded >= max_jobs:
            break

        _err(f"[linkedin] Searching: '{term}' (location={location}, hours_old={hours_old})")

        try:
            df = scrape_jobs(search_term=term, **params)
        except Exception as e:
            _err(f"[linkedin] Search '{term}' failed: {e}. Skipping.")
            continue

        if df is None or df.empty:
            _err(f"[linkedin] No results for '{term}'.")
            continue

        _err(f"[linkedin] Got {len(df)} rows for '{term}'.")

        for _, row in df.iterrows():
            if yielded >= max_jobs:
                break

            job = _row_to_job(row, run_id, scraped_at)
            if job is None:
                continue

            url = job["meta"]["source_url"]
            if url in seen_urls:
                continue  # dedup across searches
            seen_urls.add(url)

            yield job
            yielded += 1

    _err(f"[linkedin] Done. {yielded} unique jobs yielded.")


# ---------------------------------------------------------------------------
# Row → Job schema
# ---------------------------------------------------------------------------

def _row_to_job(row, run_id: str, scraped_at: str) -> dict | None:
    """
    Map a single JobSpy DataFrame row to our Job schema.
    Returns None if the row is too malformed to use.
    """
    # Source URL — required; skip row if absent
    source_url = _str(row.get("job_url") or row.get("job_url_direct", ""))
    if not source_url:
        return None

    # Job ID: prefer JobSpy's id column; fall back to hash of URL
    raw_id = row.get("id")
    if raw_id and str(raw_id).strip():
        job_id = str(raw_id).strip()
    else:
        job_id = hashlib.md5(source_url.encode()).hexdigest()[:16]

    job = make_empty_job("linkedin", source_url, job_id, run_id, scraped_at)

    # Title
    title = _str(row.get("title", ""))
    job["title"] = title
    job["seniority"] = guess_seniority(title)

    # Company
    job["company"]["name"] = _str(row.get("company", ""))

    # Location
    location_str = _str(row.get("location", ""))
    loc = parse_location(location_str)
    job["location"]["type"]      = loc["type"]
    job["location"]["cities"]    = loc["cities"]
    job["location"]["countries"] = loc["countries"]
    if loc["type"] is None and location_str:
        add_flag(job, FLAGS.LOCATION_UNPARSED)

    # posted_at — JobSpy returns date_posted as a Python date or datetime
    date_posted = row.get("date_posted")
    if date_posted is not None:
        try:
            if hasattr(date_posted, "isoformat"):
                # date or datetime — normalize to UTC ISO string
                if isinstance(date_posted, datetime):
                    if date_posted.tzinfo is None:
                        date_posted = date_posted.replace(tzinfo=timezone.utc)
                    job["meta"]["posted_at"] = date_posted.isoformat()
                else:
                    # date object — convert to datetime at midnight UTC
                    dt = datetime(date_posted.year, date_posted.month, date_posted.day,
                                  tzinfo=timezone.utc)
                    job["meta"]["posted_at"] = dt.isoformat()
            else:
                job["meta"]["posted_at"] = str(date_posted)
        except Exception:
            add_flag(job, FLAGS.POSTED_AT_MISSING)
    else:
        add_flag(job, FLAGS.POSTED_AT_MISSING)

    # Employment type — JobSpy uses job_type column
    job_type_str = _str(row.get("job_type", ""))
    emp_type = parse_employment_type(job_type_str)
    job["employment_type"] = emp_type
    if emp_type is None and job_type_str:
        add_flag(job, FLAGS.EMPLOYMENT_TYPE_MISSING)

    # Compensation — JobSpy provides these as separate columns
    min_amount = _float(row.get("min_amount"))
    max_amount = _float(row.get("max_amount"))
    currency   = _str(row.get("currency", "")) or None
    interval   = _str(row.get("interval", "")) or None

    # Normalize JobSpy interval strings to our schema values
    if interval:
        interval = _normalize_interval(interval)

    job["compensation"] = {
        "min":      min_amount,
        "max":      max_amount,
        "currency": currency,
        "interval": interval,
    }
    if min_amount is None:
        add_flag(job, FLAGS.SALARY_MISSING)
    elif currency is None:
        add_flag(job, FLAGS.CURRENCY_UNSUPPORTED)

    # description_raw — only present if linkedin_fetch_description=True
    description = _str(row.get("description", ""))
    job["description_raw"] = description  # "" if not fetched; fetcher overwrites

    return job


def _str(val) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None


def _normalize_interval(interval: str) -> str | None:
    """Map JobSpy interval strings to our schema values."""
    mapping = {
        "yearly":  "annual",
        "annual":  "annual",
        "monthly": "monthly",
        "weekly":  None,       # weekly not supported; comp flagged as interval_missing
        "hourly":  "hourly",
        "hour":    "hourly",
    }
    return mapping.get(interval.lower().strip())


def _err(msg: str) -> None:
    import sys
    print(msg, file=sys.stderr, flush=True)