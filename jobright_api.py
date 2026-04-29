"""
jobright_api.py — Jobright API adapter.

Calls Jobright's endpoint:
  https://jobright.ai/swan/recommend/list/jobs

Pagination protocol:
  - refresh=true only on the first page (position=0)
  - position is an offset; page size is fixed at 10
  - sort_condition selects "latest" vs other ranking modes

Rate limit / anti-scraping:
  Server-side risk control triggers around ~30–40 jobs per session with
  errorCode 43004 ("reach job list limit").

We enforce:
  - HARD_CAP = 50 (raise gradually; enforce single backoff+retry on 43004)
  - polite THROTTLE_SECONDS between calls (2.0s)
"""

import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from scraper.common.normalize import (
    guess_seniority,
    parse_employment_type,
    parse_location,
    parse_salary,
)
from scraper.common.output import now_iso
from scraper.common.schema import make_empty_job

API_URL = "https://jobright.ai/swan/recommend/list/jobs"
PAGE_SIZE = 10
THROTTLE_SECONDS = 2.0
HARD_CAP = 50

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/147.0.0.0 Safari/537.36"
)


def _err(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _load_session_id(cookies_path: Path) -> str:
    if not cookies_path.exists():
        raise FileNotFoundError(
            f"Cookie file not found at {cookies_path}.\n"
            "Create it with a SESSION_ID exported from your browser.\n"
            "Example formats accepted:\n"
            '  { "SESSION_ID": "..." }\n'
            '  [ { "name": "SESSION_ID", "value": "..." } ]\n'
        )

    raw = json.loads(cookies_path.read_text(encoding="utf-8"))

    # Accept dict format: { "SESSION_ID": "..." }
    if isinstance(raw, dict):
        sid = raw.get("SESSION_ID") or raw.get("session_id")
        if sid:
            return str(sid)

    # Accept Playwright-export cookie array format:
    # [ { "name": "SESSION_ID", "value": "..." }, ... ]
    if isinstance(raw, list):
        for c in raw:
            if isinstance(c, dict) and c.get("name") == "SESSION_ID":
                sid = c.get("value")
                if sid:
                    return str(sid)

    raise ValueError(f"{cookies_path} does not contain a SESSION_ID")


def _fetch_page(session_id: str, position: int, refresh: bool, sort_condition: int = 1) -> dict:
    params = {
        "refresh": "true" if refresh else "false",
        "sortCondition": str(sort_condition),
        "position": str(position),
        "count": str(PAGE_SIZE),
        "syncRerank": "false",
    }

    headers = {
        "accept":             "application/json, text/plain, */*",
        "accept-language":    "en-US,en;q=0.9",
        "x-client-type":      "web",
        "referer":            "https://jobright.ai/jobs/recommend",
        "user-agent":         USER_AGENT,
        "dnt":                "1",
        "priority":           "u=1, i",
        "sec-ch-ua":          '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        # Only SESSION_ID cookie matters; other cookies are non-functional for this adapter.
        "cookie":            f"SESSION_ID={session_id}",
    }

    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET", headers=headers)

    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = resp.read().decode("utf-8", errors="replace")
        return json.loads(payload)


def _map_seniority(jr_seniority: Optional[str], title: str) -> str:
    # Prefer title-based guess (more stable across API variations).
    return guess_seniority(title) if title else (jr_seniority or "mid").lower()  # fallback


def _build_description(job_result: dict) -> str:
    parts: list[str] = []
    job_summary = job_result.get("jobSummary")
    if job_summary:
        parts.append(str(job_summary))

    resp = job_result.get("coreResponsibilities") or []
    if resp:
        parts.append("\n\nResponsibilities:")
        parts.extend(f"- {r}" for r in resp)

    quals = job_result.get("qualifications") or {}
    must = quals.get("mustHave") or []
    if must:
        parts.append("\n\nRequired qualifications:")
        parts.extend(f"- {q}" for q in must)

    pref = quals.get("preferredHave") or []
    if pref:
        parts.append("\n\nPreferred qualifications:")
        parts.extend(f"- {q}" for q in pref)

    return "\n".join(parts).strip()


def _map_job(item: dict, run_id: str, scraped_at: str) -> dict:
    jr = item.get("jobResult") or {}
    cr = item.get("companyResult") or {}

    job_id = jr.get("jobId") or ""
    job_id = str(job_id).strip()
    if not job_id:
        raise ValueError("Missing jobId")

    source_url = jr.get("applyLink") or jr.get("originalUrl") or f"https://jobright.ai/jobs/info/{job_id}"

    title = str(jr.get("jobTitle") or "").strip()
    company_name = str(cr.get("companyName") or "").strip()

    work_model = str(jr.get("workModel") or "").strip()
    w = work_model.lower()
    is_remote = bool(jr.get("isRemote"))

    location_type: Optional[str] = None
    if is_remote:
        location_type = "remote"
    elif "hybrid" in w:
        location_type = "hybrid"
    elif "remote" in w:
        location_type = "remote"
    elif "on-site" in w or "onsite" in w or "on site" in w:
        location_type = "onsite"

    location_str = jr.get("jobLocation") or ""
    loc = parse_location(str(location_str))
    # Prefer parsed location type, but fall back to our best-effort from workModel/isRemote.
    loc_type = loc.get("type") or location_type

    employment_type = parse_employment_type(str(jr.get("employmentType") or ""))

    # Prefer Jobright's structured numeric fields; they're already in full USD/year.
    # Fall back to parse_salary(salaryDesc) only if the API didn't provide numbers.
    min_salary = jr.get("minSalary")
    max_salary = jr.get("maxSalary")
    if min_salary is not None or max_salary is not None:
        comp = {
            "min":      float(min_salary) if min_salary is not None else None,
            "max":      float(max_salary) if max_salary is not None else None,
            "currency": "USD",
            "interval": "annual",
        }
    else:
        salary_desc = jr.get("salaryDesc") or ""
        comp = parse_salary(str(salary_desc))

    # posted_at: keep provider's value as-is. The pipeline's
    # postFetchChecks uses Date.parse and will mark it missing if invalid.
    posted_at_raw = jr.get("publishTime") or jr.get("postedAt")
    posted_at: str | None = str(posted_at_raw) if posted_at_raw else None

    # Recommendation scores (0–100) — optional.
    skill_match = next(
        (
            s.get("score")
            for s in (jr.get("recommendationScores") or [])
            if isinstance(s, dict) and s.get("featureName") == "q_job_skill_match"
        ),
        None,
    )

    # Basic placeholders used before extraction.
    job = make_empty_job(
        "jobright_api",
        str(source_url),
        str(job_id),
        run_id,
        scraped_at,
    )

    job["title"] = title
    job["seniority"] = guess_seniority(title)
    job["employment_type"] = employment_type
    job["company"]["name"] = company_name
    job["location"]["type"] = loc_type
    job["location"]["cities"] = loc.get("cities") or []
    job["location"]["countries"] = loc.get("countries") or []
    job["compensation"] = comp

    job["meta"]["source_score"] = (
        float(skill_match) if skill_match is not None and str(skill_match).strip() else None
    )
    job["meta"]["posted_at"] = posted_at

    # Clearance / sponsorship flags used by the hard filter.
    is_clearance_required = bool(jr.get("isClearanceRequired"))
    job["security_clearance"] = "public_trust" if is_clearance_required else "none"
    job["visa_sponsorship"] = jr.get("isH1bSponsor")  # boolean | None (best effort)

    # Seed description for EXTRACT=1 runs even if fetch fails.
    job["description_raw"] = _build_description(jr)

    # Save extra API metadata in meta for later calibration/debugging.
    industry_match = next(
        (
            s.get("score")
            for s in (jr.get("recommendationScores") or [])
            if isinstance(s, dict) and s.get("featureName") == "q_industry_match"
        ),
        None,
    )

    h1b_2025 = next(
        (
            h.get("count")
            for h in (cr.get("h1bAnnualJobCount") or [])
            if isinstance(h, dict) and str(h.get("year")) == "2025"
        ),
        0,
    )

    job["meta"]["jobright_display_score"]  = item.get("displayScore")
    job["meta"]["jobright_rank_desc"]      = item.get("rankDesc")
    job["meta"]["jobright_skill_match"]    = (
        float(skill_match) if skill_match is not None else None
    )
    job["meta"]["jobright_industry_match"] = (
        float(industry_match) if industry_match is not None else None
    )
    job["meta"]["company_is_agency"]       = cr.get("isAgency", False)
    job["meta"]["company_size"]            = cr.get("companySize")
    job["meta"]["company_h1b_count_2025"]  = int(h1b_2025) if h1b_2025 is not None else 0

    return job


def scrape(
    max_jobs: int = HARD_CAP,
    sort_condition: int = 1,
    run_id: str = "",
    cookies_path: Path | None = None,
) -> Iterator[dict]:
    """
    Fetch up to max_jobs from Jobright API. Paginates 10 at a time.

    Stops on:
      - risk control errorCode 43004
      - API error
      - empty result page
    """
    if cookies_path is None:
        raise ValueError("cookies_path is required for jobright_api")

    if max_jobs > HARD_CAP:
        _err(f"[jobright_api] WARNING max_jobs={max_jobs} exceeds HARD_CAP={HARD_CAP}; capping")
        max_jobs = HARD_CAP

    session_id = _load_session_id(cookies_path)
    scraped_at = now_iso()

    seen_ids: set[str] = set()
    jobs: list[dict] = []
    position = 0
    retried_after_rate_limit = False  # one retry per run

    while len(jobs) < max_jobs:
        refresh = position == 0
        try:
            data = _fetch_page(session_id, position, refresh, sort_condition=sort_condition)
        except Exception as e:
            _err(f"[jobright_api] Network/HTTP error at position={position}: {e}")
            break
        
        if not data.get("success"):
            err_code = data.get("errorCode")
            err_msg = data.get("errorMsg", "")
            if err_code == 43004:
                if not retried_after_rate_limit:
                    _err(
                        f"[jobright_api] Rate limit at position={position}. Backing off 60s and retrying once."
                    )
                    time.sleep(60)
                    retried_after_rate_limit = True
                    continue

                _err(
                    f"[jobright_api] Rate limit at position={position} after retry. Stopping with {len(jobs)} jobs."
                )
                break
            _err(f"[jobright_api] API error {err_code}: {err_msg}")
            break

        page_items = ((data.get("result") or {}).get("jobList")) or []
        if not page_items:
            _err(f"[jobright_api] Empty page at position={position} — ending.")
            break

        new_count = 0
        for item in page_items:
            try:
                mapped = _map_job(item, run_id=run_id, scraped_at=scraped_at)
            except Exception:
                continue

            mapped_id = mapped.get("meta", {}).get("job_id")
            if not mapped_id or mapped_id in seen_ids:
                continue
            seen_ids.add(mapped_id)

            yield mapped
            jobs.append(mapped)
            new_count += 1
            if len(jobs) >= max_jobs:
                break

        _err(
            f"[jobright_api] position={position} refresh={refresh} page_items={len(page_items)} new={new_count} total={len(jobs)}"
        )

        position += PAGE_SIZE
        if len(jobs) < max_jobs:
            time.sleep(THROTTLE_SECONDS)

    _err(f"[jobright_api] Done. Yielded {len(jobs)} jobs.")

