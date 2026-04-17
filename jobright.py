"""
jobright.py — Jobright.ai scraper (infinite scroll).

WARNING: All selectors below use Jobright's CSS-module hashed class names.
These are NOT stable. If the site rebuilds its frontend, every selector here
will break silently. When that happens: open the page, inspect a job card,
find the new hashed class names, update the constants below.

Hashed selectors (as of 2026-04-17):
    CARD_SEL        = "div.index_job-card__oqX1M"
    SCROLL_LIST_SEL = ".index_jobs-list-scrollable__oMkUx"
    TITLE_SEL       = "h2.index_job-title__Riiip"
    COMPANY_SEL     = "div.index_company-name__jnxCX"
    META_ITEMS_SEL  = "div.index_job-metadata-item__Ok2Yh span"
    SCORE_SEL       = "span.index_percent-value__e9Uef"
"""

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from scraper.common.cookies import load_cookies
from scraper.common.normalize import guess_seniority
from scraper.common.output import now_iso
from scraper.common.schema import FLAGS, add_flag, make_empty_job

# ---------------------------------------------------------------------------
# Selector constants — update here when site rebuilds
# ---------------------------------------------------------------------------
CARD_SEL        = "div.index_job-card__oqX1M"
SCROLL_LIST_SEL = ".index_jobs-list-scrollable__oMkUx"
TITLE_SEL       = "h2.index_job-title__Riiip"
COMPANY_SEL     = "div.index_company-name__jnxCX"
META_ITEMS_SEL  = "div.index_job-metadata-item__Ok2Yh span"
SCORE_SEL       = "span.index_percent-value__e9Uef"

JOBRIGHT_URL    = "https://jobright.ai/jobs/recommend"
MAX_NO_NEW      = 15    # stop after this many consecutive scroll steps with no new cards
SCROLL_DELTA    = 800   # px per wheel step
SCROLL_DELAY_MS = 1200  # ms between scroll steps


def scrape(
    max_jobs: int,
    run_id: str,
    cookies_path: Path,
    headless: bool = True,
) -> Iterator[dict]:
    """
    Scrape Jobright recommended jobs via infinite scroll.

    Yields Job dicts (schema.py shape) one at a time.
    Does NOT accumulate all jobs in memory before yielding.

    Args:
        max_jobs:     stop after this many unique jobs
        run_id:       from output.make_run_id()
        cookies_path: path to browser-exported Jobright cookies JSON
        headless:     False = show browser window (useful for debugging)

    Raises:
        FileNotFoundError: if cookies file is missing
        RuntimeError: if job list container not found on page (selector broken)
    """
    cookies = load_cookies(cookies_path)
    scraped_at = now_iso()
    seen_ids: set[str] = set()
    yielded = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        context.add_cookies(cookies)

        page = context.new_page()
        page.goto(JOBRIGHT_URL)

        # Wait for first cards to appear — more reliable than a fixed timeout
        page.wait_for_selector(CARD_SEL, timeout=15000)

        # Get scroll container bounding box — must succeed or we fail loud
        box = page.locator(SCROLL_LIST_SEL).bounding_box()
        if box is None:
            raise RuntimeError(
                f"Scroll container '{SCROLL_LIST_SEL}' not found on page. "
                "Jobright may have rebuilt its frontend — update SCROLL_LIST_SEL."
            )

        cx = box["x"] + box["width"] / 2
        cy = box["y"] + 200   # top-third of visible list, away from page scrollbar
        page.mouse.move(cx, cy)

        _err(f"[jobright] Job list at x={cx:.0f}, y={cy:.0f}. Scrolling for {max_jobs} jobs...")

        no_new_count = 0

        for step in range(300):
            new_jobs = _parse_cards(page.content(), seen_ids, run_id, scraped_at)

            for job in new_jobs:
                yield job
                yielded += 1
                if yielded >= max_jobs:
                    _err(f"[jobright] Reached {max_jobs} jobs, stopping.")
                    browser.close()
                    return

            if new_jobs:
                _err(f"[jobright] Step {step + 1}: +{len(new_jobs)} new (total: {yielded})")
                no_new_count = 0
            else:
                no_new_count += 1

            if no_new_count >= MAX_NO_NEW:
                _err(f"[jobright] No new jobs after {MAX_NO_NEW} scrolls, stopping.")
                break

            page.mouse.move(cx, cy)
            page.mouse.wheel(0, SCROLL_DELTA)
            page.wait_for_timeout(SCROLL_DELAY_MS)

            if step % 5 == 0:
                page.keyboard.press("PageDown")
                page.wait_for_timeout(500)

        browser.close()


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _parse_cards(
    html: str,
    seen_ids: set[str],
    run_id: str,
    scraped_at: str,
) -> list[dict]:
    """
    Parse job cards from page HTML. Returns only cards not already in seen_ids.
    Mutates seen_ids to mark newly parsed IDs.
    """
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(CARD_SEL)
    new_jobs = []

    for card in cards:
        job_id = card.get("id", "").strip()
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        source_url = f"https://jobright.ai/jobs/info/{job_id}"
        job = make_empty_job("jobright", source_url, job_id, run_id, scraped_at)

        # Title
        title_el = card.select_one(TITLE_SEL)
        title = title_el.text.strip() if title_el else ""
        job["title"] = title
        job["seniority"] = guess_seniority(title)

        # Company
        company_el = card.select_one(COMPANY_SEL)
        job["company"]["name"] = company_el.text.strip() if company_el else ""

        # Match score: "85%" → 85.0
        score_el = card.select_one(SCORE_SEL)
        if score_el:
            raw_score = score_el.text.strip().replace("%", "")
            try:
                job["meta"]["source_score"] = float(raw_score)
            except ValueError:
                pass  # leave as None

        # Metadata (location, posted, etc.) → description_raw for now.
        # Fetcher will overwrite description_raw with full JD later.
        # We stash the listing metadata here so nothing is lost before fetch.
        meta_items = card.select(META_ITEMS_SEL)
        if meta_items:
            job["description_raw"] = " | ".join(m.text.strip() for m in meta_items)

        new_jobs.append(job)

    return new_jobs


def _err(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)