"""
dice.py — Dice.com recommended jobs scraper (paginated).

Target URL: https://www.dice.com/recommended-jobs

Selectors verified against live HTML snapshot (2026-04-17).
Pagination is page-by-page (not infinite scroll).

Selector stability note:
    Dice uses semantic data-testid and id attributes — more stable than
    Jobright's hashed class names. Still, if selectors break, update the
    constants below and re-verify against a fresh HTML snapshot.
"""

import re
import sys
from pathlib import Path
from typing import Iterator

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from scraper.common.cookies import load_cookies
from scraper.common.normalize import (
    guess_seniority,
    parse_employment_type,
    parse_location,
    parse_posted_at,
    parse_salary,
)
from scraper.common.output import now_iso
from scraper.common.schema import FLAGS, add_flag, make_empty_job

from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Selector constants
# ---------------------------------------------------------------------------
CARD_SEL         = "div[data-testid='job-card']"
DETAIL_LINK_SEL  = "a[data-testid='job-search-job-detail-link']"
COMPANY_SEL      = "p.mb-0.line-clamp-2"
LOCATION_TAGS_SEL = "p.text-sm.font-normal.text-zinc-600"
EMPLOYMENT_SEL   = "p[id='employmentType-label']"
SALARY_SEL       = "p[id='salary-label']"
NEXT_BTN_SEL     = "span[aria-label='Next']"

DICE_URL         = "https://www.dice.com/recommended-jobs"
CARD_WAIT_MS     = 10000   # wait for cards to appear after page load / navigation


def scrape(
    max_jobs: int,
    run_id: str,
    cookies_path: Path,
    headless: bool = True,
) -> Iterator[dict]:
    """
    Scrape Dice recommended jobs, paginating through results.

    Yields Job dicts (schema.py shape) one at a time as pages are parsed.
    Stops when max_jobs reached or no Next button available.

    Args:
        max_jobs:     stop after this many unique jobs
        run_id:       from output.make_run_id()
        cookies_path: path to browser-exported Dice cookies JSON
        headless:     False = show browser window (useful for debugging)

    Raises:
        FileNotFoundError: if cookies file missing
        RuntimeError: if job cards never appear (selector broken or auth failed)
    """
    cookies = load_cookies(cookies_path)
    scraped_at = now_iso()
    now = datetime.now(timezone.utc)
    seen_ids: set[str] = set()
    yielded = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        context.add_cookies(cookies)

        page = context.new_page()
        page.goto(DICE_URL)

        try:
            page.wait_for_selector(CARD_SEL, timeout=CARD_WAIT_MS)
        except PlaywrightTimeoutError:
            raise RuntimeError(
                f"No job cards found at {DICE_URL} after {CARD_WAIT_MS}ms. "
                "Possible causes: auth failed (rotate cookies), selector changed, "
                "or Dice blocked the request."
            )

        page_num = 1
        _err(f"[dice] Loaded page {page_num}. Scraping up to {max_jobs} jobs...")

        while True:
            # Remember first card's data-id so we can detect page navigation
            first_card_id = _get_first_card_id(page.content())

            jobs_on_page = _parse_cards(page.content(), seen_ids, run_id, scraped_at, now)
            _err(f"[dice] Page {page_num}: {len(jobs_on_page)} new jobs")

            for job in jobs_on_page:
                yield job
                yielded += 1
                if yielded >= max_jobs:
                    _err(f"[dice] Reached {max_jobs} jobs, stopping.")
                    browser.close()
                    return

            # Check Next button
            next_btn = page.locator(NEXT_BTN_SEL)
            if next_btn.count() == 0:
                _err("[dice] No Next button found. Done.")
                break

            disabled = next_btn.get_attribute("data-disabled")
            if disabled == "true":
                _err("[dice] Next button disabled (last page). Done.")
                break

            # Navigate to next page
            next_btn.click()
            page_num += 1

            # Wait for stale first card to detach and new cards to appear.
            # Strategy: wait for the old first card's data-id to disappear,
            # then wait for fresh cards to be present.
            try:
                if first_card_id:
                    page.wait_for_selector(
                        f"div[data-testid='job-card'][data-id='{first_card_id}']",
                        state="detached",
                        timeout=8000,
                    )
                page.wait_for_selector(CARD_SEL, timeout=CARD_WAIT_MS)
            except PlaywrightTimeoutError:
                _err(f"[dice] Timeout waiting for page {page_num} to load. Stopping.")
                break

        browser.close()


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _get_first_card_id(html: str) -> str | None:
    """Return data-id of the first job card, used as stale-detection anchor."""
    soup = BeautifulSoup(html, "html.parser")
    card = soup.select_one(CARD_SEL)
    return card.get("data-id") if card else None


def _parse_cards(
    html: str,
    seen_ids: set[str],
    run_id: str,
    scraped_at: str,
    now: datetime,
) -> list[dict]:
    """
    Parse all job cards from a single Dice page HTML snapshot.
    Returns only cards not already in seen_ids.
    Mutates seen_ids.
    """
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(CARD_SEL)
    new_jobs = []

    for card in cards:
        job_id = card.get("data-id", "").strip()
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        # Detail URL + title from the same anchor
        detail_link = card.select_one(DETAIL_LINK_SEL)
        if not detail_link:
            continue  # malformed card, skip

        href = detail_link.get("href", "")
        source_url = href if href.startswith("http") else f"https://www.dice.com{href}"
        title = detail_link.text.strip()

        job = make_empty_job("dice", source_url, job_id, run_id, scraped_at)
        job["title"] = title
        job["seniority"] = guess_seniority(title)

        # Company: first p.mb-0.line-clamp-2 inside the logo span
        company_el = card.select_one(COMPANY_SEL)
        job["company"]["name"] = company_el.text.strip() if company_el else ""

        # Location + Posted: two p.text-sm.font-normal.text-zinc-600 in location span
        # First = location string, Second = posted string (after the bullet separator)
        location_tags = card.select(LOCATION_TAGS_SEL)
        location_text = location_tags[0].text.strip() if len(location_tags) > 0 else ""
        posted_text   = location_tags[1].text.strip() if len(location_tags) > 1 else ""

        loc = parse_location(location_text)
        job["location"]["type"]      = loc["type"]
        job["location"]["cities"]    = loc["cities"]
        job["location"]["countries"] = loc["countries"]

        if loc["type"] is None and location_text:
            add_flag(job, FLAGS.LOCATION_UNPARSED)

        posted_at = parse_posted_at(posted_text, now)
        job["meta"]["posted_at"] = posted_at
        if posted_at is None:
            add_flag(job, FLAGS.POSTED_AT_MISSING)

        # Employment type
        emp_el = card.select_one(EMPLOYMENT_SEL)
        emp_text = emp_el.text.strip() if emp_el else ""
        emp_type = parse_employment_type(emp_text)
        job["employment_type"] = emp_type
        if emp_type is None and emp_text:
            add_flag(job, FLAGS.EMPLOYMENT_TYPE_MISSING)
        if re.search(r"third.?party", emp_text, re.IGNORECASE):
            add_flag(job, FLAGS.THIRD_PARTY_CONTRACT)

        # Salary
        sal_el = card.select_one(SALARY_SEL)
        sal_text = sal_el.text.strip() if sal_el else ""
        comp = parse_salary(sal_text)
        job["compensation"] = comp
        if comp["min"] is None:
            add_flag(job, FLAGS.SALARY_MISSING)
        elif comp["currency"] is None:
            add_flag(job, FLAGS.CURRENCY_UNSUPPORTED)

        new_jobs.append(job)

    return new_jobs


def _err(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)