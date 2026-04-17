"""
scraper/common — shared utilities for all scrapers.

Exports:
    schema    — Job dict shape, make_empty_job, add_flag, FLAGS
    normalize — parse_posted_at, parse_location, parse_employment_type,
                parse_salary, guess_seniority
    cookies   — load_cookies
    output    — make_run_id, now_iso, write_jsonl, read_jsonl
"""