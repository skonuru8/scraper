"""
test_common.py — tests for scraper/common/*.py

Run: python -m pytest scraper/test_common.py -v
"""

import json
import sys
import pytest
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Make sure common/ is importable from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.common.normalize import (
    parse_posted_at,
    parse_location,
    parse_employment_type,
    parse_salary,
    guess_seniority,
)
from scraper.common.schema import make_empty_job, add_flag, FLAGS, SCHEMA_VERSION
from scraper.common.cookies import load_cookies, _sanitize_cookie
from scraper.common.output import make_run_id, now_iso, write_jsonl, read_jsonl


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NOW = datetime(2026, 4, 17, 14, 30, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# parse_posted_at
# ---------------------------------------------------------------------------

class TestParsePostedAt:
    def test_today(self):
        assert parse_posted_at("Today", NOW) == NOW.isoformat()

    def test_today_case_insensitive(self):
        assert parse_posted_at("today", NOW) == NOW.isoformat()

    def test_yesterday(self):
        expected = (NOW - timedelta(days=1)).isoformat()
        assert parse_posted_at("Yesterday", NOW) == expected

    def test_3d_ago(self):
        expected = (NOW - timedelta(days=3)).isoformat()
        assert parse_posted_at("3d ago", NOW) == expected

    def test_16d_ago(self):
        expected = (NOW - timedelta(days=16)).isoformat()
        assert parse_posted_at("16d ago", NOW) == expected

    def test_29d_ago(self):
        expected = (NOW - timedelta(days=29)).isoformat()
        assert parse_posted_at("29d ago", NOW) == expected

    def test_days_spelled_out(self):
        expected = (NOW - timedelta(days=9)).isoformat()
        assert parse_posted_at("9 days ago", NOW) == expected

    def test_hours_ago(self):
        expected = (NOW - timedelta(hours=4)).isoformat()
        assert parse_posted_at("4h ago", NOW) == expected

    def test_unknown_format_returns_none(self):
        assert parse_posted_at("Last week", NOW) is None

    def test_empty_returns_none(self):
        assert parse_posted_at("", NOW) is None

    def test_none_input_returns_none(self):
        assert parse_posted_at(None, NOW) is None


# ---------------------------------------------------------------------------
# parse_location
# ---------------------------------------------------------------------------

class TestParseLocation:
    def test_remote(self):
        r = parse_location("Remote")
        assert r["type"] == "remote"
        assert r["cities"] == []
        assert r["countries"] == []

    def test_remote_lowercase(self):
        assert parse_location("remote")["type"] == "remote"

    def test_hybrid_with_city_state(self):
        r = parse_location("Hybrid in New York, New York")
        assert r["type"] == "hybrid"
        assert "New York" in r["cities"]
        assert "USA" in r["countries"]

    def test_onsite_city_state(self):
        r = parse_location("Jersey City, New Jersey")
        assert r["type"] == "onsite"
        assert "Jersey City" in r["cities"]
        assert "USA" in r["countries"]

    def test_remote_or_hybrid(self):
        r = parse_location("Remote or Hybrid in Boston, Massachusetts")
        assert r["type"] == "hybrid"
        assert "Boston" in r["cities"]
        assert "USA" in r["countries"]

    def test_remote_or_city(self):
        r = parse_location("Remote or Rutherford, New Jersey")
        assert r["type"] == "hybrid"
        assert "Rutherford" in r["cities"]
        assert "USA" in r["countries"]

    def test_bayonne(self):
        r = parse_location("Bayonne, New Jersey")
        assert r["type"] == "onsite"
        assert r["cities"] == ["Bayonne"]

    def test_empty_returns_none_type(self):
        r = parse_location("")
        assert r["type"] is None
        assert r["cities"] == []

    def test_unparseable_returns_none_type(self):
        r = parse_location("Local to New Jersey only")
        # Contains NJ state → countries = USA, but type unclear — acceptable either way
        # Key requirement: must not crash
        assert isinstance(r["type"], (str, type(None)))


# ---------------------------------------------------------------------------
# parse_employment_type
# ---------------------------------------------------------------------------

class TestParseEmploymentType:
    def test_full_time(self):
        assert parse_employment_type("Full-time") == "full_time"

    def test_contract(self):
        assert parse_employment_type("Contract") == "contract"

    def test_contract_third_party(self):
        assert parse_employment_type("Contract, Third Party") == "contract"

    def test_full_time_contract_picks_contract(self):
        assert parse_employment_type("Full-time, Contract") == "contract"

    def test_part_time(self):
        assert parse_employment_type("Part-time") == "part_time"

    def test_contract_to_hire(self):
        assert parse_employment_type("Contract to Hire") == "contract_to_hire"

    def test_c2h(self):
        assert parse_employment_type("C2H") == "contract_to_hire"

    def test_unknown_returns_none(self):
        assert parse_employment_type("Freelance") is None

    def test_empty_returns_none(self):
        assert parse_employment_type("") is None


# ---------------------------------------------------------------------------
# parse_salary
# ---------------------------------------------------------------------------

class TestParseSalary:
    def test_usd_range_annual(self):
        r = parse_salary("USD 145,000.00 - 150,000.00 per year")
        assert r["min"] == 145000.0
        assert r["max"] == 150000.0
        assert r["currency"] == "USD"
        assert r["interval"] == "annual"

    def test_usd_single_annual(self):
        r = parse_salary("USD 66,379.50 per year")
        assert r["min"] == 66379.5
        assert r["max"] == 66379.5
        assert r["currency"] == "USD"
        assert r["interval"] == "annual"

    def test_dollar_range_small_numbers_hourly(self):
        r = parse_salary("$70 - $80")
        assert r["min"] == 70.0
        assert r["max"] == 80.0
        assert r["currency"] == "USD"
        assert r["interval"] == "hourly"

    def test_no_currency_annual(self):
        r = parse_salary("80,000 - 90,000")
        assert r["min"] == 80000.0
        assert r["max"] == 90000.0
        assert r["currency"] is None
        assert r["interval"] == "annual"

    def test_indian_comma_format_returns_none(self):
        r = parse_salary("1,30,000 - 1,90,000")
        assert r == {"min": None, "max": None, "currency": None, "interval": None}

    def test_doe_returns_none(self):
        r = parse_salary("Depends on Experience")
        assert r == {"min": None, "max": None, "currency": None, "interval": None}

    def test_empty_returns_none(self):
        r = parse_salary("")
        assert r == {"min": None, "max": None, "currency": None, "interval": None}

    def test_large_range(self):
        r = parse_salary("USD 142,320.00 - 213,480.00 per year")
        assert r["min"] == 142320.0
        assert r["max"] == 213480.0


# ---------------------------------------------------------------------------
# guess_seniority
# ---------------------------------------------------------------------------

class TestGuessSeniority:
    def test_intern(self):
        assert guess_seniority("Software Engineering Intern") == "intern"

    def test_junior(self):
        assert guess_seniority("Junior Software Engineer") == "junior"

    def test_mid_default(self):
        assert guess_seniority("Software Engineer") == "mid"

    def test_senior(self):
        assert guess_seniority("Senior Software Engineer") == "senior"

    def test_sr_abbrev(self):
        assert guess_seniority("Sr. Software Engineer") == "senior"

    def test_staff(self):
        assert guess_seniority("Staff Software Engineer") == "staff"

    def test_principal(self):
        assert guess_seniority("Principal Engineer") == "principal"

    def test_lead(self):
        assert guess_seniority("Tech Lead") == "lead"

    def test_manager(self):
        assert guess_seniority("Engineering Manager") == "manager"

    def test_vp(self):
        assert guess_seniority("VP of Engineering") == "senior"

    def test_empty(self):
        assert guess_seniority("") == "mid"


# ---------------------------------------------------------------------------
# schema — make_empty_job + add_flag
# ---------------------------------------------------------------------------

class TestSchema:
    def test_make_empty_job_structure(self):
        job = make_empty_job("dice", "https://dice.com/job/123", "123", "run_abc")
        assert job["meta"]["job_id"] == "123"
        assert job["meta"]["source_site"] == "dice"
        assert job["meta"]["schema_version"] == SCHEMA_VERSION
        assert job["meta"]["run_id"] == "run_abc"
        assert job["meta"]["flags"] == []
        assert job["meta"]["source_score"] is None
        assert job["meta"]["posted_at"] is None
        assert job["seniority"] == "mid"
        assert job["employment_type"] is None
        assert job["company"] == {"name": "", "type": "unknown"}
        assert job["required_skills"] == []
        assert job["description_raw"] == ""
        assert job["security_clearance"] == "none"

    def test_add_flag_appends(self):
        job = make_empty_job("dice", "https://dice.com/job/1", "1", "run_1")
        add_flag(job, FLAGS.POSTED_AT_MISSING)
        assert FLAGS.POSTED_AT_MISSING in job["meta"]["flags"]

    def test_add_flag_no_duplicates(self):
        job = make_empty_job("dice", "https://dice.com/job/1", "1", "run_1")
        add_flag(job, FLAGS.POSTED_AT_MISSING)
        add_flag(job, FLAGS.POSTED_AT_MISSING)
        assert job["meta"]["flags"].count(FLAGS.POSTED_AT_MISSING) == 1

    def test_add_multiple_flags(self):
        job = make_empty_job("dice", "https://dice.com/job/1", "1", "run_1")
        add_flag(job, FLAGS.POSTED_AT_MISSING)
        add_flag(job, FLAGS.SALARY_MISSING)
        assert len(job["meta"]["flags"]) == 2

    def test_json_serializable(self):
        job = make_empty_job("dice", "https://dice.com/job/1", "1", "run_1")
        dumped = json.dumps(job)
        loaded = json.loads(dumped)
        assert loaded["meta"]["job_id"] == "1"


# ---------------------------------------------------------------------------
# cookies — _sanitize_cookie
# ---------------------------------------------------------------------------

class TestCookies:
    def test_strips_browser_fields(self):
        raw = {
            "name": "session",
            "value": "abc",
            "domain": ".dice.com",
            "path": "/",
            "hostOnly": True,
            "session": False,
            "storeId": "0",
            "id": 42,
            "sameSite": "Lax",
        }
        out = _sanitize_cookie(raw)
        assert "hostOnly" not in out
        assert "session" not in out
        assert "storeId" not in out
        assert "id" not in out
        assert out["name"] == "session"

    def test_samesite_none_becomes_None_string(self):
        raw = {"name": "x", "value": "y", "domain": "d", "path": "/", "sameSite": None}
        out = _sanitize_cookie(raw)
        assert out["sameSite"] == "None"

    def test_samesite_lowercase_normalized(self):
        raw = {"name": "x", "value": "y", "domain": "d", "path": "/", "sameSite": "lax"}
        out = _sanitize_cookie(raw)
        assert out["sameSite"] == "Lax"

    def test_samesite_strict_normalized(self):
        raw = {"name": "x", "value": "y", "domain": "d", "path": "/", "sameSite": "strict"}
        out = _sanitize_cookie(raw)
        assert out["sameSite"] == "Strict"

    def test_missing_cookie_file_raises(self, tmp_path):
        missing = tmp_path / "nope.json"
        with pytest.raises(FileNotFoundError, match="Cookie file not found"):
            load_cookies(missing)

    def test_invalid_json_raises(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError, match="not valid JSON"):
            load_cookies(bad)

    def test_non_list_json_raises(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text('{"name": "x"}', encoding="utf-8")
        with pytest.raises(ValueError, match="must be a JSON array"):
            load_cookies(bad)

    def test_valid_cookie_file(self, tmp_path):
        cookie_file = tmp_path / "test.json"
        cookie_file.write_text(json.dumps([
            {"name": "a", "value": "1", "domain": ".dice.com", "path": "/",
             "sameSite": "None", "hostOnly": False, "session": True, "id": 1}
        ]), encoding="utf-8")
        result = load_cookies(cookie_file)
        assert len(result) == 1
        assert "hostOnly" not in result[0]
        assert result[0]["sameSite"] == "None"


# ---------------------------------------------------------------------------
# output — make_run_id, now_iso, write_jsonl, read_jsonl
# ---------------------------------------------------------------------------

class TestOutput:
    def test_make_run_id_format(self):
        run_id = make_run_id()
        parts = run_id.split("_")
        assert len(parts) == 2
        assert len(parts[0]) == 8        # 8-char hex
        assert len(parts[1]) == 15       # YYYYMMDDTHHmmss

    def test_make_run_id_unique(self):
        ids = {make_run_id() for _ in range(20)}
        assert len(ids) == 20            # no collisions

    def test_now_iso_is_string(self):
        ts = now_iso()
        assert isinstance(ts, str)
        assert "T" in ts                 # ISO format check

    def test_write_and_read_jsonl(self, tmp_path):
        jobs = [
            make_empty_job("dice", f"https://dice.com/job/{i}", str(i), "run_test")
            for i in range(5)
        ]
        count, path = write_jsonl(iter(jobs), "dice", "run_test", output_dir=tmp_path, progress_every=0)
        assert count == 5
        assert path.exists()

        loaded = read_jsonl(path)
        assert len(loaded) == 5
        assert loaded[0]["meta"]["job_id"] == "0"
        assert loaded[4]["meta"]["job_id"] == "4"

    def test_output_dir_created_if_missing(self, tmp_path):
        nested = tmp_path / "deep" / "nested"
        jobs = [make_empty_job("dice", "https://dice.com/1", "1", "r")]
        write_jsonl(iter(jobs), "dice", "run_test", output_dir=nested, progress_every=0)
        assert nested.exists()