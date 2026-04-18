"""
schema.py — canonical Job dict shape for all scrapers.
Matches job-filter/src/types.ts exactly.
All scrapers call make_empty_job() then fill what they can.
"""

from datetime import datetime, timezone


SCHEMA_VERSION = "1.0.0"

# --- Seniority levels (must match constants.ts) ---
SENIORITY = {
    "intern", "junior", "mid", "senior", "staff", "lead", "principal", "manager"
}

# --- Employment types (must match constants.ts) ---
EMPLOYMENT_TYPE = {
    "full_time", "contract", "contract_to_hire", "part_time"
}

# --- Source sites ---
SOURCE_SITES = {"dice", "jobright", "linkedin", "indeed", "glassdoor", "ziprecruiter"}

# --- Flags (soft signals, do not reject) ---
class FLAGS:
    POSTED_AT_MISSING      = "posted_at_missing"
    REMOTE_UNCLEAR         = "remote_unclear"
    CURRENCY_UNSUPPORTED   = "currency_unsupported"
    SALARY_MISSING         = "salary_missing"
    EMPLOYMENT_TYPE_MISSING = "employment_type_missing"
    LOCATION_UNPARSED      = "location_unparsed"
    SOURCE_SCORE_SUSPECT   = "source_score_suspect"
    EDUCATION_UNPARSED     = "education_unparsed"
    STALE_POSTING          = "stale_posting"
    THIRD_PARTY_CONTRACT   = "third_party_contract"


def make_empty_job(
    source_site: str,
    source_url: str,
    job_id: str,
    run_id: str,
    scraped_at: str | None = None,
) -> dict:
    """
    Returns a Job dict with all fields at safe defaults.
    Scrapers fill title, company, location, etc. after calling this.
    Extractor fills required_skills, years_experience, education_required,
    visa_sponsorship, domain, responsibilities, description_raw later.
    """
    if scraped_at is None:
        scraped_at = datetime.now(timezone.utc).isoformat()

    return {
        "meta": {
            "job_id":         job_id,
            "schema_version": SCHEMA_VERSION,
            "source_site":    source_site,
            "source_url":     source_url,
            "source_score":   None,       # float 0–100, or None
            "posted_at":      None,       # ISO str or None
            "scraped_at":     scraped_at,
            "run_id":         run_id,
            "flags":          [],
        },
        "title":           "",
        "seniority":       "mid",         # guess_seniority fills this
        "employment_type": None,

        "company": {
            "name": "",
            "type": "unknown",
        },

        "location": {
            "type":      None,            # remote | hybrid | onsite | None
            "timezone":  None,
            "cities":    [],
            "countries": [],
        },

        "compensation": {
            "min":      None,
            "max":      None,
            "currency": None,
            "interval": None,
        },

        # Extractor fills these:
        "required_skills":    [],
        "years_experience":   {"min": None, "max": None},
        "education_required": {"minimum": "", "field": ""},
        "visa_sponsorship":   None,
        "security_clearance": "none",     # default safe; extractor may update
        "domain":             None,
        "responsibilities":   [],
        "description_raw":    "",         # fetcher fills
    }


def add_flag(job: dict, flag: str) -> None:
    """Append flag to job.meta.flags if not already present."""
    if flag not in job["meta"]["flags"]:
        job["meta"]["flags"].append(flag)