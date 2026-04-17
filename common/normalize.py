"""
normalize.py — pure normalization functions for scraped strings.
No I/O, no side effects. Each function tested against fixture strings.
"""

import re
from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# parse_posted_at
# ---------------------------------------------------------------------------

def parse_posted_at(text: str, now: datetime) -> str | None:
    """
    Convert relative date strings from job listings to ISO 8601.

    Supported variants:
        "Today"       → now.isoformat()
        "Yesterday"   → (now - 1d).isoformat()
        "3d ago"      → (now - 3d).isoformat()
        "16d ago"     → (now - 16d).isoformat()
        anything else → None  (caller should add posted_at_missing flag)

    Args:
        text: raw posted-at string from scraper
        now:  datetime representing the moment of scraping (timezone-aware preferred)

    Returns:
        ISO string or None
    """
    if not text:
        return None

    t = text.strip().lower()

    if t == "today":
        return now.isoformat()

    if t == "yesterday":
        return (now - timedelta(days=1)).isoformat()

    # "Nd ago" or "N days ago"
    m = re.match(r"^(\d+)\s*d(?:ays?)?\s*ago$", t)
    if m:
        days = int(m.group(1))
        return (now - timedelta(days=days)).isoformat()

    # "N hours ago" — treat as today
    m = re.match(r"^(\d+)\s*h(?:ours?)?\s*ago$", t)
    if m:
        hours = int(m.group(1))
        return (now - timedelta(hours=hours)).isoformat()

    return None


# ---------------------------------------------------------------------------
# parse_location
# ---------------------------------------------------------------------------

# US state names → country inference
_US_STATES = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut",
    "delaware","florida","georgia","hawaii","idaho","illinois","indiana","iowa",
    "kansas","kentucky","louisiana","maine","maryland","massachusetts","michigan",
    "minnesota","mississippi","missouri","montana","nebraska","nevada",
    "new hampshire","new jersey","new mexico","new york","north carolina",
    "north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island",
    "south carolina","south dakota","tennessee","texas","utah","vermont",
    "virginia","washington","west virginia","wisconsin","wyoming",
    # common abbreviations
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia",
    "ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt",
    "va","wa","wv","wi","wy",
}

def _contains_us_state(text: str) -> bool:
    lower = text.lower()
    return any(state in lower for state in _US_STATES)


def parse_location(text: str) -> dict:
    """
    Parse a location string into {type, cities, countries}.
    timezone always None (not inferable from listing text alone).

    Variants handled:
        "Remote"                                     → {remote, [], []}
        "Remote or Hybrid in Boston, Massachusetts"  → {hybrid, ["Boston"], ["USA"]}
        "Hybrid in New York, New York"               → {hybrid, ["New York"], ["USA"]}
        "Bayonne, New Jersey"                        → {onsite, ["Bayonne"], ["USA"]}
        "Remote or Rutherford, New Jersey"           → {hybrid, ["Rutherford"], ["USA"]}
        "Jersey City, New Jersey"                    → {onsite, ["Jersey City"], ["USA"]}
        unparseable                                  → {None, [], []}

    Rule: "remote OR city" = hybrid (wider type).
    US state present in string → countries = ["USA"].
    """
    if not text:
        return {"type": None, "timezone": None, "cities": [], "countries": []}

    t = text.strip()
    lower = t.lower()

    # Pure remote
    if lower in ("remote", "remote only", "fully remote"):
        return {"type": "remote", "timezone": None, "cities": [], "countries": []}

    countries = ["USA"] if _contains_us_state(t) else []

    # "Remote or ..." / "Remote or Hybrid in ..."
    if lower.startswith("remote or"):
        remainder = re.sub(r"^remote or\s*(hybrid\s*(in\s*)?)?", "", t, flags=re.IGNORECASE).strip()
        cities = _extract_city(remainder)
        return {"type": "hybrid", "timezone": None, "cities": cities, "countries": countries}

    # "Hybrid in ..."
    m = re.match(r"hybrid\s+(in\s+)?(.+)", t, re.IGNORECASE)
    if m:
        cities = _extract_city(m.group(2))
        return {"type": "hybrid", "timezone": None, "cities": cities, "countries": countries}

    # "Onsite in ..." / "In office ..."
    m = re.match(r"(?:onsite|on-site|in.?office)\s+(in\s+)?(.+)", t, re.IGNORECASE)
    if m:
        cities = _extract_city(m.group(2))
        return {"type": "onsite", "timezone": None, "cities": cities, "countries": countries}

    # "City, State" pattern (implicit onsite)
    # Must have at least one comma and a known US state to be confident
    if "," in t and _contains_us_state(t):
        city = t.split(",")[0].strip()
        return {"type": "onsite", "timezone": None, "cities": [city], "countries": countries}

    # Has "hybrid" anywhere
    if "hybrid" in lower:
        cities = _extract_city(t)
        return {"type": "hybrid", "timezone": None, "cities": cities, "countries": countries}

    # Has "remote" anywhere (but not matched above)
    if "remote" in lower:
        return {"type": "remote", "timezone": None, "cities": [], "countries": countries}

    # Unparseable
    return {"type": None, "timezone": None, "cities": [], "countries": []}


def _extract_city(text: str) -> list[str]:
    """
    Best-effort city extraction: take the part before the first comma.
    Does not discard cities that share a state name (e.g. "New York, New York").
    Only discards if the remaining text has NO comma (bare state name with no city).
    """
    if not text:
        return []
    parts = [p.strip() for p in text.split(",")]
    if not parts[0]:
        return []
    city = parts[0]
    # If there's only one token and it's a known state abbreviation, skip
    if len(parts) == 1 and city.lower() in _US_STATES and len(city) <= 3:
        return []
    return [city]


# ---------------------------------------------------------------------------
# parse_employment_type
# ---------------------------------------------------------------------------

def parse_employment_type(text: str) -> str | None:
    """
    Normalize employment type strings.

        "Full-time"              → "full_time"
        "Contract"               → "contract"
        "Contract, Third Party"  → "contract"
        "Full-time, Contract"    → "contract"   (more restrictive wins)
        "Part-time"              → "part_time"
        "Contract to Hire"/"C2H" → "contract_to_hire"
        else                     → None
    """
    if not text:
        return None

    lower = text.strip().lower()

    if re.search(r"contract.to.hire|c2h", lower):
        return "contract_to_hire"

    has_contract  = bool(re.search(r"\bcontract\b", lower))
    has_full_time = bool(re.search(r"full.?time", lower))
    has_part_time = bool(re.search(r"part.?time", lower))

    # Contract wins over full-time when both present ("Full-time, Contract")
    if has_contract:
        return "contract"
    if has_full_time:
        return "full_time"
    if has_part_time:
        return "part_time"

    return None


# ---------------------------------------------------------------------------
# parse_salary
# ---------------------------------------------------------------------------

def parse_salary(text: str) -> dict:
    """
    Parse salary/compensation strings.
    Conservative: ambiguous format → all None.

    Returns: {min, max, currency, interval}

    Rules:
      - "USD" or "$" present → currency = "USD", else None
      - min < 1000 → assume hourly, else annual
      - Indian comma format (e.g. "1,30,000") → all None (ambiguous)
      - "Depends on Experience" / absent → all None

    Variants:
        "USD 145,000.00 - 150,000.00 per year"  → {145000, 150000, "USD", "annual"}
        "USD 66,379.50 per year"                → {66379.5, 66379.5, "USD", "annual"}
        "$70 - $80"                             → {70, 80, "USD", "hourly"}
        "80,000 - 90,000"                       → {80000, 90000, None, "annual"}
        "1,30,000 - 1,90,000"                   → all None
    """
    _empty = {"min": None, "max": None, "currency": None, "interval": None}

    if not text:
        return _empty

    t = text.strip()
    lower = t.lower()

    # Explicit "no info" phrases
    if re.search(r"depends on|doe|not specified|competitive|negotiable", lower):
        return _empty

    # Detect currency
    currency = None
    if "usd" in lower or "$" in t:
        currency = "USD"

    # Strip currency symbols and labels for number extraction
    cleaned = re.sub(r"usd|per\s+year|per\s+hour|/yr|/hr|\$", "", t, flags=re.IGNORECASE)
    cleaned = cleaned.strip()

    # Detect Indian comma format: X,XX,XXX (group of 2 after first group of 1-3)
    if re.search(r"\d{1,3},\d{2},\d{3}", cleaned):
        return _empty

    # Extract all numbers (may have commas and decimals)
    numbers = re.findall(r"[\d,]+(?:\.\d+)?", cleaned)
    if not numbers:
        return _empty

    def to_float(s: str) -> float | None:
        try:
            return float(s.replace(",", ""))
        except ValueError:
            return None

    vals = [v for v in (to_float(n) for n in numbers) if v is not None]
    if not vals:
        return _empty

    # Infer interval from explicit text first, then from magnitude
    if re.search(r"per\s+hour|/hr|\bhr\b|\bhourly\b", t, re.IGNORECASE):
        interval = "hourly"
    elif re.search(r"per\s+year|/yr|\bannual\b|\byearly\b", t, re.IGNORECASE):
        interval = "annual"
    else:
        # Infer from magnitude: < 1000 → hourly
        interval = "hourly" if min(vals) < 1000 else "annual"

    lo = min(vals)
    hi = max(vals)

    return {"min": lo, "max": hi, "currency": currency, "interval": interval}


# ---------------------------------------------------------------------------
# guess_seniority
# ---------------------------------------------------------------------------

def guess_seniority(title: str) -> str:
    """
    Infer seniority level from job title string.
    Checks in priority order (most specific first).

    Returns one of: intern | junior | mid | senior | staff | lead | principal | manager
    """
    if not title:
        return "mid"

    lower = title.lower()

    if "intern" in lower:
        return "intern"
    if "principal" in lower:
        return "principal"
    if "staff" in lower:
        return "staff"
    if re.search(r"\blead\b|tech lead|technical lead", lower):
        return "lead"
    if re.search(r"\bmanager\b|engineering manager|\bem\b", lower):
        return "manager"
    if re.search(r"\bvice president\b|\bvp\b|\bavp\b", lower):
        return "senior"
    if re.search(r"\bsenior\b|\bsr\.?\b", lower):
        return "senior"
    if re.search(r"\bjunior\b|\bjr\.?\b", lower):
        return "junior"

    return "mid"