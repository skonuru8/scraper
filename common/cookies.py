"""
cookies.py — load and sanitize browser-exported cookie JSON for Playwright.

Browser extensions (EditThisCookie, etc.) export fields Playwright doesn't
accept. This module strips them and fixes sameSite values.
"""

import json
from pathlib import Path


# Fields Playwright's context.add_cookies() does NOT accept
_STRIP_FIELDS = {"hostOnly", "session", "storeId", "id", "sameSite_"}

# Playwright sameSite valid values
_VALID_SAMESITE = {"Strict", "Lax", "None"}


def load_cookies(path: Path) -> list[dict]:
    """
    Load a browser-exported cookie JSON file and return a list of dicts
    ready to pass to Playwright's context.add_cookies().

    Transformations applied:
      - Fix sameSite: None/null/"none" → "None" (Playwright requires title-case)
      - Fix sameSite: "strict" → "Strict", "lax" → "Lax"
      - Strip non-Playwright fields: hostOnly, session, storeId, id

    Raises:
        FileNotFoundError: if the cookie file is missing (with actionable message)
        ValueError: if the file is not valid JSON or not a list
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Cookie file not found at {path}.\n"
            "Export cookies from your browser using EditThisCookie or similar,\n"
            f"save to {path}, then retry.\n"
            "See instructions.md — Step 0 for details.\n"
            "NEVER commit cookie files to git."
        )

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Cookie file at {path} is not valid JSON: {e}") from e

    if not isinstance(raw, list):
        raise ValueError(
            f"Cookie file at {path} must be a JSON array. Got: {type(raw).__name__}"
        )

    return [_sanitize_cookie(c) for c in raw]


def _sanitize_cookie(cookie: dict) -> dict:
    """
    Sanitize a single cookie dict for Playwright compatibility.
    Returns a new dict (does not mutate input).
    """
    out = {k: v for k, v in cookie.items() if k not in _STRIP_FIELDS}

    # Fix sameSite
    same_site = out.get("sameSite")
    if same_site is None or str(same_site).lower() in ("none", "null", ""):
        out["sameSite"] = "None"
    elif isinstance(same_site, str):
        # title-case it
        normalized = same_site.strip().capitalize()
        if normalized not in _VALID_SAMESITE:
            # Unknown value — default to None (permissive)
            out["sameSite"] = "None"
        else:
            out["sameSite"] = normalized

    return out