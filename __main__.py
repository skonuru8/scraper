"""
__main__.py — enables `python -m scraper`.

All logic lives in cli.py. This file is the entry point Python calls
when the package is run as a module.
"""

import sys
from scraper.cli import main

sys.exit(main())
