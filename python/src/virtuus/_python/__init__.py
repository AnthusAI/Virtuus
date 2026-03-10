"""Pure-Python backend for Virtuus."""

import os as _os
import re as _re

from virtuus._python.database import Database
from virtuus._python.gsi import GSI
from virtuus._python.sort import Sort
from virtuus._python.table import Table

__all__ = ["__version__", "Database", "GSI", "Sort", "Table", "cli_version"]


def _read_version() -> str:
    _here = _os.path.dirname(_os.path.abspath(__file__))
    _version_file = _os.path.normpath(
        _os.path.join(_here, "..", "..", "..", "..", "VERSION")
    )
    with open(_version_file) as _f:
        _raw = _f.read()
    return _re.search(r"\b\d+\.\d+\.\d+\b", _raw).group(0)


__version__: str = _read_version()


def cli_version() -> str:
    """
    Return the library version string for CLI display.

    :return: Current Virtuus version.
    :rtype: str
    """
    return __version__
