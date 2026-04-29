"""Pure-Python backend for Virtuus."""

from importlib import metadata as _importlib_metadata

from virtuus._python.database import Database
from virtuus._python.gsi import GSI
from virtuus._python.sort import Sort
from virtuus._python.table import Table

__all__ = ["__version__", "Database", "GSI", "Sort", "Table", "cli_version"]


def _read_version() -> str:
    return _importlib_metadata.version("virtuus")


__version__: str = _read_version()


def cli_version() -> str:
    """
    Return the library version string for CLI display.

    :return: Current Virtuus version.
    :rtype: str
    """
    return __version__
