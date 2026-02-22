"""Virtuus — file-backed in-memory indexed table engine."""

from __future__ import annotations

import os

from virtuus._python import (
    GSI,
    Database,
    Table,
    __version__,
)
from virtuus._python import (
    cli_version as _py_cli_version,
)
from virtuus._python.sort import Sort

_backend = os.getenv("VIRTUUS_BACKEND", "auto").lower()

if _backend == "rust":  # pragma: no cover - env-specific
    from virtuus._rust import GSI, Database, Table, __version__  # pragma: no cover
elif _backend != "python":  # pragma: no cover - env-specific
    try:  # pragma: no cover
        from virtuus._rust import GSI, Database, Table, __version__  # pragma: no cover
    except Exception:  # noqa: BLE001  # pragma: no cover
        pass


def cli_version() -> str:
    """
    Return the library version string for CLI display.

    :return: Current Virtuus version.
    :rtype: str
    """
    return _py_cli_version()


__all__ = ["__version__", "Database", "GSI", "Sort", "Table", "cli_version"]
