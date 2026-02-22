"""Virtuus — file-backed in-memory indexed table engine."""

from __future__ import annotations

import os

from virtuus._python.sort import Sort

_backend = os.getenv("VIRTUUS_BACKEND", "auto").lower()

if _backend == "python":
    from virtuus._python import GSI, Database, Table, __version__
elif _backend == "rust":
    from virtuus._rust import GSI, Database, Table, __version__
else:
    try:
        from virtuus._rust import GSI, Database, Table, __version__
    except Exception:  # noqa: BLE001
        from virtuus._python import GSI, Database, Table, __version__

__all__ = ["__version__", "Database", "GSI", "Sort", "Table"]
