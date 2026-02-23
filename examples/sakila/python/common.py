from __future__ import annotations

from pathlib import Path
import os

os.environ.setdefault("VIRTUUS_BACKEND", "python")

from virtuus import Database


def load_db() -> Database:
    base_dir = Path(__file__).resolve().parents[1]
    schema_path = base_dir / "schema.yml"
    return Database.from_schema(str(schema_path), str(base_dir))
