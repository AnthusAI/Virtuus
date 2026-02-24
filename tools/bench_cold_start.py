#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import duckdb  # type: ignore
except Exception:
    duckdb = None

try:
    from tinydb import TinyDB, Query  # type: ignore
except Exception:
    TinyDB = None
    Query = None

from features.steps import benchmark_steps as bench  # type: ignore


@dataclass
class Fixture:
    root: Path
    total: int
    record_size_kb: float
    search_term: str
    pk_value: str


def _parse_list(value: str, cast, default):
    if not value:
        return default
    out = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(cast(part))
    return out or default


def _parse_int_list(value: str, default: list[int]) -> list[int]:
    return _parse_list(value, int, default)


def _parse_float_list(value: str, default: list[float]) -> list[float]:
    return _parse_list(value, float, default)


def _instance_type() -> str:
    return os.getenv("VIRTUUS_BENCH_INSTANCE_TYPE", "local")


def _fixture_dir(base: Path, total: int, record_size_kb: float) -> Path:
    return base / f"single_table_total_{total}_record_{record_size_kb:g}kb"


def _iter_records(docs_dir: Path):
    for path in sorted(docs_dir.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            yield json.load(handle)


def _ensure_fixture(base: Path, total: int, record_size_kb: float) -> Fixture:
    root = _fixture_dir(base, total, record_size_kb)
    meta_path = root / "meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return Fixture(
            root=root,
            total=int(meta["total"]),
            record_size_kb=float(meta["record_size_kb"]),
            search_term=str(meta["search_term"]),
            pk_value=str(meta["pk_value"]),
        )

    root.mkdir(parents=True, exist_ok=True)
    ctx = SimpleNamespace()
    ctx.bench_total_records_target = total
    ctx.bench_record_size_kb = record_size_kb
    bench._generate_single_table(ctx, root, 1)
    search_term = "doc 1"
    pk_value = "doc-1"
    meta = {
        "total": total,
        "record_size_kb": record_size_kb,
        "search_term": search_term,
        "pk_value": pk_value,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return Fixture(root=root, total=total, record_size_kb=record_size_kb, search_term=search_term, pk_value=pk_value)


def _ensure_sqlite_db(fixture: Fixture, db_path: Path) -> None:
    if db_path.exists():
        return
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE documents (id TEXT PRIMARY KEY, title TEXT, body TEXT, category TEXT, created_at TEXT)"
    )
    conn.execute(
        "CREATE VIRTUAL TABLE documents_fts USING fts5(id, title, body, content='documents', content_rowid='rowid')"
    )
    docs_dir = fixture.root / "documents"
    for record in _iter_records(docs_dir):
        conn.execute(
            "INSERT INTO documents (id, title, body, category, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                record.get("id"),
                record.get("title"),
                record.get("body"),
                record.get("category"),
                record.get("created_at"),
            ),
        )
    conn.execute(
        "INSERT INTO documents_fts(rowid, id, title, body) SELECT rowid, id, title, body FROM documents"
    )
    conn.commit()
    conn.close()


def _ensure_duckdb_db(fixture: Fixture, db_path: Path) -> None:
    if db_path.exists():
        return
    if duckdb is None:
        raise RuntimeError("duckdb is not installed")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE documents (id VARCHAR, title VARCHAR, body VARCHAR, category VARCHAR, created_at VARCHAR)"
    )
    docs_dir = fixture.root / "documents"
    for record in _iter_records(docs_dir):
        conn.execute(
            "INSERT INTO documents VALUES (?, ?, ?, ?, ?)",
            (
                record.get("id"),
                record.get("title"),
                record.get("body"),
                record.get("category"),
                record.get("created_at"),
            ),
        )
    conn.close()


def _ensure_tinydb_db(fixture: Fixture, db_path: Path) -> None:
    if db_path.exists():
        return
    if TinyDB is None:
        raise RuntimeError("tinydb is not installed")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = TinyDB(str(db_path))
    docs_dir = fixture.root / "documents"
    for record in _iter_records(docs_dir):
        db.insert(record)
    db.close()


def _run_worker(args: argparse.Namespace) -> None:
    start = time.perf_counter()
    result_count = 0
    if args.engine == "sqlite":
        conn = sqlite3.connect(args.db_path)
        if args.query == "pk_lookup":
            result = conn.execute(
                "SELECT id FROM documents WHERE id = ?", (args.pk_value,)
            ).fetchone()
            result_count = 1 if result else 0
        else:
            rows = conn.execute(
                "SELECT id FROM documents_fts WHERE documents_fts MATCH ? LIMIT 20",
                (args.search_term,),
            ).fetchall()
            result_count = len(rows)
        conn.close()
    elif args.engine == "duckdb":
        if duckdb is None:
            raise RuntimeError("duckdb is not installed")
        conn = duckdb.connect(args.db_path)
        if args.query == "pk_lookup":
            rows = conn.execute(
                "SELECT id FROM documents WHERE id = ?", (args.pk_value,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id FROM documents WHERE body ILIKE ? LIMIT 20",
                (f"%{args.search_term}%",),
            ).fetchall()
        result_count = len(rows)
        conn.close()
    elif args.engine == "tinydb":
        if TinyDB is None or Query is None:
            raise RuntimeError("tinydb is not installed")
        db = TinyDB(args.db_path)
        docs = db.table("_default")
        query = Query()
        if args.query == "pk_lookup":
            rows = docs.search(query.id == args.pk_value)
        else:
            rows = docs.search(query.body.test(lambda v: args.search_term in (v or "")))
        result_count = len(rows)
        db.close()
    elif args.engine == "virtuus":
        from virtuus import Database, Table  # type: ignore

        docs_dir = Path(args.data_root) / "documents"
        db = Database()
        table = Table(
            "documents",
            primary_key="id",
            directory=str(docs_dir),
            storage=args.storage_mode,
            search_fields=["title", "body"],
        )
        table.load_from_dir()
        db.add_table("documents", table)
        if args.query == "pk_lookup":
            result = table.get(args.pk_value)
            result_count = 1 if result is not None else 0
        else:
            result_count = len(list(table.search(args.search_term)))
    else:
        raise RuntimeError(f"unknown engine {args.engine}")

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    payload = {"elapsed_ms": elapsed_ms, "result_count": result_count}
    print(json.dumps(payload))


def _compute_percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    if pct <= 0:
        return values[0]
    if pct >= 100:
        return values[-1]
    rank = (pct / 100.0) * (len(values) - 1)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    weight = rank - low
    return values[low] * (1 - weight) + values[high] * weight


def _run_bench() -> None:
    out_dir = Path(os.getenv("VIRTUUS_COLD_DIR", "benchmarks/output_cold_start"))
    out_dir.mkdir(parents=True, exist_ok=True)
    fixtures_root = out_dir / "fixtures"
    db_root = out_dir / "databases"

    totals = _parse_int_list(os.getenv("VIRTUUS_COLD_TOTALS", "10000,100000"), [10000, 100000])
    record_sizes = _parse_float_list(os.getenv("VIRTUUS_COLD_RECORD_SIZES_KB", "0.5,2"), [0.5, 2.0])
    iterations = int(os.getenv("VIRTUUS_COLD_ITERATIONS", "5"))

    engines = _parse_list(
        os.getenv("VIRTUUS_COLD_ENGINES", "sqlite,duckdb,tinydb,virtuus"),
        str,
        ["sqlite", "duckdb", "tinydb", "virtuus"],
    )
    queries = _parse_list(
        os.getenv("VIRTUUS_COLD_QUERIES", "pk_lookup,search_single_term"),
        str,
        ["pk_lookup", "search_single_term"],
    )
    storage_modes = _parse_list(
        os.getenv("VIRTUUS_COLD_STORAGE_MODES", "index_only,memory"),
        str,
        ["index_only", "memory"],
    )

    results_path = out_dir / "results.json"
    if results_path.exists():
        results: list[dict[str, Any]] = json.loads(results_path.read_text(encoding="utf-8"))
    else:
        results = []

    seen: set[tuple[str, str, str | None, float, int, str]] = set()
    for entry in results:
        meta = entry.get("metadata", {}) or {}
        seen.add(
            (
                entry.get("name"),
                meta.get("engine"),
                meta.get("storage_mode"),
                float(meta.get("record_size_kb")) if meta.get("record_size_kb") is not None else 0.0,
                int(meta.get("total_records")) if meta.get("total_records") is not None else 0,
                meta.get("instance_type") or "local",
            )
        )

    for total in totals:
        for record_size_kb in record_sizes:
            fixture = _ensure_fixture(fixtures_root, total, record_size_kb)
            sqlite_path = db_root / f"sqlite_total_{total}_record_{record_size_kb:g}kb.db"
            duckdb_path = db_root / f"duckdb_total_{total}_record_{record_size_kb:g}kb.duckdb"
            tinydb_path = db_root / f"tinydb_total_{total}_record_{record_size_kb:g}kb.json"

            if "sqlite" in engines:
                _ensure_sqlite_db(fixture, sqlite_path)
            if "duckdb" in engines:
                _ensure_duckdb_db(fixture, duckdb_path)
            if "tinydb" in engines:
                _ensure_tinydb_db(fixture, tinydb_path)

            for engine in engines:
                for query in queries:
                    mode_list = storage_modes if engine == "virtuus" else [None]
                    for storage_mode in mode_list:
                        key = (
                            f"cold_start_{query}",
                            engine,
                            storage_mode,
                            record_size_kb,
                            total,
                            _instance_type(),
                        )
                        if key in seen:
                            continue
                        timings: list[float] = []
                        for _ in range(iterations):
                            worker_args = [
                                sys.executable,
                                str(Path(__file__).resolve()),
                                "--worker",
                                "--engine",
                                engine,
                                "--query",
                                query,
                                "--data-root",
                                str(fixture.root),
                                "--pk-value",
                                fixture.pk_value,
                                "--search-term",
                                fixture.search_term,
                            ]
                            if engine == "sqlite":
                                worker_args += ["--db-path", str(sqlite_path)]
                            elif engine == "duckdb":
                                worker_args += ["--db-path", str(duckdb_path)]
                            elif engine == "tinydb":
                                worker_args += ["--db-path", str(tinydb_path)]
                            else:
                                worker_args += ["--storage-mode", storage_mode or "index_only"]
                            env = os.environ.copy()
                            env["PYTHONPATH"] = str(ROOT / "python" / "src")
                            proc = subprocess.run(
                                worker_args,
                                check=True,
                                capture_output=True,
                                text=True,
                                env=env,
                            )
                            payload = json.loads(proc.stdout.strip())
                            timings.append(float(payload["elapsed_ms"]))

                        entry = {
                            "name": f"cold_start_{query}",
                            "metadata": {
                                "engine": engine,
                                "storage_mode": storage_mode,
                                "total_records": total,
                                "record_size_kb": record_size_kb,
                                "p50": _compute_percentile(timings, 50),
                                "p95": _compute_percentile(timings, 95),
                                "p99": _compute_percentile(timings, 99),
                                "iterations": iterations,
                                "instance_type": _instance_type(),
                            },
                        }
                        results.append(entry)
                        results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    csv_path = out_dir / "results.csv"
    rows = [
        "name,engine,storage_mode,total_records,record_size_kb,p50,p95,p99,iterations,instance_type"
    ]
    for entry in results:
        meta = entry.get("metadata", {}) or {}
        rows.append(
            ",".join(
                [
                    str(entry.get("name")),
                    str(meta.get("engine", "")),
                    str(meta.get("storage_mode", "")),
                    str(meta.get("total_records", "")),
                    str(meta.get("record_size_kb", "")),
                    str(meta.get("p50", "")),
                    str(meta.get("p95", "")),
                    str(meta.get("p99", "")),
                    str(meta.get("iterations", "")),
                    str(meta.get("instance_type", "")),
                ]
            )
        )
    csv_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"Wrote {results_path} and {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--engine")
    parser.add_argument("--query")
    parser.add_argument("--data-root")
    parser.add_argument("--db-path")
    parser.add_argument("--pk-value")
    parser.add_argument("--search-term")
    parser.add_argument("--storage-mode", default="index_only")
    args = parser.parse_args()

    if args.worker:
        _run_worker(args)
    else:
        _run_bench()


if __name__ == "__main__":
    main()
