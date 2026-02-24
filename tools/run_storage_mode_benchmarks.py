#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
import multiprocessing as mp
import time
import signal
import traceback
import tempfile
import resource

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from features.steps import benchmark_steps as b  # type: ignore


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


def _parse_str_list(value: str, default: list[str]) -> list[str]:
    return _parse_list(value, str, default)


def _parse_int_list(value: str, default: list[int]) -> list[int]:
    return _parse_list(value, int, default)


def _parse_float_list(value: str, default: list[float]) -> list[float]:
    return _parse_list(value, float, default)


def _rss_kb() -> int:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # On macOS ru_maxrss is bytes; on Linux it's kilobytes.
        rss = int(usage.ru_maxrss)
        if rss > 0 and rss < 1024:  # unlikely real RSS in bytes; assume already KB
            return rss
        # Heuristic: if very large, assume bytes and convert to KB.
        return rss // 1024
    except Exception:
        try:
            output = subprocess.check_output(
                ["ps", "-o", "rss=", "-p", str(os.getpid())],
                text=True,
            )
            return int(output.strip() or 0)
        except Exception:
            return -1


def _memory_probe(ctx_dict: dict, queue: mp.Queue) -> None:
    ctx = SimpleNamespace(**ctx_dict)
    _ = b._load_warm_db(ctx)
    queue.put(_rss_kb())


def measure_memory_kb(ctx: SimpleNamespace) -> int:
    if os.getenv("VIRTUUS_BENCH_SKIP_RSS") == "1":
        return -1
    ctx_dict = ctx.__dict__.copy()
    # Avoid pickling heavy/non-serializable objects (database instances, temp dirs).
    ctx_dict.pop("bench_db", None)
    ctx_dict.pop("bench_tmp", None)
    queue: mp.Queue = mp.Queue()
    proc = mp.get_context("spawn").Process(target=_memory_probe, args=(ctx_dict, queue))
    proc.start()
    proc.join()
    if proc.exitcode != 0:
        raise RuntimeError("memory probe failed")
    return int(queue.get())


def _entry_metrics(entry: dict) -> dict:
    metrics = {"p50": None, "p95": None, "p99": None, "timing_ms": None}
    if "timing_ms" in entry:
        metrics["timing_ms"] = entry["timing_ms"]
    meta = entry.get("metadata", {}) or {}
    for key in ("p50", "p95", "p99"):
        if key in meta:
            metrics[key] = meta[key]
    return metrics


def run() -> None:
    timeout_s = int(os.getenv("VIRTUUS_BENCH_TIMEOUT", "120"))
    if not os.getenv("VIRTUUS_BENCH_DIR"):
        tmp_root = Path(tempfile.mkdtemp(prefix="virtuus_bench_"))
        os.environ["VIRTUUS_BENCH_DIR"] = str(tmp_root)
    storage_modes = _parse_str_list(
        os.getenv("VIRTUUS_BENCH_STORAGE_MODES", "index_only,memory"),
        ["index_only", "memory"],
    )
    record_sizes = _parse_float_list(
        os.getenv("VIRTUUS_BENCH_RECORD_SIZES_KB", "0.5,2,10"),
        [0.5, 2.0, 10.0],
    )
    dataset_shapes = _parse_str_list(
        os.getenv("VIRTUUS_BENCH_DATASET_SHAPES", "single_table,social_media"),
        ["single_table", "social_media"],
    )
    totals = _parse_int_list(
        os.getenv("VIRTUUS_BENCH_TOTALS", "10000,50000"), [10000, 50000]
    )
    backends = _parse_str_list(
        os.getenv("VIRTUUS_BENCH_BACKENDS", "rust,python"), ["rust", "python"]
    )

    search_iters = int(os.getenv("VIRTUUS_BENCH_SEARCH_ITERATIONS", "500"))
    pk_iters = int(os.getenv("VIRTUUS_BENCH_PK_ITERATIONS", "100"))
    gsi_iters = int(os.getenv("VIRTUUS_BENCH_GSI_ITERATIONS", "100"))
    scan_iters = int(os.getenv("VIRTUUS_BENCH_SCAN_ITERATIONS", "20"))

    out_root = ROOT / "benchmarks" / "output_storage"
    out_root.mkdir(parents=True, exist_ok=True)

    for backend in backends:
        for shape in dataset_shapes:
            print(f"[bench] backend={backend} profile={shape}")
            output_dir = out_root / backend / shape
            output_dir.mkdir(parents=True, exist_ok=True)
            os.environ["VIRTUUS_BENCH_DIR"] = str(output_dir)

            results_path = output_dir / "results.json"
            if results_path.exists():
                try:
                    results: list[dict] = json.loads(
                        results_path.read_text(encoding="utf-8")
                    )
                except Exception:
                    results = []
            else:
                results = []

            seen: set[tuple] = set()
            for entry in results:
                meta = entry.get("metadata", {}) or {}
                seen.add(
                    (
                        entry.get("name"),
                        meta.get("storage_mode"),
                        meta.get("record_size_kb"),
                        meta.get("total_records"),
                        meta.get("profile"),
                        meta.get("backend"),
                    )
                )

            for total in totals:
                for record_size_kb in record_sizes:
                    print(
                        f"[bench] combo start backend={backend} profile={shape} total={total} record_size_kb={record_size_kb}"
                    )
                    # seed fixtures once per size
                    base_ctx = SimpleNamespace()
                    base_ctx.bench_backend = backend
                    base_ctx.bench_profile = shape
                    base_ctx.bench_scale = 1
                    base_ctx.bench_total_records_target = total
                    base_ctx.bench_record_size_kb = record_size_kb
                    base_ctx.bench_search_enabled = True

                    def run_with_alarm(fn, status_name):
                        start = time.time()

                        def handler(signum, frame):  # pragma: no cover - timer
                            raise TimeoutError("timeout")

                        old = signal.signal(signal.SIGALRM, handler)
                        signal.alarm(timeout_s)
                        status = None
                        error = None
                        try:
                            fn()
                        except TimeoutError as exc:
                            status = "timeout"
                            error = str(exc)
                        except Exception as exc:  # noqa: BLE001
                            status = "failed"
                            error = str(exc)
                        finally:
                            signal.alarm(0)
                            signal.signal(signal.SIGALRM, old)
                        if status:
                            results.append(
                                {
                                    "name": status_name,
                                    "status": status,
                                    "error": error,
                                    "elapsed_ms": (time.time() - start) * 1000,
                                    "metadata": {
                                        "storage_mode": None,
                                        "record_size_kb": record_size_kb,
                                        "profile": shape,
                                        "backend": backend,
                                        "total_records": total,
                                    },
                                }
                            )
                            return False
                        return True

                    if not run_with_alarm(
                        lambda: b._ensure_fixtures(base_ctx), "fixtures"
                    ):
                        continue

                    for storage_mode in storage_modes:
                        combo_base = (
                            storage_mode,
                            record_size_kb,
                            total,
                            shape,
                            backend,
                        )
                        ctx = SimpleNamespace(**base_ctx.__dict__)
                        ctx.bench_storage_mode = storage_mode
                        ctx.bench_db = None

                        def record(entry):
                            key = (
                                entry.get("name"),
                                *combo_base,
                            )
                            if key not in seen:
                                results.append(entry)
                                seen.add(key)
                                # Persist incrementally so partial progress survives interruptions
                                results_path.write_text(
                                    json.dumps(results, indent=2), encoding="utf-8"
                                )

                        meta_common = {
                            "storage_mode": storage_mode,
                            "record_size_kb": record_size_kb,
                            "profile": shape,
                            "backend": backend,
                            "total_records": total,
                        }

                        record(
                            {
                                "name": "combo_start",
                                "status": "ok",
                                "metadata": dict(meta_common),
                            }
                        )

                        def run_with_timeout(step_fn, *args):
                            start = time.time()

                            def handler(signum, frame):  # pragma: no cover - timer
                                raise TimeoutError("benchmark timeout")

                            old = signal.signal(signal.SIGALRM, handler)
                            signal.alarm(timeout_s)
                            status = None
                            error = None
                            entry = None
                            try:
                                step_fn(*args)
                                entry = ctx.last_benchmark
                            except TimeoutError as exc:
                                status = "timeout"
                                error = str(exc)
                            except Exception as exc:  # noqa: BLE001
                                status = "failed"
                                error = str(exc)
                            finally:
                                signal.alarm(0)
                                signal.signal(signal.SIGALRM, old)
                            elapsed_ms = (time.time() - start) * 1000
                            if status is None and entry:
                                print(
                                    f"[bench] ok name={entry.get('name')} storage={storage_mode} total={total} size={record_size_kb} backend={backend}"
                                )
                                return entry
                            bench_label = args[1] if len(args) > 1 else "benchmark"
                            print(
                                f"[bench] {status or 'failed'} name={bench_label} storage={storage_mode} total={total} size={record_size_kb} backend={backend} err={error}"
                            )
                            return {
                                "name": args[1] if len(args) > 1 else "benchmark",
                                "status": status or "failed",
                                "error": error,
                                "elapsed_ms": elapsed_ms,
                                "metadata": dict(meta_common),
                            }

                        for bench_name, iters in (
                            ("search_single_term", search_iters),
                            ("search_multi_term", search_iters),
                            ("pk_lookup", pk_iters),
                            ("gsi_partition_lookup", gsi_iters),
                            ("gsi_sorted_query", gsi_iters),
                            ("scan", scan_iters),
                        ):
                            print(
                                f"[bench] start name={bench_name} storage={storage_mode} total={total} size={record_size_kb} backend={backend}"
                            )
                            entry = run_with_timeout(
                                b.step_run_benchmark_iterations, ctx, bench_name, iters
                            )
                            record(entry)

                        print(
                            f"[bench] start name=incremental_refresh storage={storage_mode} total={total} size={record_size_kb} backend={backend}"
                        )
                        entry = run_with_timeout(
                            b.step_run_incremental_refresh, ctx, "incremental_refresh"
                        )
                        record(entry)

                        if not os.getenv("VIRTUUS_BENCH_SKIP_MEMORY"):
                            rss_kb = measure_memory_kb(ctx)
                            mem_entry = {
                                "name": "memory_rss",
                                "metadata": {
                                    "rss_kb": rss_kb,
                                    "rss_bytes": rss_kb * 1024,
                                    "storage_mode": storage_mode,
                                    "record_size_kb": record_size_kb,
                                    "profile": shape,
                                    "backend": backend,
                                    "total_records": total,
                                },
                            }
                            key = (
                                mem_entry["name"],
                                *combo_base,
                            )
                            if key not in seen:
                                results.append(mem_entry)
                                seen.add(key)

            results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

            csv_lines = [
                "backend,profile,storage_mode,record_size_kb,total_records,name,p50,p95,p99,timing_ms,rss_kb"
            ]
            for entry in results:
                meta = entry.get("metadata", {}) or {}
                metrics = _entry_metrics(entry)
                csv_lines.append(
                    ",".join(
                        [
                            str(meta.get("backend", backend)),
                            str(meta.get("profile", shape)),
                            str(meta.get("storage_mode", "")),
                            str(meta.get("record_size_kb", "")),
                            str(meta.get("total_records", "")),
                            str(entry.get("name", "")),
                            str(metrics.get("p50", "")),
                            str(metrics.get("p95", "")),
                            str(metrics.get("p99", "")),
                            str(metrics.get("timing_ms", "")),
                            str(meta.get("rss_kb", "")),
                        ]
                    )
                )
            (output_dir / "results.csv").write_text(
                "\n".join(csv_lines) + "\n", encoding="utf-8"
            )
            print(f"Wrote {results_path}")


if __name__ == "__main__":
    run()
