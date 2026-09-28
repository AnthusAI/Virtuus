"""Local JSON-lines service for retained Virtuus tables."""

from __future__ import annotations

import json
import argparse
import socketserver
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from virtuus import Table

PROTOCOL_VERSION = "1.0"


@dataclass
class _ResidentTable:
    table: Table
    reconcile_seconds: float
    last_reconcile: float


class Service:
    """Own memory-resident, file-backed tables for local clients."""

    def __init__(self) -> None:
        self._tables: dict[str, _ResidentTable] = {}

    @staticmethod
    def _handle(spec: dict[str, Any]) -> str:
        return f"{Path(spec['directory']).resolve()}:{spec['name']}"

    def open_table(self, spec: dict[str, Any]) -> str:
        """Open a table from a declarative specification."""
        handle = self._handle(spec)
        if handle in self._tables:
            return handle
        table = Table(
            spec["name"],
            primary_key=spec["primary_key"],
            directory=str(spec["directory"]),
            validation="error",
            storage="memory",
            check_interval=int(spec.get("reconcile_seconds", 2)),
            pretty_json=bool(spec.get("pretty_json", False)),
        )
        for index in spec.get("indexes", []):
            table.add_gsi(index["name"], index["partition_key"], index.get("sort_key"))
        table.load_from_dir()
        self._tables[handle] = _ResidentTable(
            table=table,
            reconcile_seconds=float(spec.get("reconcile_seconds", 2)),
            last_reconcile=time.monotonic(),
        )
        return handle

    def _table(self, handle: str) -> Table:
        resident = self._tables[handle]
        if time.monotonic() - resident.last_reconcile >= resident.reconcile_seconds:
            resident.table.refresh()
            resident.last_reconcile = time.monotonic()
        return resident.table

    def dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        """Process one protocol request."""
        try:
            action = request["action"]
            if action == "ping":
                result: Any = {"protocol_version": PROTOCOL_VERSION}
            elif action == "status":
                result = {"protocol_version": PROTOCOL_VERSION, "tables": len(self._tables)}
            elif action == "shutdown":
                result = {"shutdown": True}
            elif action == "open_table":
                result = {"handle": self.open_table(request["spec"])}
            else:
                table = self._table(request["handle"])
                if action == "get":
                    result = table.get(request["pk"])
                elif action == "scan":
                    result = table.scan()
                elif action == "query":
                    result = table.query_gsi(request["index"], request["value"])
                elif action == "put":
                    table.put(request["record"])
                    result = None
                elif action == "put_many":
                    records = request["records"]
                    table.bulk_load(records)
                    result = {"count": len(records)}
                elif action == "delete":
                    table.delete(request["pk"])
                    result = None
                elif action == "refresh":
                    result = table.refresh()
                else:
                    raise ValueError(f"unknown action: {action}")
            return {"ok": True, "result": result}
        except (KeyError, ValueError, TypeError) as error:
            return {"ok": False, "error": str(error)}


class _RequestHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        raw = self.rfile.readline()
        if not raw:
            return
        request = json.loads(raw)
        response = self.server.service.dispatch(request)  # type: ignore[attr-defined]
        self.wfile.write(json.dumps(response).encode("utf-8") + b"\n")
        if request.get("action") == "shutdown":
            self.server.shutdown()  # type: ignore[attr-defined]


class UnixService(socketserver.ThreadingUnixStreamServer):
    """Unix-domain-socket host for :class:`Service`."""

    def __init__(self, socket_path: Path) -> None:
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        if socket_path.exists():
            socket_path.unlink()
        self.service = Service()
        super().__init__(str(socket_path), _RequestHandler)


def main() -> None:
    """Run the native Python service entry point."""
    parser = argparse.ArgumentParser(description="Virtuus local table service")
    parser.add_argument("--socket", required=True, type=Path)
    args = parser.parse_args()
    with UnixService(args.socket) as server:
        server.serve_forever()


if __name__ == "__main__":  # pragma: no cover - command entry point
    main()
