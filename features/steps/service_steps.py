"""Steps that exercise the Python local Virtuus service."""

from __future__ import annotations

import tempfile
from pathlib import Path

from behave import given, then

from virtuus.service import Service


@given("a local Virtuus service with a table")
def given_local_service(context: object) -> None:
    """Open a memory-backed, file-persisted service table."""
    directory = Path(tempfile.mkdtemp())
    context.service_directory = directory
    context.local_service = Service()
    context.local_handle = context.local_service.open_table(
        {
            "name": "records",
            "directory": str(directory),
            "primary_key": "id",
            "validation": "warn",
            "indexes": [{"name": "by_status", "partition_key": "status"}],
        }
    )


@then("the local service supports retained table operations")
def then_service_operations(context: object) -> None:
    """Exercise every generic dispatch operation and its validation paths."""
    service = context.local_service
    handle = context.local_handle
    assert service._table(handle) is service._table(handle)
    assert service.dispatch({"action": "ping"})["result"]["protocol_version"] == "1.0"
    assert service.dispatch({"action": "status"})["result"]["tables"] == 1
    assert service.dispatch({"action": "get"})["ok"] is False
    assert service.dispatch({"action": "unknown", "handle": handle})["ok"] is False
    assert service.dispatch(
        {"action": "put", "handle": handle, "record": {"id": "one", "status": "open"}}
    )["ok"]
    assert service.dispatch(
        {
            "action": "put_many",
            "handle": handle,
            "records": [{"id": "two", "status": "open"}],
        }
    )["ok"]
    assert (
        service.dispatch({"action": "get", "handle": handle, "pk": "one"})["result"][
            "id"
        ]
        == "one"
    )
    assert len(service.dispatch({"action": "scan", "handle": handle})["result"]) == 2
    assert service.dispatch(
        {"action": "query", "handle": handle, "index": "by_status", "value": "open"}
    )["ok"]
    assert service.dispatch({"action": "refresh", "handle": handle})["ok"]
    assert service.dispatch({"action": "delete", "handle": handle, "pk": "two"})["ok"]
    assert service.dispatch({"action": "shutdown"})["result"]["shutdown"] is True
