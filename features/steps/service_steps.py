"""Steps that exercise the Python local Virtuus service."""

from __future__ import annotations

import importlib
import socketserver
import tempfile
from pathlib import Path
from unittest.mock import patch

from behave import given, then, when

from virtuus import Table
from virtuus.errors import IoError, ParseError
from virtuus.service import Service


@given("a local Virtuus service with a table")
def given_local_service(context: object) -> None:
    """Open a memory-backed, file-persisted service table."""
    directory = Path(tempfile.mkdtemp())
    context.service_directory = directory
    context.local_service = Service()
    response = context.local_service.dispatch(
        {
            "action": "open_table",
            "spec": {
                "name": "records",
                "directory": str(directory),
                "primary_key": "id",
                "validation": "warn",
                "indexes": [
                    {"name": "by_status", "partition_key": "status"},
                    {"name": "by_label", "partition_key": "labels[*]"},
                    {
                        "name": "blocked_by",
                        "partition_key": (
                            "dependencies[dependency_type=blocked-by].target"
                        ),
                    },
                ],
            },
        }
    )
    assert response["ok"]
    context.local_handle = response["result"]["handle"]


@then("the local service supports retained table operations")
def then_service_operations(context: object) -> None:
    """Exercise every generic dispatch operation and its validation paths."""
    service = context.local_service
    handle = context.local_handle
    assert service._table(handle) is service._table(handle)
    assert (
        service.open_table(
            {
                "name": "records",
                "directory": str(context.service_directory),
                "primary_key": "id",
            }
        )
        == handle
    )
    service._tables[handle].last_reconcile = 0
    service._table(handle)
    assert service.dispatch({"action": "ping"})["result"]["protocol_version"] == "1.0"
    assert service.dispatch({"action": "status"})["result"]["tables"] == 1
    assert service.dispatch({"action": "get"})["ok"] is False
    assert service.dispatch({"action": "unknown", "handle": handle})["ok"] is False
    assert service.dispatch(
        {
            "action": "put",
            "handle": handle,
            "record": {
                "id": "one",
                "status": "open",
                "labels": ["core"],
                "dependencies": [{"dependency_type": "blocked-by", "target": "two"}],
            },
        }
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
    assert (
        service.dispatch(
            {"action": "query", "handle": handle, "index": "by_label", "value": "core"}
        )["result"][0]["id"]
        == "one"
    )
    assert (
        service.dispatch(
            {"action": "query", "handle": handle, "index": "blocked_by", "value": "two"}
        )["result"][0]["id"]
        == "one"
    )
    assert service.dispatch({"action": "refresh", "handle": handle})["ok"]
    assert service.dispatch({"action": "delete", "handle": handle, "pk": "two"})["ok"]
    assert service.dispatch({"action": "shutdown"})["result"]["shutdown"] is True
    _exercise_table_io_errors(context.service_directory)


def _exercise_table_io_errors(directory: Path) -> None:
    """Cover persistence failures and refresh removal behavior."""
    table = Table(
        "direct",
        primary_key="id",
        directory=str(directory),
        storage="memory",
        validation="error",
    )
    table.set_pretty_json(True)
    table.put({"id": "present"})
    table._remove_record_from_load("present")

    with patch("virtuus._python.table.os.listdir", side_effect=OSError("blocked")):
        try:
            table.load_from_dir()
        except IoError:
            pass
        else:
            raise AssertionError("expected an IoError from listdir")

    unreadable = directory / "unreadable.json"
    unreadable.write_text('{"id": "unreadable"}')
    with patch("builtins.open", side_effect=OSError("blocked")):
        try:
            table.load_from_dir()
        except IoError:
            pass
        else:
            raise AssertionError("expected an IoError from open")
    table.validation = "warn"
    with patch("builtins.open", side_effect=OSError("blocked")):
        table.load_from_dir()
    assert table.warnings

    unreadable.write_text("not json")
    table.validation = "error"
    try:
        table.load_from_dir()
    except ParseError:
        pass
    else:
        raise AssertionError("expected a ParseError from invalid JSON")
    table.validation = "warn"
    table.load_from_dir()
    assert len(table.warnings) > 1

    with patch("virtuus._python.table.os.makedirs", side_effect=OSError("blocked")):
        try:
            table._write_record_to_disk("write-error", {"id": "write-error"})
        except IoError:
            pass
        else:
            raise AssertionError("expected an IoError from makedirs")

    deleted = directory / "delete-error.json"
    deleted.write_text('{"id": "delete-error"}')
    with patch("virtuus._python.table.os.remove", side_effect=OSError("blocked")):
        try:
            table._delete_record_from_disk("delete-error")
        except IoError:
            pass
        else:
            raise AssertionError("expected an IoError from remove")

    with patch(
        "virtuus._python.table.tempfile.mkstemp", side_effect=OSError("blocked")
    ):
        try:
            table._write_json_atomic(str(directory / "atomic-error.json"), {"id": "x"})
        except IoError:
            pass
        else:
            raise AssertionError("expected an IoError from mkstemp")


def _reload_virtuus_service_modules() -> None:
    import virtuus
    import virtuus.service

    importlib.reload(virtuus.service)
    importlib.reload(virtuus)


@given("the platform does not provide Unix domain socket servers")
def given_platform_without_unix_sockets(context: object) -> None:
    """Hide the Unix-only socketserver class for the rest of the scenario."""
    original_server = socketserver.ThreadingUnixStreamServer
    del socketserver.ThreadingUnixStreamServer

    def restore_unix_socket_server() -> None:
        socketserver.ThreadingUnixStreamServer = original_server
        _reload_virtuus_service_modules()

    context.add_cleanup(restore_unix_socket_server)


@when("the virtuus package is imported")
def when_virtuus_imported(context: object) -> None:
    """Re-import the virtuus package and its service module."""
    _reload_virtuus_service_modules()
    import virtuus

    context.imported_virtuus = virtuus


@then("the virtuus package exposes the local service")
def then_virtuus_exposes_service(context: object) -> None:
    """Assert the service is usable without a Unix socket host."""
    virtuus = context.imported_virtuus
    assert virtuus.PROTOCOL_VERSION == "1.0"
    assert virtuus.Service().dispatch({"action": "ping"})["ok"] is True


@then('starting a Unix service fails with "{message}"')
def then_unix_service_fails(context: object, message: str) -> None:
    """Assert the Unix-socket host reports the missing platform support."""
    virtuus = context.imported_virtuus
    try:
        virtuus.UnixService(Path(tempfile.mkdtemp()) / "virtuus.sock")
    except OSError as error:
        assert str(error) == message
    else:
        raise AssertionError("expected UnixService to fail without Unix sockets")
