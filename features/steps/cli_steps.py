from __future__ import annotations

import json
import shlex
import subprocess
import tempfile
from pathlib import Path

from behave import given, then, when


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ensure_cli_root(context) -> Path:
    if not hasattr(context, "cli_tmp"):
        context.cli_tmp = tempfile.TemporaryDirectory()
        context.cli_root = Path(context.cli_tmp.name)
    return context.cli_root


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _run_virtuus(context, args: str) -> None:
    root = _ensure_cli_root(context)
    binary = _repo_root() / "rust" / "target" / "debug" / "virtuus"
    if not binary.exists():
        subprocess.run(
            ["cargo", "build"],
            cwd=_repo_root() / "rust",
            check=True,
        )
    command = [str(binary)] + shlex.split(args)
    result = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    context.cli_stdout = result.stdout
    context.cli_stderr = result.stderr
    context.cli_status = result.returncode


@given('a data directory with a "users" folder containing JSON files')
def step_data_users(context):
    root = _ensure_cli_root(context)
    data_dir = root / "data" / "users"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        data_dir / "alice.json",
        {"id": "user-1", "name": "Alice", "email": "alice@example.com"},
    )
    _write_json(
        data_dir / "bob.json",
        {"id": "user-2", "name": "Bob", "email": "bob@example.com"},
    )


@given("a data directory and a schema.yml file")
def step_data_schema(context):
    root = _ensure_cli_root(context)
    data_dir = root / "data" / "users"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_json(data_dir / "user-1.json", {"id": "user-1", "name": "Alice"})
    schema = "\n".join(
        [
            "tables:",
            "  users:",
            "    primary_key: id",
            "    directory: users",
            "",
        ]
    )
    (root / "schema.yml").write_text(schema, encoding="utf-8")


@given('a data directory with a "users" folder')
def step_data_users_empty(context):
    root = _ensure_cli_root(context)
    data_dir = root / "data" / "users"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        data_dir / "bob.json",
        {"id": "user-2", "name": "Bob", "email": "bob@example.com"},
    )


@given("a data directory with records")
def step_data_records(context):
    root = _ensure_cli_root(context)
    data_dir = root / "data" / "users"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        data_dir / "user-1.json",
        {"id": "user-1", "name": "Alice", "email": "alice@example.com"},
    )


@when("I run virtuus query with valid parameters")
def step_run_query_valid(context):
    _run_virtuus(context, "query --dir ./data --table users --pk user-1")


@when("I run virtuus query --dir ./data --table nonexistent")
def step_run_query_missing_table(context):
    _run_virtuus(context, "query --dir ./data --table nonexistent")


@when(
    "I run virtuus query --dir ./data --table users --index by_email --where email=alice@example.com"
)
def step_run_query_index_match(context):
    _run_virtuus(
        context,
        "query --dir ./data --table users --index by_email --where email=alice@example.com",
    )


@when(
    "I run virtuus query --dir ./data --table users --index by_email --where email=nobody@example.com"
)
def step_run_query_index_none(context):
    _run_virtuus(
        context,
        "query --dir ./data --table users --index by_email --where email=nobody@example.com",
    )


@when("I run virtuus query --dir ./data --schema schema.yml --table users --pk user-1")
def step_run_query_schema(context):
    _run_virtuus(
        context,
        "query --dir ./data --schema schema.yml --table users --pk user-1",
    )


@then("the output should be valid JSON")
def step_output_valid_json(context):
    json.loads(context.cli_stdout)


@then("the output should contain the matching user record")
def step_output_contains_user(context):
    payload = json.loads(context.cli_stdout)
    assert any(
        item.get("email") == "alice@example.com"
        for item in payload
        if isinstance(item, dict)
    )


@then('the output should be the user record for "user-1"')
def step_output_user(context):
    payload = json.loads(context.cli_stdout)
    assert payload.get("id") == "user-1"


@then("the output should be an empty JSON array")
def step_output_empty(context):
    payload = json.loads(context.cli_stdout)
    assert payload == []


@then("the command should exit with a non-zero status")
def step_nonzero_exit(context):
    assert context.cli_status != 0


@then("the error message should indicate the table was not found")
def step_error_not_found(context):
    assert "not found" in (context.cli_stderr or "").lower()


@then("results should be printed to stdout as JSON")
def step_stdout_json(context):
    json.loads(context.cli_stdout)


@then("the process should exit with status 0")
def step_exit_zero(context):
    assert context.cli_status == 0
