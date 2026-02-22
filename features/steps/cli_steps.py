from __future__ import annotations

import http.client
import json
import shlex
import socket
import subprocess
import tempfile
import time
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


def _ensure_server_fixture(context) -> Path:
    root = _ensure_cli_root(context)
    data_dir = root / "data" / "users"
    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        _write_json(data_dir / "user-1.json", {"id": "user-1", "name": "Alice"})
    schema_path = root / "schema.yml"
    if not schema_path.exists():
        schema = "\n".join(
            [
                "tables:",
                "  users:",
                "    primary_key: id",
                "    directory: users",
                "",
            ]
        )
        schema_path.write_text(schema, encoding="utf-8")
    return root


def _wait_for_server(port: int) -> None:
    for _ in range(30):
        try:
            conn = http.client.HTTPConnection("localhost", port, timeout=1)
            conn.request("GET", "/health")
            resp = conn.getresponse()
            resp.read()
            conn.close()
            if resp.status == 200:
                return
        except Exception:
            time.sleep(0.1)


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _start_server(context, port: int) -> None:
    if getattr(context, "server_process", None):
        return
    root = _ensure_server_fixture(context)
    binary = _repo_root() / "rust" / "target" / "debug" / "virtuus"
    if not binary.exists():
        subprocess.run(
            ["cargo", "build"],
            cwd=_repo_root() / "rust",
            check=True,
        )
    context.server_process = subprocess.Popen(
        [str(binary), "serve", "--dir", "./data", "--schema", "schema.yml", "--port", str(port)],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    context.server_port = port
    _wait_for_server(port)


def _http_request(context, method: str, path: str, body: str | None = None) -> None:
    port = getattr(context, "server_port", 8080)
    conn = http.client.HTTPConnection("localhost", port, timeout=2)
    headers = {"Content-Type": "application/json"}
    payload = body or ""
    conn.request(method, path, body=payload, headers=headers)
    resp = conn.getresponse()
    context.http_status = resp.status
    context.http_headers = dict(resp.getheaders())
    context.http_body = resp.read().decode("utf-8")
    conn.close()


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


@given("a data directory with a schema")
def step_data_schema_server(context):
    _ensure_server_fixture(context)


@given("a running Virtuus server")
def step_running_server(context):
    _start_server(context, _pick_free_port())


@given("a running Virtuus server with loaded data")
def step_running_server_loaded(context):
    _start_server(context, _pick_free_port())


@when("I start virtuus serve --dir ./data --schema schema.yml --port 8080")
def step_start_server(context):
    _start_server(context, 8080)


@when('I POST a JSON query {{"users": {{"pk": "user-1"}}}} to http://localhost:8080/query')
def step_post_query(context):
    _http_request(context, "POST", "/query", '{"users": {"pk": "user-1"}}')


@when("I POST a query")
def step_post_query_default(context):
    _http_request(context, "POST", "/query", '{"users": {"pk": "user-1"}}')


@when("I POST an invalid JSON body")
def step_post_invalid_json(context):
    _http_request(context, "POST", "/query", "{invalid")


@when("I start the server and send 10 queries")
def step_start_server_and_queries(context):
    _start_server(context, _pick_free_port())
    for _ in range(10):
        _http_request(context, "POST", "/query", '{"users": {"pk": "user-1"}}')


@when("a file is added to a table's directory")
def step_add_file(context):
    root = _ensure_server_fixture(context)
    data_dir = root / "data" / "users"
    _write_json(data_dir / "user-2.json", {"id": "user-2", "name": "Bob"})


@when("I POST a query for that table")
def step_post_query_table(context):
    _http_request(context, "POST", "/query", '{"users": {}}')


@when("I GET /health")
def step_get_health(context):
    _http_request(context, "GET", "/health")


@when("I POST to /describe")
def step_post_describe(context):
    _http_request(context, "POST", "/describe", "{}")


@when("I POST to /validate")
def step_post_validate(context):
    _http_request(context, "POST", "/validate", "{}")


@when("I POST to /warm")
def step_post_warm(context):
    _http_request(context, "POST", "/warm", "{}")


@then("the response should be valid JSON containing the user record")
def step_response_contains_user(context):
    payload = json.loads(context.http_body)
    assert payload.get("id") == "user-1"


@then("the response Content-Type should be application/json")
def step_response_content_type(context):
    content_type = None
    for key, value in (context.http_headers or {}).items():
        if key.lower() == "content-type":
            content_type = value
            break
    assert content_type is not None
    assert "application/json" in content_type


@then("the response should have a 400 status code")
def step_response_400(context):
    assert context.http_status == 400


@then("the response should include an error message")
def step_response_error(context):
    assert "error" in (context.http_body or "").lower()


@then("data should be loaded from disk only once at startup")
def step_load_once(context):
    _http_request(context, "GET", "/health")
    payload = json.loads(context.http_body)
    assert payload.get("load_count") == 1


@then("the response should include the new record via JIT refresh")
def step_response_jit(context):
    payload = json.loads(context.http_body)
    items = payload.get("items", [])
    assert any(item.get("id") == "user-2" for item in items)


@then("the response should have a 200 status code")
def step_response_200(context):
    assert context.http_status == 200


@then("the response should be valid JSON with server status")
def step_response_status(context):
    payload = json.loads(context.http_body)
    assert payload.get("status") == "ok"


@then("the response should be valid JSON with table metadata")
def step_response_metadata(context):
    payload = json.loads(context.http_body)
    assert "users" in payload


@then("the response should be valid JSON with validation results")
def step_response_validation(context):
    payload = json.loads(context.http_body)
    assert "violations" in payload


@then("all tables should be refreshed")
def step_tables_refreshed(context):
    payload = json.loads(context.http_body)
    tables = payload.get("tables", [])
    assert "users" in tables
