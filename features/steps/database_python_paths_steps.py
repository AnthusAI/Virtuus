import json
import os
import tempfile
from pathlib import Path

from behave import given, when, then

from virtuus._python import Database


def _write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


@given("a temporary python data root with users fixture and schema yaml")
def step_setup_paths(context):
    context.tmp = tempfile.TemporaryDirectory()
    root = Path(context.tmp.name)
    users_dir = root / "users"
    _write_json(users_dir / "u1.json", {"id": "u1", "status": "active"})
    schema = {
        "tables": {
            "users": {
                "primary_key": "id",
                "directory": "users",
                "gsis": {"by_status": {"partition_key": "status"}},
            }
        }
    }
    context.schema_path = root / "schema.yaml"
    context.schema_path.write_text(json.dumps(schema), encoding="utf-8")
    context.data_root = str(root)


@when("I load the python database from that yaml")
def step_load_yaml(context):
    context.db = Database.from_schema(str(context.schema_path), data_root=context.data_root)


@then("describe() should report users table not stale with 1 record")
def step_assert_describe(context):
    desc = context.db.describe()["users"]
    assert desc["record_count"] == 1
    assert desc.get("stale") is False


@then("validate should return no violations")
def step_assert_validate(context):
    assert context.db.validate() == []
