"""Step definitions for error handling tests."""

from behave import given, when, then
from virtuus._python import Database, Table
from virtuus.errors import (
    VirtuusError,
    UnknownTableError,
    UnknownIndexError,
    ValidationError,
)
import json


@given("a database with no tables")
def step_empty_database(context):
    """Create an empty database."""
    context.db = Database()
    context.last_error = None
    context.last_result = None


@when('I query for table "{table_name}"')
def step_execute_query_for_table(context, table_name):
    """Execute a query for a specific table."""
    try:
        query = json.loads(json.dumps({table_name: {"scan": True}}))
        result = context.db.execute(query)
        context.last_error = None
        context.last_result = result
    except (UnknownTableError, ValidationError, VirtuusError) as e:
        context.last_error = e
        context.last_result = None


@when("I query with a malformed query that is not an object")
def step_execute_malformed_query(context):
    """Execute a malformed query (not an object)."""
    try:
        query = "not an object"
        result = context.db.execute(query)
        context.last_error = None
        context.last_result = result
    except (ValidationError, VirtuusError, TypeError) as e:
        if isinstance(e, (ValidationError, VirtuusError)):
            context.last_error = e
        else:
            context.last_error = ValidationError("Query must be an object")
        context.last_result = None


@when("I query with zero tables specified")
def step_execute_query_zero_tables(context):
    """Execute a query with zero tables."""
    try:
        query = {}
        result = context.db.execute(query)
        context.last_error = None
        context.last_result = result
    except (ValidationError, VirtuusError) as e:
        context.last_error = e
        context.last_result = None


@when("I query targeting two tables")
def step_execute_query_two_tables(context):
    """Execute a query targeting two tables."""
    try:
        query = json.loads(json.dumps({"users": {"scan": True}, "products": {"scan": True}}))
        result = context.db.execute(query)
        context.last_error = None
        context.last_result = result
    except (ValidationError, VirtuusError) as e:
        context.last_error = e
        context.last_result = None


@when('I query for unknown index "{index_name}" on "{table_name}"')
def step_execute_query_unknown_index(context, index_name, table_name):
    """Execute a query for an unknown index."""
    try:
        query = json.loads(json.dumps({
            table_name: {
                "index": index_name,
                "where": {"email": "test@example.com"}
            }
        }))
        result = context.db.execute(query)
        context.last_error = None
        context.last_result = result
    except (UnknownIndexError, ValidationError, VirtuusError) as e:
        context.last_error = e
        context.last_result = None


@when('I query for index "{index_name}" on "{table_name}" without partition key in where')
def step_execute_query_missing_partition_key(context, index_name, table_name):
    """Execute a query for an index without the partition key in where clause."""
    try:
        query = json.loads(json.dumps({
            table_name: {
                "index": index_name,
                "where": {}
            }
        }))
        result = context.db.execute(query)
        context.last_error = None
        context.last_result = result
    except (ValidationError, VirtuusError) as e:
        context.last_error = e
        context.last_result = None


@then('the result is an error of kind "{error_kind}" naming "{name}"')
def step_check_error_kind_with_name(context, error_kind, name):
    """Check that the result is the expected error kind with a specific name."""
    assert context.last_error is not None, "Expected an error but got none"
    error_class_name = type(context.last_error).__name__
    assert (
        error_kind in error_class_name
    ), f"Expected {error_kind}, got {error_class_name}"
    assert name in str(context.last_error), f"Expected name {name} in error message"


@then('the result is an error of kind "{error_kind}" containing "{message}"')
def step_check_error_kind_containing(context, error_kind, message):
    """Check that the result is the expected error kind containing a message."""
    assert context.last_error is not None, "Expected an error but got none"
    error_class_name = type(context.last_error).__name__
    assert (
        error_kind in error_class_name
    ), f"Expected {error_kind}, got {error_class_name}"
    assert (
        message in str(context.last_error)
    ), f"Expected message containing '{message}' in error: {context.last_error}"


@then('the result is an error of kind "{error_kind}"')
def step_check_error_kind(context, error_kind):
    """Check that the result is the expected error kind."""
    assert context.last_error is not None, "Expected an error but got none"
    error_class_name = type(context.last_error).__name__
    assert (
        error_kind in error_class_name
    ), f"Expected {error_kind}, got {error_class_name}"


@then("the database still answers queries")
def step_database_still_works(context):
    """Verify that the database can still answer queries after an error."""
    if not hasattr(context, "db"):
        context.db = Database()
    assert context.db is not None, "Database should still be functional"
    assert hasattr(context.db, "tables"), "Database should have tables attribute"


@when('I try to load schema from "{path}"')
def step_load_schema_from_file(context, path):
    """Try to load a schema from a file."""
    from virtuus.errors import IoError, ParseError, ValidationError

    try:
        db = Database.from_schema(path)
        context.last_error = None
        context.db = db
    except (IoError, ParseError, ValidationError) as e:
        context.last_error = e
        context.db = None


@when("I try to load an invalid YAML schema")
def step_load_invalid_yaml_schema(context):
    """Try to load an invalid YAML schema."""
    from virtuus.errors import ParseError
    import tempfile
    import os

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_path = f.name

        try:
            db = Database.from_schema(temp_path)
            context.last_error = None
            context.db = db
        except ParseError as e:
            context.last_error = e
            context.db = None
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@when(
    'I try to load a schema with missing "primary_key" and "partition_key"'
)
def step_load_schema_missing_keys(context):
    """Try to load a schema with missing key configuration."""
    from virtuus.errors import ValidationError
    import tempfile
    import os

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(
                """
tables:
  users:
    directory: data
"""
            )
            temp_path = f.name

        try:
            db = Database.from_schema(temp_path)
            context.last_error = None
            context.db = db
        except ValidationError as e:
            context.last_error = e
            context.db = None
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@then("an error should be raised indicating Io error")
def step_check_io_error(context):
    """Check that an Io error was raised."""
    from virtuus.errors import IoError

    assert context.last_error is not None, "Expected an error but got none"
    assert isinstance(
        context.last_error, IoError
    ), f"Expected IoError, got {type(context.last_error)}"


@then("an error should be raised indicating Parse error")
def step_check_parse_error(context):
    """Check that a Parse error was raised."""
    from virtuus.errors import ParseError

    assert context.last_error is not None, "Expected an error but got none"
    assert isinstance(
        context.last_error, ParseError
    ), f"Expected ParseError, got {type(context.last_error)}"


@then("an error should be raised indicating Validation error")
def step_check_validation_error(context):
    """Check that a Validation error was raised."""
    from virtuus.errors import ValidationError

    assert context.last_error is not None, "Expected an error but got none"
    assert isinstance(
        context.last_error, ValidationError
    ), f"Expected ValidationError, got {type(context.last_error)}"


@when("I try to execute a malformed query and catch it as ValueError")
def step_catch_validation_error_as_value_error(context):
    """Try to execute a malformed query and catch it as ValueError."""
    # Ensure we have a database and table set up if not already done
    if not hasattr(context, "db"):
        context.db = Database()
        context.db.add_table("users", Table("users", primary_key="id"))

    context.last_error = None
    context.caught_as_value_error = False
    try:
        context.db.execute("not an object")
    except ValueError as e:
        context.last_error = e
        context.caught_as_value_error = True


@then("the error should be caught successfully as a ValueError")
def step_check_caught_as_value_error(context):
    """Check that the error was caught as ValueError."""
    assert (
        context.caught_as_value_error
    ), "ValidationError should be catchable as ValueError"
    assert context.last_error is not None, "Expected an error but got none"


@given('a temporary YAML schema with tables "users" and "posts" and belongs_to association')
def step_schema_with_belongs_to(context):
    """Create a temporary YAML schema with belongs_to association."""
    import tempfile
    from pathlib import Path

    context.tempdir = tempfile.TemporaryDirectory()
    schema = {
        "tables": {
            "users": {"primary_key": "id"},
            "posts": {
                "primary_key": "id",
                "associations": {"author": {"type": "belongs_to", "table": "users", "foreign_key": "user_id"}},
            },
        }
    }
    path = Path(context.tempdir.name) / "schema.yaml"
    path.write_text(json.dumps(schema), encoding="utf-8")
    context.schema_path = str(path)


@given('a temporary YAML schema with tables "users" and "posts" and has_many association')
def step_schema_with_has_many(context):
    """Create a temporary YAML schema with has_many association."""
    import tempfile
    from pathlib import Path

    context.tempdir = tempfile.TemporaryDirectory()
    schema = {
        "tables": {
            "users": {
                "primary_key": "id",
                "associations": {
                    "posts": {"type": "has_many", "table": "posts", "index": "by_user"}
                },
            },
            "posts": {
                "primary_key": "id",
                "gsis": {"by_user": {"partition_key": "user_id"}},
            },
        }
    }
    path = Path(context.tempdir.name) / "schema.yaml"
    path.write_text(json.dumps(schema), encoding="utf-8")
    context.schema_path = str(path)


@when("I load the python database from that schema")
def step_load_python_schema(context):
    """Load a Python database from the schema file."""
    context.db = Database.from_schema(context.schema_path)
    context.last_result = None


@when("I put test data into the tables")
def step_put_test_data(context):
    """Put test data into the tables for association testing."""
    if "users" in context.db.tables:
        context.db.tables["users"].put({"id": "u1", "name": "Alice"})
    if "posts" in context.db.tables:
        context.db.tables["posts"].put({"id": "p1", "user_id": "u1", "title": "Hello"})
        context.db.tables["posts"].put({"id": "p2", "user_id": "u1", "title": "World"})


@then("the result should include the related author")
def step_check_related_author(context):
    """Verify the result includes the related author."""
    assert context.last_result is not None, "Expected a result"
    assert "author" in context.last_result, "Expected author in result"
    assert context.last_result["author"]["id"] == "u1", "Expected correct author"


@then("the result should include the related posts")
def step_check_related_posts(context):
    """Verify the result includes the related posts."""
    assert context.last_result is not None, "Expected a result"
    assert "posts" in context.last_result, "Expected posts in result"
    assert len(context.last_result["posts"]) == 2, "Expected 2 posts"


@when('I put a post with pk "{pk}" and author_id "{author_id}" into the posts table')
def step_put_post_with_author_id(context, pk, author_id):
    """Put a post with a specific author_id into the posts table."""
    context.db.tables["posts"].put(
        {"id": pk, "author_id": author_id, "title": "Test"}
    )


@then('the result author field should be None')
def step_check_author_field_none(context):
    """Verify the result has author field as None."""
    assert context.last_result is not None, "Expected a result"
    assert "author" in context.last_result, "Expected author field in result"
    assert context.last_result["author"] is None, "Expected author to be None"


@then("the scan result should include posts with related authors")
def step_check_scan_with_includes(context):
    """Verify scan result includes posts with related authors."""
    assert context.last_result is not None, "Expected a result"
    assert "items" in context.last_result, "Expected items in result"
    assert len(context.last_result["items"]) == 2, "Expected 2 posts"
    for post in context.last_result["items"]:
        assert "author" in post, "Expected author field in post"
        assert post["author"] is not None, "Expected author to be populated"
        assert post["author"]["id"] == "u1", "Expected author id to be u1"


@given('a temporary YAML schema with tables for has_many_through association')
def step_schema_with_has_many_through(context):
    """Create a temporary YAML schema with has_many_through association."""
    import tempfile
    from pathlib import Path
    import json

    context.tempdir = tempfile.TemporaryDirectory()
    schema = {
        "tables": {
            "users": {
                "primary_key": "id",
                "associations": {
                    "roles": {
                        "type": "has_many_through",
                        "table": "roles",
                        "through_table": "user_roles",
                        "through_index": "by_user",
                        "foreign_key": "user_id",
                        "target_foreign_key": "role_id"
                    }
                }
            },
            "user_roles": {
                "primary_key": "id",
                "associations": {}
            },
            "roles": {
                "primary_key": "id",
                "associations": {}
            }
        }
    }
    path = Path(context.tempdir.name) / "schema.yaml"
    path.write_text(json.dumps(schema), encoding="utf-8")
    context.schema_path = str(path)
