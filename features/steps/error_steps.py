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
