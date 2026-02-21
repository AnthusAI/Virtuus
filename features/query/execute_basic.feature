Feature: Execute basic queries
  As a developer querying a database
  I want to use execute() with basic directives
  So that I can retrieve and filter records through a unified interface

  Scenario: Query by primary key
    Given a database with a "users" table containing {"id": "user-1", "name": "Alice"}
    When I execute {"users": {"pk": "user-1"}}
    Then the result should return the user record for "user-1"

  Scenario: Query with where clause
    Given a database with a "users" table containing:
      | id     | name  | status   |
      | user-1 | Alice | active   |
      | user-2 | Bob   | inactive |
      | user-3 | Carol | active   |
    When I execute {"users": {"where": {"status": "active"}}}
    Then the result should contain 2 records
    And the result should include "user-1" and "user-3"

  Scenario: Query with field projection
    Given a database with a "users" table containing {"id": "user-1", "name": "Alice", "email": "alice@example.com", "status": "active"}
    When I execute {"users": {"pk": "user-1", "fields": ["id", "name"]}}
    Then the result should contain only the "id" and "name" fields

  Scenario: Query with limit
    Given a database with a "users" table containing 100 records
    When I execute {"users": {"limit": 10}}
    Then the result should contain exactly 10 records

  Scenario: Query non-existent table raises an error
    Given a database with a "users" table
    When I execute {"nonexistent": {"pk": "x"}}
    Then an error should be raised indicating table "nonexistent" does not exist

  Scenario: Query non-existent GSI raises an error
    Given a database with a "users" table and no GSI named "by_foo"
    When I execute {"users": {"index": "by_foo", "where": {"foo": "bar"}}}
    Then an error should be raised indicating GSI "by_foo" does not exist
