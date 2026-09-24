Feature: Errors are values
  Query-time validation errors raised when executing a query against the database.

  Scenario: Query that is not an object raises Validation error
    Given a database with a "users" table
    When I try to execute "not an object"
    Then an error should be raised about malformed query
    And the database should still answer subsequent queries

  Scenario: Query with zero tables raises Validation error
    Given a database with a "users" table
    When I execute {}
    Then an error should be raised about targeting exactly one table
    And the database should still answer subsequent queries

  Scenario: Query targeting two tables raises Validation error
    Given a database with a "users" table and a "posts" table
    When I execute {"users": {"scan": true}, "posts": {"scan": true}}
    Then an error should be raised about targeting exactly one table
    And the database should still answer subsequent queries

  Scenario: Query non-existent table raises UnknownTable error
    Given a database with a "users" table
    When I execute {"products": {"pk": "x"}}
    Then an error should be raised indicating table "products" does not exist
    And the database should still answer subsequent queries

  Scenario: Query non-existent GSI raises UnknownIndex error
    Given a database with a "users" table and no GSI named "by_foo"
    When I execute {"users": {"index": "by_foo", "where": {"foo": "bar"}}}
    Then an error should be raised indicating GSI "by_foo" does not exist
    And the database should still answer subsequent queries

  Scenario: Query GSI without partition key raises Validation error
    Given a database with a "posts" table and GSI "by_user" on "user_id"
    When I execute {"posts": {"index": "by_user", "where": {}}}
    Then an error should be raised about the missing partition key in query
    And the database should still answer subsequent queries
