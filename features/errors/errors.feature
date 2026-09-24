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

  Scenario: Loading schema from non-existent file raises Io error
    When I try to load schema from "nonexistent_file.yml"
    Then an error should be raised indicating Io error

  Scenario: Loading invalid YAML schema raises Parse error
    When I try to load an invalid YAML schema
    Then an error should be raised indicating Parse error

  Scenario: Loading schema with missing table configuration field raises Validation error
    When I try to load a schema with missing "primary_key" and "partition_key"
    Then an error should be raised indicating Validation error

  Scenario: Query with include to missing belongs_to target raises UnknownTable error
    Given a posts table with belongs_to "author" pointing to missing "users" table
    When I execute {"posts": {"pk": "p1", "include": {"author": {}}}}
    Then an error should be raised indicating table "users" does not exist
    And the database should still answer subsequent queries

  Scenario: Query with include to missing has_many index target raises UnknownTable error
    Given a users table with has_many "posts" pointing to missing "posts" table
    When I execute {"users": {"pk": "u1", "include": {"posts": {}}}}
    Then an error should be raised indicating table "posts" does not exist
    And the database should still answer subsequent queries

  @python-only
  Scenario: ValidationError can be caught as ValueError for backward compatibility
    Given a database with a "users" table
    When I try to execute a malformed query and catch it as ValueError
    Then the error should be caught successfully as a ValueError

  Scenario: Load schema from YAML with belongs_to association and query with include
    Given a temporary YAML schema with tables "users" and "posts" and belongs_to association
    When I load the python database from that schema
    And I put test data into the tables
    And I execute the python database query:
      """
      {"posts": {"pk": "p1", "include": {"author": {}}}}
      """
    Then the result should include the related author

  Scenario: Load schema from YAML with has_many association and query with include
    Given a temporary YAML schema with tables "users" and "posts" and has_many association
    When I load the python database from that schema
    And I put test data into the tables
    And I execute the python database query:
      """
      {"users": {"pk": "u1", "include": {"posts": {}}}}
      """
    Then the result should include the related posts

  Scenario: Query with include returns None when related record missing
    Given a temporary YAML schema with tables "users" and "posts" and belongs_to association
    When I load the python database from that schema
    And I put a post with pk "p2" and author_id "u99" into the posts table
    And I execute the python database query:
      """
      {"posts": {"pk": "p2", "include": {"author": {}}}}
      """
    Then the result author field should be None

  Scenario: Query scan with include returns related records
    Given a temporary YAML schema with tables "users" and "posts" and belongs_to association
    When I load the python database from that schema
    And I put test data into the tables
    And I execute the python database query:
      """
      {"posts": {"scan": true, "include": {"author": {}}}}
      """
    Then the scan result should include posts with related authors

  Scenario: Query with nested empty include on single belongs_to association
    Given a temporary YAML schema with tables "users" and "posts" and belongs_to association
    When I load the python database from that schema
    And I put test data into the tables
    And I execute the python database query:
      """
      {"posts": {"pk": "p1", "include": {"author": {"include": {}}}}}
      """
    Then the result should include the related author
