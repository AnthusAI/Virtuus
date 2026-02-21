Feature: Warm reindex
  As a developer optimizing query latency
  I want to proactively refresh tables before queries need them
  So that queries never pay the cost of JIT refresh

  Scenario: Warm refreshes a single table proactively
    Given a table "users" loaded from a directory
    And a new JSON file is added to the directory
    When I call warm on the table
    Then the table should contain the new record
    And subsequent queries should not trigger a refresh

  Scenario: Warm refreshes all tables in a database
    Given a database with tables "users" and "posts" loaded from directories
    And new files are added to both directories
    When I call warm on the database
    Then both tables should contain their new records

  Scenario: Warm is a no-op when all tables are fresh
    Given a database with tables loaded from directories
    When I call warm with no file changes
    Then no files should be re-read from disk
