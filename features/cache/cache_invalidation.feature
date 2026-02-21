Feature: Cache invalidation
  As a developer using file-backed tables
  I want the cache to detect when files on disk have changed
  So that queries always return up-to-date results

  Scenario: Table detects no changes when files are unchanged
    Given a table "users" loaded from a directory with 5 JSON files
    When I check if the table is stale
    Then it should report fresh

  Scenario: Table detects staleness when a file is modified
    Given a table "users" loaded from a directory with 5 JSON files
    When a JSON file in the directory is modified
    And I check if the table is stale
    Then it should report stale

  Scenario: Table detects staleness when a file is added
    Given a table "users" loaded from a directory with 5 JSON files
    When a new JSON file is added to the directory
    And I check if the table is stale
    Then it should report stale

  Scenario: Table detects staleness when a file is deleted
    Given a table "users" loaded from a directory with 5 JSON files
    When a JSON file is removed from the directory
    And I check if the table is stale
    Then it should report stale

  Scenario: JIT refresh on query when stale
    Given a table "users" loaded from a directory
    And a new JSON file is added to the directory
    When I query the table
    Then the new record should be included in results
    And the table should report fresh afterward

  Scenario: Query skips refresh when fresh
    Given a table "users" loaded from a directory
    When I query the table twice with no file changes between
    Then the second query should not trigger a refresh

  Scenario: check_interval prevents re-checking staleness within the interval
    Given a table "users" loaded from a directory with check_interval of 5 seconds
    When a JSON file is modified
    And I check if the table is stale within 5 seconds of the last check
    Then it should report fresh without scanning files

  Scenario: auto_refresh disabled requires explicit warm
    Given a table "users" loaded from a directory with auto_refresh disabled
    And a new JSON file is added to the directory
    When I query the table
    Then the new record should not be included in results
    When I call warm on the table
    Then the new record should be included in results
