Feature: Cache check (dry-run refresh)
  As a developer monitoring data freshness
  I want to see what would change without actually refreshing
  So that I can make informed decisions about when to refresh

  Scenario: Check reports no changes when fresh
    Given a table "users" loaded from a directory with 5 JSON files
    When I call check on the table
    Then the result should report 0 added, 0 modified, 0 deleted

  Scenario: Check reports added files
    Given a table "users" loaded from a directory with 5 JSON files
    And 2 new JSON files are added to the directory
    When I call check on the table
    Then the result should report 2 added, 0 modified, 0 deleted

  Scenario: Check reports modified files
    Given a table "users" loaded from a directory with 5 JSON files
    And 1 JSON file is modified on disk
    When I call check on the table
    Then the result should report 0 added, 1 modified, 0 deleted

  Scenario: Check reports deleted files
    Given a table "users" loaded from a directory with 5 JSON files
    And 1 JSON file is removed from the directory
    When I call check on the table
    Then the result should report 0 added, 0 modified, 1 deleted

  Scenario: Check does not actually refresh the table
    Given a table "users" loaded from a directory with 5 JSON files
    And 2 new JSON files are added to the directory
    When I call check on the table
    Then the table should still contain 5 records

  Scenario: on_refresh hook fires after actual refresh with change summary
    Given a table "users" with an on_refresh hook registered
    And a new JSON file is added to the directory
    When the table is refreshed
    Then the on_refresh hook should receive a change summary
    And the summary should include counts of added, modified, and deleted files
