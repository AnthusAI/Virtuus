Feature: Table export
  As a developer creating snapshots
  I want to export all in-memory records to JSON files
  So that I can create backups, migrate data, or seed test fixtures

  Scenario: Export writes all records to a directory
    Given a table "users" with 10 records in memory
    When I export the table to a new directory
    Then the directory should contain 10 JSON files
    And each file should contain a valid JSON record

  Scenario: Export uses atomic writes
    Given a table "users" with records in memory
    When I export the table to a directory
    Then each file should be written atomically via temp+rename

  Scenario: Export empty table creates empty directory
    Given an empty table "users"
    When I export the table to a new directory
    Then the directory should exist and contain 0 files

  Scenario: Exported data can be loaded back
    Given a table "users" with 5 records
    When I export the table to a directory
    And I create a new table and load from that directory
    Then the new table should contain the same 5 records
