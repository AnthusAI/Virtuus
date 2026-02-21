Feature: Incremental refresh
  As a developer with large tables
  I want refresh to only reload changed files
  So that updates are fast even with thousands of records

  Scenario: Refresh loads only new files
    Given a table "users" loaded from 5 JSON files with a GSI on "status"
    When 2 new JSON files are added to the directory
    And the table is refreshed
    Then the table should contain 7 records
    And all GSIs should include the 2 new records

  Scenario: Refresh removes deleted records
    Given a table "users" loaded from 5 JSON files with a GSI on "status"
    When 1 JSON file is deleted from the directory
    And the table is refreshed
    Then the table should contain 4 records
    And the deleted record should be absent from all GSIs

  Scenario: Refresh reloads modified files
    Given a table "users" loaded from a directory
    And a JSON file is modified to change a GSI-indexed field
    When the table is refreshed
    Then the record should reflect the updated field value
    And GSI queries should return the record under the new index value

  Scenario: Refresh is incremental, not full rebuild
    Given a table "users" loaded from 100 JSON files
    When 1 file is modified and the table is refreshed
    Then only 1 file should be re-read from disk
