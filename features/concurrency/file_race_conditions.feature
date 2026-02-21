Feature: File race conditions
  As a developer handling filesystem edge cases
  I want Virtuus to handle unexpected file states gracefully
  So that the system remains stable under adversarial conditions

  Scenario: Graceful handling of truncated JSON file
    Given a table "users" loaded from a directory
    When a JSON file in the directory is replaced with truncated content
    And the table is refreshed
    Then the refresh should report the corrupted file as an error
    And the table should still contain all other valid records
    And queries should continue to work

  Scenario: File disappears between stat and read
    Given a table "users" loaded from a directory
    When a file is detected during the directory scan but deleted before it can be read
    And the table is refreshed
    Then the refresh should handle the missing file gracefully
    And no unhandled error should occur

  Scenario: File appears during directory scan
    Given a table "users" loaded from a directory
    When a new file is created while a refresh scan is in progress
    And the table is refreshed
    Then the table should be in a consistent state
    And the new file should be picked up in this or the next refresh

  Scenario: Empty JSON file
    Given a table "users" loaded from a directory
    When an empty file (0 bytes) exists in the directory
    And the table is refreshed
    Then the empty file should be reported as an error
    And other records should remain accessible
