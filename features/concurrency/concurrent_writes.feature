@python-only
Feature: Concurrent writes
  As a developer handling parallel write operations
  I want concurrent puts to all succeed without data loss
  So that no writes are silently dropped

  Scenario: Concurrent puts to different keys all persist
    Given a database with an empty "users" table
    When 100 threads simultaneously put records with unique PKs
    Then the table should contain 100 records
    And all 100 JSON files should exist on disk

  Scenario: No corrupted files from concurrent writes
    Given a database with a "users" table
    When 50 threads simultaneously put records
    Then every JSON file on disk should contain valid JSON

  Scenario: Concurrent puts maintain GSI consistency
    Given a database with a "users" table and GSI "by_status" on "status"
    When 100 threads simultaneously put records with various statuses
    Then the sum of all GSI partition sizes should equal the total record count

  Scenario: Concurrent puts to the same PK use last-write-wins
    Given a database with a "users" table
    When 10 threads simultaneously put records with the same PK "user-1" but different data
    Then the table should contain exactly 1 record with PK "user-1"
    And the record should match one of the 10 written versions
    And no error should have occurred
