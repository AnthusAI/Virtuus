Feature: Concurrent reads
  As a developer deploying Virtuus under load
  I want concurrent readers to get consistent results
  So that the system is safe for multi-threaded use

  Scenario: 100 concurrent readers get consistent results
    Given a database with "users" table containing 1000 records
    And a GSI "by_status" on "status"
    When 100 threads simultaneously query index "by_status" for "active"
    Then all 100 threads should return the same result set
    And no errors should occur

  Scenario: Concurrent PK lookups return correct records
    Given a database with "users" table containing 1000 records
    When 50 threads simultaneously get different records by PK
    Then each thread should receive the correct record
    And no thread should receive another thread's record

  Scenario: Concurrent scans return complete results
    Given a database with "users" table containing 500 records
    When 20 threads simultaneously scan the table
    Then all 20 scans should return 500 records each
