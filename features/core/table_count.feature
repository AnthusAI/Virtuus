Feature: Table count
  As a developer checking data volumes
  I want to count records without materializing full result lists
  So that I can efficiently answer "how many?" questions

  Scenario: Count all records in a table
    Given a table "users" with 50 records
    When I call count on the table
    Then the result should be 50

  Scenario: Count records in a GSI partition
    Given a table "users" with a GSI "by_status" on "status"
    And 30 records with status "active" and 20 with status "inactive"
    When I call count on index "by_status" for value "active"
    Then the result should be 30

  Scenario: Count on empty table returns zero
    Given an empty table "users"
    When I call count on the table
    Then the result should be 0

  Scenario: Count on non-existent GSI partition returns zero
    Given a table "users" with a GSI "by_status" on "status"
    And 10 records with status "active"
    When I call count on index "by_status" for value "suspended"
    Then the result should be 0
