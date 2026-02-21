Feature: Database validate (referential integrity)
  As a developer checking data quality
  I want to validate that all foreign keys resolve to existing records
  So that I can detect orphaned or broken references in imported data

  Scenario: Clean data passes validation
    Given a database with "users" and "posts" tables
    And every post's user_id references an existing user
    When I call validate on the database
    Then the result should be an empty list of violations

  Scenario: Missing parent detected
    Given a database with "users" and "posts" tables
    And post "post-1" has user_id "user-999" which does not exist in users
    When I call validate on the database
    Then the result should include a violation for post "post-1" referencing missing user "user-999"

  Scenario: Multiple violations reported
    Given a database with "users" and "posts" tables
    And 3 posts reference non-existent users
    When I call validate on the database
    Then the result should contain 3 violations

  Scenario: Violation includes full context
    Given a database with a referential integrity violation
    When I call validate on the database
    Then each violation should include table, record_pk, association, foreign_key, and missing_target

  Scenario: Validate skips has_many associations
    Given a database with only has_many associations defined
    When I call validate on the database
    Then the result should be an empty list
