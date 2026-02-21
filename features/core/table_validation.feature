Feature: Table validation on put
  As a developer catching data errors early
  I want opt-in validation that checks records on put
  So that missing keys don't silently break indexes

  Scenario: Validation off by default (silent mode)
    Given a table "users" with primary key "id" and validation "silent"
    When I put a record missing the "id" field
    Then no error or warning should occur

  Scenario: Validation in warn mode logs a warning for missing PK
    Given a table "users" with primary key "id" and validation "warn"
    When I put a record missing the "id" field
    Then a warning should be logged about the missing primary key

  Scenario: Validation in error mode raises for missing PK
    Given a table "users" with primary key "id" and validation "error"
    When I put a record missing the "id" field
    Then an error should be raised about the missing primary key

  Scenario: Validation warns about missing GSI-indexed field
    Given a table "users" with primary key "id" and validation "warn"
    And a GSI "by_email" with partition key "email"
    When I put a record {"id": "user-1"} missing the "email" field
    Then a warning should be logged about the missing GSI field "email"

  Scenario: Valid record passes all validation
    Given a table "users" with primary key "id" and validation "error"
    And a GSI "by_email" with partition key "email"
    When I put a record {"id": "user-1", "email": "alice@example.com"}
    Then no error should occur
