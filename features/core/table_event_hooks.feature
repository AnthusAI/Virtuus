Feature: Table event hooks
  As a developer extending table behavior
  I want to register callbacks that fire on put and delete
  So that I can add logging, metrics, or reactive patterns

  Scenario: on_put hook fires after a record is put
    Given a table "users" with an on_put hook registered
    When I put a record {"id": "user-1", "name": "Alice"}
    Then the on_put hook should have been called with the record

  Scenario: on_delete hook fires after a record is deleted
    Given a table "users" with an on_delete hook registered
    And a record {"id": "user-1", "name": "Alice"}
    When I delete record "user-1"
    Then the on_delete hook should have been called with the record

  Scenario: Multiple hooks fire in order
    Given a table "users" with 3 on_put hooks registered
    When I put a record
    Then all 3 hooks should fire in registration order

  Scenario: Hook error does not block the operation
    Given a table "users" with an on_put hook that raises an error
    When I put a record {"id": "user-1", "name": "Alice"}
    Then the record should be stored successfully
    And the hook error should be logged

  Scenario: Hook receives the full record
    Given a table "users" with an on_put hook registered
    When I put a record {"id": "user-1", "name": "Alice", "email": "alice@example.com"}
    Then the hook should receive all fields of the record
