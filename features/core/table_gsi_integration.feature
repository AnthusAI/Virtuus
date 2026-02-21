Feature: Table GSI integration
  As a developer using indexed tables
  I want GSIs to be automatically maintained as I put and delete records
  So that indexed queries always return accurate results

  Scenario: Adding a GSI to a table
    Given a table "users" with primary key "id"
    When I add a GSI "by_email" with partition key "email"
    Then the table should have a GSI named "by_email"

  Scenario: Put auto-indexes the record in all GSIs
    Given a table "users" with a GSI "by_email" on "email" and a GSI "by_status" on "status"
    When I put a record {"id": "user-1", "email": "alice@example.com", "status": "active"}
    Then querying GSI "by_email" for "alice@example.com" should return "user-1"
    And querying GSI "by_status" for "active" should return "user-1"

  Scenario: Delete removes the record from all GSIs
    Given a table "users" with a GSI "by_email" on "email"
    And a record {"id": "user-1", "email": "alice@example.com"}
    When I delete record "user-1"
    Then querying GSI "by_email" for "alice@example.com" should return empty

  Scenario: Query via GSI returns full records
    Given a table "users" with a GSI "by_status" on "status"
    And records:
      | id     | name  | status   |
      | user-1 | Alice | active   |
      | user-2 | Bob   | active   |
      | user-3 | Carol | inactive |
    When I query the table via GSI "by_status" for "active"
    Then the result should contain 2 full records with all fields
