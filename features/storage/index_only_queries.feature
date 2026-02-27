Feature: Index-only query behavior
  As a developer
  I want PK and GSI queries to work in index-only mode
  So that indexed access remains fast without holding full records

  Scenario: PK lookup works in index-only mode
    Given a YAML schema file defining:
      """
      tables:
        users:
          primary_key: id
          directory: users
      """
    And a data directory for "users" with records:
      | id      | name  |
      | user-1  | Alice |
      | user-2  | Bob   |
    When I load the schema
    And I execute {"users": {"pk": "user-1"}}
    Then the result should return the user record for "user-1"

  Scenario: GSI query works in index-only mode
    Given a YAML schema file defining:
      """
      tables:
        users:
          primary_key: id
          directory: users
          gsis:
            by_status:
              partition_key: status
      """
    And a data directory for "users" with records:
      | id      | name  | status |
      | user-1  | Alice | active |
      | user-2  | Bob   | active |
      | user-3  | Carol | inactive |
    When I load the schema
    And I execute {"users": {"index": "by_status", "where": {"status": "active"}}}
    Then the result should include "user-1" and "user-2"
