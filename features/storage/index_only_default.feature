Feature: Index-only storage default
  As a developer managing large datasets
  I want file-backed tables to default to index-only storage
  So that memory scales with indexes instead of full records

  Scenario: File-backed tables default to index-only storage
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
    When I load the schema
    And I call describe on the database
    Then the "users" table should report storage mode "index_only"

  Scenario: Storage mode can be set to memory
    Given a YAML schema file defining:
      """
      tables:
        users:
          primary_key: id
          directory: users
          storage: memory
      """
    And a data directory for "users" with records:
      | id      | name  |
      | user-1  | Alice |
    When I load the schema
    And I call describe on the database
    Then the "users" table should report storage mode "memory"
