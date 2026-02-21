Feature: Table describe
  As a developer debugging in a container
  I want to inspect table metadata without querying records
  So that I can understand the table's configuration and state

  Scenario: Describe returns table metadata
    Given a table "users" with primary key "id"
    And a GSI "by_email" with partition key "email"
    And 25 records loaded
    When I call describe on the table
    Then the result should include:
      | field        | value   |
      | name         | users   |
      | primary_key  | id      |
      | record_count | 25      |
    And the result should list GSI "by_email"

  Scenario: Describe includes association names
    Given a table "users" with primary key "id"
    And a has_many association "posts" to table "posts"
    When I call describe on the table
    Then the result should list association "posts"

  Scenario: Describe on empty table
    Given an empty table "users" with primary key "id"
    When I call describe on the table
    Then the result should include record_count of 0
