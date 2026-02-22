@python-only
Feature: Python backend coverage
  As a developer
  I want direct Python backend scenarios
  So that core database paths are covered without relying on Rust

  Scenario: Execute queries and includes on Python backend
    Given a fresh python database
    And a python table "users" with primary key "id"
    And a python gsi table "posts" with primary key "id" and gsi "by_user" on "user_id"
    And a has_many association "posts" from "users" via GSI "by_user" on table "posts"
    And records exist in table "users":
      | id   | name  |
      | u1   | Alice |
    And records exist in table "posts":
      | id  | user_id | title   |
      | p1  | u1      | Hello   |
      | p2  | u1      | World   |
    When I execute the python database query:
      """
      {"users": {"pk": "u1", "fields": ["id"], "include": {"posts": {}}}}
      """
    Then the result should include posts for user "u1"
    And executing the python database query:
      """
      {"posts": {"index": "by_user", "where": {"user_id": "u1"}}}
      """
    Then the result count should be 2

  Scenario: Validate detects missing parent on Python backend
    Given a fresh python database
    And a python table "users" with primary key "id"
    And a python table "posts" with primary key "id"
    And a belongs_to association "author" on "posts" targeting "users" via "user_id"
    And records exist in table "posts":
      | id  | user_id |
      | p1  | missing |
    When I validate the python database
    Then the validation should report a missing parent for "p1"

  Scenario: Load from YAML schema file on Python backend
    Given a temporary schema YAML file with tables "users" and "posts"
    When I load a python database from that schema file
    Then the database should contain tables "users" and "posts"
    And warming the python database should succeed

  Scenario: Query with pagination and where filter on Python backend
    Given a fresh python database
    And a python table "users" with primary key "id"
    And records exist in table "users":
      | id  | status |
      | a1  | active |
      | a2  | active |
      | i1  | inactive |
    When I execute the python database query:
      """
      {"users": {"where": {"status": "active"}, "limit": 1}}
      """
    Then the result should contain 1 item and a next_token
    When I execute the python database query:
      """
      {"users": {"where": {"status": "active"}, "limit": 1, "next_token": "<previous_token>"}}
      """
    Then the result should contain 1 item and no next_token

  Scenario: Querying a missing GSI raises an error
    Given a fresh python database
    And a python table "users" with primary key "id"
    And records exist in table "users":
      | id | name |
      | u1 | X    |
    When I execute the python database query expecting an error:
      """
      {"users": {"index": "does_not_exist", "where": {"x": "y"}}}
      """
