Feature: Has-many associations
  As a developer querying related records
  I want to define has_many associations between tables
  So that I can retrieve all child records for a parent

  Scenario: Define a has_many association
    Given a table "users" with primary key "id"
    And a table "posts" with primary key "id" and a GSI "by_user" on "user_id"
    When I define a has_many association "posts" on "users" targeting table "posts" via index "by_user"
    Then the "users" table should have an association named "posts"

  Scenario: Resolve has_many returns related records
    Given a table "users" with a has_many association "posts" via GSI "by_user" on table "posts"
    And user {"id": "user-1", "name": "Alice"}
    And posts:
      | id     | user_id | title       |
      | post-1 | user-1  | First Post  |
      | post-2 | user-1  | Second Post |
      | post-3 | user-2  | Other Post  |
    When I resolve the "posts" association for user "user-1"
    Then the result should contain 2 posts
    And the result should include "post-1" and "post-2"
    And the result should not include "post-3"

  Scenario: Resolve has_many with no related records returns empty
    Given a table "users" with a has_many association "posts" via GSI "by_user" on table "posts"
    And user {"id": "user-1", "name": "Alice"}
    And no posts with user_id "user-1"
    When I resolve the "posts" association for user "user-1"
    Then the result should be empty
