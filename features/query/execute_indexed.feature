Feature: Execute indexed queries
  As a developer querying by secondary indexes
  I want to use execute() with index and sort directives
  So that I can perform efficient indexed lookups

  Scenario: Query by GSI partition key
    Given a database with a "posts" table and GSI "by_user" on "user_id"
    And posts:
      | id     | user_id | title       |
      | post-1 | user-1  | First Post  |
      | post-2 | user-1  | Second Post |
      | post-3 | user-2  | Other Post  |
    When I execute {"posts": {"index": "by_user", "where": {"user_id": "user-1"}}}
    Then the result should contain 2 posts for user-1

  Scenario: Query by GSI with sort condition
    Given a database with a "posts" table and GSI "by_user" on "user_id" sorted by "created_at"
    And posts for user-1 with created_at values "2025-01-01", "2025-06-01", "2025-12-01"
    When I execute {"posts": {"index": "by_user", "where": {"user_id": "user-1"}, "sort": {"gte": "2025-06-01"}}}
    Then the result should contain 2 posts with created_at >= "2025-06-01"

  Scenario: Query with descending sort direction
    Given a database with a "posts" table and GSI "by_user" on "user_id" sorted by "created_at"
    And 3 posts for user-1 with ascending created_at values
    When I execute {"posts": {"index": "by_user", "where": {"user_id": "user-1"}, "sort_direction": "desc"}}
    Then the result should be in descending created_at order
