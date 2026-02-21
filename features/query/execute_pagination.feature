Feature: Execute paginated queries
  As a developer handling large result sets
  I want to paginate through results using cursors
  So that I can retrieve data in manageable pages

  Scenario: First page returns results with next_token
    Given a database with a "users" table containing 50 records
    When I execute {"users": {"limit": 10}}
    Then the result should contain 10 records
    And the result should include a "next_token" value

  Scenario: Second page uses next_token from first page
    Given a database with a "users" table containing 50 records
    And I execute {"users": {"limit": 10}} and receive a next_token
    When I execute {"users": {"limit": 10, "next_token": "<previous_token>"}}
    Then the result should contain the next 10 records
    And no record should overlap with the first page

  Scenario: Last page returns no next_token
    Given a database with a "users" table containing 25 records
    And I page through with limit 10
    When I reach the last page
    Then the result should contain 5 records
    And the result should not include a "next_token"

  Scenario: Pagination works with indexed queries
    Given a database with a "posts" table and GSI "by_user" on "user_id"
    And 30 posts for user "user-1"
    When I execute {"posts": {"index": "by_user", "where": {"user_id": "user-1"}, "limit": 10}}
    Then the result should contain 10 posts
    And the result should include a "next_token"

  Scenario: Full traversal returns all records
    Given a database with a "users" table containing 25 records
    When I page through all records with limit 10
    Then the total collected records should be 25
    And there should be no duplicates

  Scenario: Pagination with descending sort order
    Given a database with a "posts" table and GSI "by_user" on "user_id" sorted by "created_at"
    And 20 posts for user "user-1" with sequential created_at values
    When I page through with {"posts": {"index": "by_user", "where": {"user_id": "user-1"}, "limit": 5, "sort_direction": "desc"}}
    Then each page should contain records in descending created_at order
    And the full traversal should return all 20 posts in reverse chronological order
