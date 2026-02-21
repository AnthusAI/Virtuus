Feature: Execute nested queries with includes
  As a developer querying related data
  I want to use execute() with include directives
  So that I can retrieve records with their associations in one call

  Scenario: Include has_many association
    Given a database with "users" and "posts" tables
    And user "user-1" has 3 posts
    When I execute {"users": {"pk": "user-1", "include": {"posts": {}}}}
    Then the result should include the user with a nested "posts" array of 3 records

  Scenario: Include belongs_to association
    Given a database with "posts" and "users" tables
    And post "post-1" belongs to user "user-1"
    When I execute {"posts": {"pk": "post-1", "include": {"author": {}}}}
    Then the result should include the post with a nested "author" object

  Scenario: Include has_many_through association
    Given a database with "jobs", "job_assignments", and "workers" tables
    And job "job-1" has 2 workers through job_assignments
    When I execute {"jobs": {"pk": "job-1", "include": {"workers": {}}}}
    Then the result should include the job with a nested "workers" array of 2 records

  Scenario: Multi-level nested includes
    Given a database with "users", "posts", and "comments" tables
    And user "user-1" has posts, and each post has comments
    When I execute {"users": {"pk": "user-1", "include": {"posts": {"include": {"comments": {}}}}}}
    Then the result should include user → posts → comments nested 3 levels deep

  Scenario: Include with field projection on nested records
    Given a database with "users" and "posts" tables
    And user "user-1" has posts
    When I execute {"users": {"pk": "user-1", "include": {"posts": {"fields": ["id", "title"]}}}}
    Then each nested post should only contain "id" and "title" fields

  Scenario: Include with empty association returns empty array
    Given a database with "users" and "posts" tables
    And user "user-1" has no posts
    When I execute {"users": {"pk": "user-1", "include": {"posts": {}}}}
    Then the nested "posts" array should be empty
