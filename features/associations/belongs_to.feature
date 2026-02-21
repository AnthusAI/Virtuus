Feature: Belongs-to associations
  As a developer querying parent records
  I want to define belongs_to associations between tables
  So that I can look up the parent record for a child

  Scenario: Define a belongs_to association
    Given a table "posts" with primary key "id"
    And a table "users" with primary key "id"
    When I define a belongs_to association "author" on "posts" targeting table "users" via foreign key "user_id"
    Then the "posts" table should have an association named "author"

  Scenario: Resolve belongs_to returns the parent record
    Given a table "posts" with a belongs_to association "author" targeting "users" via "user_id"
    And user {"id": "user-1", "name": "Alice"}
    And post {"id": "post-1", "user_id": "user-1", "title": "Hello"}
    When I resolve the "author" association for post "post-1"
    Then the result should be the user record for "user-1"

  Scenario: Resolve belongs_to with missing parent returns null
    Given a table "posts" with a belongs_to association "author" targeting "users" via "user_id"
    And post {"id": "post-1", "user_id": "user-999", "title": "Orphan"}
    And no user with id "user-999"
    When I resolve the "author" association for post "post-1"
    Then the result should be null

  Scenario: Resolve belongs_to with null foreign key returns null
    Given a table "posts" with a belongs_to association "author" targeting "users" via "user_id"
    And post {"id": "post-1", "title": "No Author"} with no user_id field
    When I resolve the "author" association for post "post-1"
    Then the result should be null
