Feature: Table CRUD operations
  As a developer managing records
  I want to put, get, delete, and scan records in a table
  So that I can maintain my data store

  Scenario: Put and get a record
    Given a table "users" with primary key "id"
    When I put a record {"id": "user-1", "name": "Alice", "email": "alice@example.com"}
    Then getting record "user-1" should return {"id": "user-1", "name": "Alice", "email": "alice@example.com"}

  Scenario: Put overwrites an existing record (upsert)
    Given a table "users" with primary key "id"
    And a record {"id": "user-1", "name": "Alice"}
    When I put a record {"id": "user-1", "name": "Alice Updated"}
    Then getting record "user-1" should return {"id": "user-1", "name": "Alice Updated"}

  Scenario: Get a non-existent record returns null
    Given a table "users" with primary key "id"
    When I get record "non-existent"
    Then the result should be null

  Scenario: Delete a record
    Given a table "users" with primary key "id"
    And a record {"id": "user-1", "name": "Alice"}
    When I delete record "user-1"
    Then getting record "user-1" should return null
    And the table should contain 0 records

  Scenario: Delete a non-existent record is a no-op
    Given a table "users" with primary key "id"
    When I delete record "non-existent"
    Then no error should occur

  Scenario: Scan returns all records
    Given a table "users" with primary key "id"
    And records:
      | id     | name  |
      | user-1 | Alice |
      | user-2 | Bob   |
      | user-3 | Carol |
    When I scan the table
    Then the result should contain 3 records

  Scenario: Bulk load from a list of records
    Given a table "users" with primary key "id"
    When I bulk load 100 records
    Then the table should contain 100 records

  Scenario: Put and get with composite primary key
    Given a table "scores" with partition key "user_id" and sort key "game_id"
    When I put a record {"user_id": "user-1", "game_id": "game-A", "score": 100}
    Then getting record with partition "user-1" and sort "game-A" should return that record

  Scenario: Delete with composite primary key
    Given a table "scores" with partition key "user_id" and sort key "game_id"
    And a record {"user_id": "user-1", "game_id": "game-A", "score": 100}
    When I delete record with partition "user-1" and sort "game-A"
    Then getting record with partition "user-1" and sort "game-A" should return null

  Scenario: Two records with same partition key but different sort keys coexist
    Given a table "scores" with partition key "user_id" and sort key "game_id"
    When I put a record {"user_id": "user-1", "game_id": "game-A", "score": 100}
    And I put a record {"user_id": "user-1", "game_id": "game-B", "score": 200}
    Then the table should contain 2 records
    And getting record with partition "user-1" and sort "game-A" should return score 100
    And getting record with partition "user-1" and sort "game-B" should return score 200
