Feature: Table file persistence
  As a developer using file-backed tables
  I want records to be persisted to JSON files on disk
  So that data survives process restarts

  Scenario: Put writes a JSON file to disk
    Given a table "users" backed by a directory
    When I put a record {"id": "user-1", "name": "Alice"}
    Then a JSON file for "user-1" should exist in the directory
    And the file should contain the record data

  Scenario: Delete removes the JSON file from disk
    Given a table "users" backed by a directory
    And a record {"id": "user-1", "name": "Alice"} persisted to disk
    When I delete record "user-1"
    Then the JSON file for "user-1" should not exist in the directory

  Scenario: Load reads all JSON files from a directory
    Given a directory with 5 JSON files representing user records
    When I create a table "users" and load from that directory
    Then the table should contain 5 records
    And each record should match its source file

  Scenario: Load ignores missing primary key files and handles duplicates
    Given a directory with 5 JSON files representing user records
    And a JSON file missing the "id" field
    And a JSON file with duplicate id "user-0" and name "Updated"
    When I create a table "users" and load from that directory
    And I load the table from that directory
    Then the table should contain 5 records

  Scenario: File writes are atomic (temp + rename)
    Given a table "users" backed by a directory
    When I put a record
    Then the write should use a temporary file followed by an atomic rename

  Scenario: Non-JSON files in the directory are ignored
    Given a directory with 3 JSON files and 2 non-JSON files
    When I load the table from that directory
    Then the table should contain 3 records
    And the non-JSON files should be untouched

  Scenario: Filename is pk.json for simple primary keys
    Given a table "users" with primary key "id" backed by a directory
    When I put a record {"id": "user-1", "name": "Alice"}
    Then the file should be named "user-1.json"

  Scenario: Filename is partition__sort.json for composite primary keys
    Given a table "scores" with partition key "user_id" and sort key "game_id" backed by a directory
    When I put a record {"user_id": "user-1", "game_id": "game-A", "score": 100}
    Then the file should be named "user-1__game-A.json"

  Scenario: PKs containing path separators are rejected
    Given a table "users" with primary key "id" backed by a directory
    When I put a record {"id": "user/1", "name": "Alice"}
    Then an error should be raised about invalid PK characters
    When I put a record {"id": "user\\1", "name": "Bob"}
    Then an error should be raised about invalid PK characters
