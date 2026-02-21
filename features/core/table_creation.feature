Feature: Table creation
  As a developer setting up data storage
  I want to create tables with primary key configurations
  So that I can store and retrieve records efficiently

  Scenario: Create a table with a simple primary key
    Given I create a table "users" with primary key "id"
    Then the table "users" should exist
    And the table should use "id" as its primary key

  Scenario: Create a table with a composite primary key
    Given I create a table "scores" with partition key "item_id" and sort key "name"
    Then the table "scores" should exist
    And the table should use "item_id" as partition key and "name" as sort key

  Scenario: Table starts empty
    Given I create a table "users" with primary key "id"
    Then the table should contain 0 records
