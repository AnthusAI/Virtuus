Feature: Schema from YAML
  As a developer configuring a database
  I want to define tables, GSIs, and associations in a YAML file
  So that the schema is declarative, versionable, and shared between languages

  Scenario: Load a simple schema with one table
    Given a YAML schema file defining a "users" table with primary key "id"
    When I call Database.from_schema with that file and a data directory
    Then the database should have a "users" table with primary key "id"

  Scenario: Load a schema with GSIs
    Given a YAML schema file defining a "users" table with GSIs "by_email" and "by_status"
    When I load the schema
    Then the "users" table should have GSI "by_email" with partition key "email"
    And the "users" table should have GSI "by_status" with partition key "status"

  Scenario: Load a schema with associations
    Given a YAML schema file defining:
      """
      tables:
        users:
          primary_key: id
          directory: users
          associations:
            posts: { type: has_many, table: posts, index: by_user }
        posts:
          primary_key: id
          directory: posts
          gsis:
            by_user: { partition_key: user_id, sort_key: created_at }
          associations:
            author: { type: belongs_to, table: users, foreign_key: user_id }
      """
    When I load the schema
    Then "users" should have a has_many "posts" association
    And "posts" should have a belongs_to "author" association

  Scenario: Schema loads data from directories
    Given a YAML schema and data directories with JSON files
    When I call Database.from_schema with the schema and data root
    Then each table should be populated with records from its directory

  Scenario: Schema with composite primary key
    Given a YAML schema defining a table with partition_key "item_id" and sort_key "name"
    When I load the schema
    Then the table should use composite primary key

  Scenario: Invalid schema reports clear error
    Given a YAML schema file with a missing required field
    When I attempt to load the schema
    Then a clear error should be raised indicating what is missing
