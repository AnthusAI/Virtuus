Feature: Database describe
  As a developer inspecting a database
  I want to call db.describe() for a full schema overview
  So that I can understand the database configuration at a glance

  Scenario: Describe returns all table names
    Given a database with tables "users", "posts", and "comments"
    When I call describe on the database
    Then the result should list all 3 table names

  Scenario: Describe includes per-table metadata
    Given a database with a "users" table with GSI "by_email" and 25 records
    When I call describe on the database
    Then the "users" entry should include primary_key, GSIs, record_count, and staleness

  Scenario: Describe includes association information
    Given a database with "users" having a has_many "posts" association
    When I call describe on the database
    Then the "users" entry should list the "posts" association

  Scenario: Describe on empty database
    Given an empty database
    When I call describe on the database
    Then the result should be an empty schema with no tables
