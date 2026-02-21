Feature: Referential integrity under load
  As a developer handling concurrent mutations with associations
  I want association resolution to be safe during concurrent deletes
  So that queries never crash or return dangling references

  Scenario: Association resolution during concurrent deletes
    Given a database with "users" and "posts" tables
    And 100 users each with 10 posts
    When 5 threads continuously delete random users
    And 20 threads continuously resolve user posts associations
    Then association results should be either a valid list or empty
    And no thread should encounter an unhandled error

  Scenario: Belongs-to resolution during concurrent parent deletes
    Given a database with "posts" belonging to "users"
    And 100 posts referencing 50 users
    When 5 threads continuously delete random users
    And 20 threads continuously resolve post author associations
    Then author results should be either a valid user or null
    And no thread should crash

  Scenario: Validate during concurrent mutations
    Given a database with "users" and "posts" tables
    When writers continuously delete users
    And a thread calls db.validate()
    Then validate should return a list of violations without crashing
    And each violation should reference a real missing target
