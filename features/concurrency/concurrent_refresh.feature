Feature: Concurrent refresh
  As a developer with files changing while queries run
  I want refresh and reads to be safe together
  So that queries return either old or new state, never a mix

  Scenario: Query during refresh returns consistent state
    Given a database with "users" table loaded from 1000 files
    And 100 new files are added to the directory
    When a refresh is triggered while 20 reader threads are querying
    Then each reader should see either the old state or the new state
    And no reader should see a partial mix of old and new

  Scenario: Multiple concurrent refreshes do not corrupt state
    Given a database with "users" table loaded from files
    When 5 threads simultaneously trigger warm()
    Then the table should end in a consistent state
    And no files should be loaded more than necessary
