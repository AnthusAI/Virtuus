@python-only
Feature: Concurrent read-write
  As a developer with mixed read/write workloads
  I want readers and writers to operate safely in parallel
  So that readers never see corrupted or partial state

  Scenario: Readers see consistent state during writes
    Given a database with "users" table containing 1000 records
    When 10 writer threads continuously put new records
    And 50 reader threads continuously scan the table
    Then readers should never see a partially-indexed record
    And all written records should eventually be visible to readers

  Scenario: GSI queries remain consistent during puts
    Given a database with "users" table and GSI "by_status" on "status"
    When writers continuously put records with status "active"
    And readers continuously query the GSI for "active"
    Then every record returned by the GSI should exist in the table
    And no reader should encounter an error
