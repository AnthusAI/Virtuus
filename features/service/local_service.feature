@python-only
Feature: Local service
  Scenario: Dispatch service operations through a resident table
    Given a local Virtuus service with a table
    Then the local service supports retained table operations
