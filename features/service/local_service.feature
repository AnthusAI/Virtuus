@python-only
Feature: Local service
  Scenario: Dispatch service operations through a resident table
    Given a local Virtuus service with a table
    Then the local service supports retained table operations

  Scenario: Virtuus imports on a platform without Unix domain sockets
    Given the platform does not provide Unix domain socket servers
    When the virtuus package is imported
    Then the virtuus package exposes the local service
    And starting a Unix service fails with "Unix domain sockets are not available on this platform"
