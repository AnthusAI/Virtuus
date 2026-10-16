@rust-only
Feature: Portable canonical project storage
  Scenario: Raw files keep their normal relative paths
    Given an ordinary project file with whitespace and unknown fields
    When I conditionally create it in a tenant project
    Then reading the same relative path returns identical bytes
    And another tenant cannot read it

  Scenario: Stale writers cannot overwrite a current issue
    Given an issue and its opaque revision
    When a writer replaces the issue using that revision
    Then another writer using the old revision is rejected

  Scenario: Repair follows a committed issue without losing a newer issue
    Given a mutation whose issue was published but event publication failed
    When the same operation is retried after a newer mutation
    Then the original event is repaired exactly once
    And the newer issue remains current

  Scenario: A repeated command cannot change its meaning
    Given a successfully published operation
    When its operation identifier is reused for different bytes
    Then the request is rejected
