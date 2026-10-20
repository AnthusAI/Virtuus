@rust-only
Feature: Full-scope purge of a tenant project
  Scenario: Purge removes canonical, catalog and control state of only its scope
    Given two scopes each holding an issue, its history, a catalog row and journal state
    When the first scope is purged with an operation identifier
    Then the first scope has no canonical objects, catalog rows or control state except the purge record
    And the second scope is byte-identical to its state before the purge

  Scenario: Purge deletes the filesystem metadata beside every canonical object
    Given two scopes each holding an issue, its history, a catalog row and journal state
    When the first scope is purged with an operation identifier
    Then no object metadata sidecar remains for the first scope

  Scenario: Replaying a completed purge returns the recorded result
    Given two scopes each holding an issue, its history, a catalog row and journal state
    And the first scope is purged with an operation identifier
    When the first scope is purged again with the same operation identifier
    Then the recorded purge result is returned unchanged

  Scenario: A purge interrupted midway resumes with the same operation identifier
    Given two scopes each holding an issue, its history, a catalog row and journal state
    And a purge of the first scope that failed while deleting control objects
    When the first scope is purged again with the same operation identifier
    Then the first scope has no canonical objects, catalog rows or control state except the purge record
    And the second scope is byte-identical to its state before the purge

  Scenario: A purge interrupted while deleting canonical objects resumes
    Given two scopes each holding an issue, its history, a catalog row and journal state
    And a purge of the first scope that failed while deleting canonical objects
    When the first scope is purged again with the same operation identifier
    Then the first scope has no canonical objects, catalog rows or control state except the purge record

  Scenario: A write after a purge never replays a pre-purge receipt
    Given two scopes each holding an issue, its history, a catalog row and journal state
    And the first scope is purged with an operation identifier
    When the original create operation is submitted again to the first scope
    Then the issue is published afresh in the first scope
