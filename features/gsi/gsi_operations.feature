Feature: GSI operations
  As a developer maintaining indexed data
  I want GSIs to stay in sync as records are added, removed, and updated
  So that queries always return accurate results

  Scenario: Put indexes a record in a hash-only GSI
    Given a hash-only GSI "by_status" with partition key "status"
    When I put a record with pk "user-1" and status "active"
    Then querying the GSI for partition "active" should include "user-1"

  Scenario: Put indexes a record in a hash+range GSI
    Given a hash+range GSI "by_org" with partition key "org_id" and sort key "created_at"
    When I put a record with pk "user-1", org_id "org-a", and created_at "2025-01-15"
    Then querying the GSI for partition "org-a" should include "user-1"

  Scenario: Remove de-indexes a record from a hash-only GSI
    Given a hash-only GSI "by_status" with partition key "status"
    And a record with pk "user-1" and status "active" is indexed
    When I remove the record with pk "user-1" and status "active"
    Then querying the GSI for partition "active" should not include "user-1"

  Scenario: Remove de-indexes a record from a hash+range GSI
    Given a hash+range GSI "by_org" with partition key "org_id" and sort key "created_at"
    And a record with pk "user-1", org_id "org-a", and created_at "2025-01-15" is indexed
    When I remove the record with pk "user-1", org_id "org-a", and created_at "2025-01-15"
    Then querying the GSI for partition "org-a" should not include "user-1"

  Scenario: Update re-indexes a record when the partition key changes
    Given a hash-only GSI "by_status" with partition key "status"
    And a record with pk "user-1" and status "active" is indexed
    When I update the record with pk "user-1" from status "active" to "inactive"
    Then querying the GSI for partition "active" should not include "user-1"
    And querying the GSI for partition "inactive" should include "user-1"

  Scenario: Update re-indexes a record when the sort key changes
    Given a hash+range GSI "by_org" with partition key "org_id" and sort key "created_at"
    And a record with pk "user-1", org_id "org-a", and created_at "2025-01-15" is indexed
    When I update the record with pk "user-1" to created_at "2025-06-01" (same org_id)
    Then the record should appear at the new sort position in partition "org-a"

  Scenario: Multiple records in the same partition
    Given a hash-only GSI "by_status" with partition key "status"
    When I put records "user-1", "user-2", "user-3" all with status "active"
    Then querying the GSI for partition "active" should return all 3 PKs
