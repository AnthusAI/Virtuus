Feature: GSI query
  As a developer querying indexed data
  I want to query GSIs by partition key with optional sort conditions
  So that I can efficiently retrieve subsets of records

  Scenario: Query hash-only GSI by partition key
    Given a hash-only GSI "by_status" populated with:
      | pk     | status   |
      | user-1 | active   |
      | user-2 | active   |
      | user-3 | inactive |
    When I query the GSI for partition "active"
    Then the result should contain PKs "user-1" and "user-2"
    And the result should not contain "user-3"

  Scenario: Query hash+range GSI returns results sorted by range key
    Given a hash+range GSI "by_org" with partition key "org_id" and sort key "created_at" populated with:
      | pk     | org_id | created_at |
      | user-1 | org-a  | 2025-03-01 |
      | user-2 | org-a  | 2025-01-15 |
      | user-3 | org-a  | 2025-06-20 |
    When I query the GSI for partition "org-a"
    Then the result should return PKs in order: "user-2", "user-1", "user-3"

  Scenario: Query with sort condition filters by range key
    Given a hash+range GSI "by_org" with partition key "org_id" and sort key "created_at" populated with:
      | pk     | org_id | created_at |
      | user-1 | org-a  | 2025-01-01 |
      | user-2 | org-a  | 2025-06-01 |
      | user-3 | org-a  | 2025-12-01 |
    When I query the GSI for partition "org-a" with sort condition gte "2025-06-01"
    Then the result should contain PKs "user-2" and "user-3"
    And the result should not contain "user-1"

  Scenario: Query with between sort condition
    Given a hash+range GSI "by_org" with partition key "org_id" and sort key "created_at" populated with:
      | pk     | org_id | created_at |
      | user-1 | org-a  | 2025-01-01 |
      | user-2 | org-a  | 2025-06-01 |
      | user-3 | org-a  | 2025-12-01 |
    When I query the GSI for partition "org-a" with sort condition between "2025-03-01" and "2025-09-01"
    Then the result should contain only "user-2"

  Scenario: Query with descending sort direction
    Given a hash+range GSI "by_org" with partition key "org_id" and sort key "created_at" populated with:
      | pk     | org_id | created_at |
      | user-1 | org-a  | 2025-01-01 |
      | user-2 | org-a  | 2025-06-01 |
      | user-3 | org-a  | 2025-12-01 |
    When I query the GSI for partition "org-a" with sort direction "desc"
    Then the result should return PKs in order: "user-3", "user-2", "user-1"

  Scenario: Query empty partition returns empty result
    Given a hash-only GSI "by_status" with no records
    When I query the GSI for partition "active"
    Then the result should be empty

  Scenario: Query non-existent partition returns empty result
    Given a hash-only GSI "by_status" populated with:
      | pk     | status |
      | user-1 | active |
    When I query the GSI for partition "suspended"
    Then the result should be empty
