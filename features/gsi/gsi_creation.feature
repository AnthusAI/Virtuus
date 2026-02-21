Feature: GSI creation
  As a developer defining table indexes
  I want to create Global Secondary Indexes with partition and optional sort keys
  So that I can query records by non-primary-key fields efficiently

  Scenario: Create a hash-only GSI
    Given a GSI named "by_email" with partition key "email"
    Then the GSI should exist with partition key "email"
    And the GSI should have no sort key

  Scenario: Create a hash+range GSI
    Given a GSI named "by_org" with partition key "org_id" and sort key "created_at"
    Then the GSI should exist with partition key "org_id"
    And the GSI should have sort key "created_at"

  Scenario: Create multiple GSIs on the same table
    Given a GSI named "by_email" with partition key "email"
    And a GSI named "by_status" with partition key "status"
    Then both GSIs should exist independently
