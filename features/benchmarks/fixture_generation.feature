@python-only @bench
Feature: Benchmark fixture generation
  As a developer measuring performance
  I want to generate realistic test data at various scales
  So that benchmarks reflect real-world usage patterns

  Scenario: Generate social_media fixtures at scale 1
    Given the "social_media" fixture profile at scale factor 1
    When I generate fixtures
    Then the "users" directory should contain 1000 JSON files
    And the "posts" directory should contain 10000 JSON files
    And the "comments" directory should contain 50000 JSON files

  Scenario: Scale factor multiplies record counts
    Given the "social_media" fixture profile at scale factor 2
    When I generate fixtures
    Then the "users" directory should contain 2000 JSON files
    And the "posts" directory should contain 20000 JSON files

  Scenario: Generated records have valid associations
    Given generated "social_media" fixtures
    Then every post's "user_id" should reference an existing user
    And every comment's "post_id" should reference an existing post

  Scenario: complex_hierarchy profile generates 10+ tables
    Given the "complex_hierarchy" fixture profile at scale factor 1
    When I generate fixtures
    Then at least 10 table directories should be created
    And the total record count should exceed 900000

  Scenario: Generated data has realistic distribution
    Given generated "social_media" fixtures
    Then user statuses should be distributed across "active", "inactive", "suspended"
    And post dates should span the configured date range
