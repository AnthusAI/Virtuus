Feature: Full-text search queries
  As a developer
  I want to query via a search directive
  So that keyword search is available through execute

  Scenario: Single-term search returns matching records
    Given a YAML schema file defining:
      """
      tables:
        news:
          primary_key: id
          directory: news
          search:
            fields: [title, body]
      """
    And a data directory for "news" with records:
      | id | title       | body             |
      | n1 | Alpha Beta  | Gamma Delta      |
      | n2 | Omega       | Alpha Zeta       |
    When I load the schema
    And I execute {"news": {"search": "alpha"}}
    Then the result should contain exactly 2 records
    And the result should include record "n1"
    And the result should include record "n2"

  Scenario: Multi-term search uses AND semantics
    Given a YAML schema file defining:
      """
      tables:
        news:
          primary_key: id
          directory: news
          search:
            fields: [title, body]
      """
    And a data directory for "news" with records:
      | id | title       | body             |
      | n1 | Alpha Beta  | Gamma Delta      |
      | n2 | Omega       | Alpha Zeta       |
    When I load the schema
    And I execute {"news": {"search": "alpha beta"}}
    Then the result should contain exactly 1 records
    And the result should include record "n1"
