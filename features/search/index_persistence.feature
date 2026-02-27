Feature: Search index persistence
  As an operator
  I want keyword indexes to persist across runs
  So that startup is fast and search stays accurate

  Scenario: Search index files are persisted after load
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
      | id | title       | body         |
      | n1 | Alpha Beta  | Gamma Delta  |
    When I load the schema
    Then the search index should be persisted for "news"
