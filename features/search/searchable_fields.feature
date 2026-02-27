Feature: Searchable fields in schema
  As a developer
  I want to declare searchable fields in the schema
  So that keyword indexes only the intended fields

  Scenario: Searchable fields are reported in describe
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
      | id | title       | body           |
      | n1 | Alpha Beta  | Gamma Delta    |
    When I load the schema
    And I call describe on the database
    Then the "news" table should list searchable fields "title" and "body"
