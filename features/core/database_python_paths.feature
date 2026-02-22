@python-only
Feature: Python database path coverage
  Scenario: Load from YAML with directory, GSI, associations and describe/validate
    Given a temporary python data root with users fixture and schema yaml
    When I load the python database from that yaml
    Then describe() should report users table not stale with 1 record
    And validate should return no violations
