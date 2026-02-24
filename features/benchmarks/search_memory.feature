@python-only @bench
Feature: Search memory benchmark
  As an operator
  I want memory benchmarks that include searchable fields
  So that I can quantify search index overhead

  Scenario: Memory harness runs with searchable fields enabled
    Given a memory benchmark configuration with search enabled
    When I run the memory benchmark harness
    Then the memory benchmark output should include RSS measurements
