@python-only @bench
Feature: Search benchmark scenarios
  As a developer tracking performance
  I want search benchmarks for single and multi-term queries
  So that I can compare index-only performance to baseline

  Scenario: Single-term search benchmark runs
    Given generated fixture data for the "bbc_news_alltime" profile
    When I run the "search_single_term" benchmark for 500 iterations
    Then the output should include p50, p95, and p99 latency values

  Scenario: Multi-term search benchmark runs
    Given generated fixture data for the "bbc_news_alltime" profile
    When I run the "search_multi_term" benchmark for 500 iterations
    Then the output should include p50, p95, and p99 latency values
