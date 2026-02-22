Feature: Benchmark scenarios
  As a developer tracking performance
  I want to run benchmark scenarios that produce valid timing output
  So that I can visualize performance and detect regressions

  Scenario: Single table cold load benchmark runs
    Given generated fixture data for the "social_media" profile
    When I run the "single_table_cold_load" benchmark
    Then the output should include a timing measurement in milliseconds

  Scenario: Full database cold load benchmark runs
    Given generated fixture data for the "social_media" profile
    When I run the "full_database_cold_load" benchmark
    Then the output should include a timing measurement in milliseconds

  Scenario: PK lookup benchmark runs
    Given a warm database loaded from fixture data
    When I run the "pk_lookup" benchmark for 1000 iterations
    Then the output should include p50, p95, and p99 latency values

  Scenario: GSI partition lookup benchmark runs
    Given a warm database loaded from fixture data
    When I run the "gsi_partition_lookup" benchmark for 1000 iterations
    Then the output should include p50, p95, and p99 latency values

  Scenario: GSI sorted query benchmark runs
    Given a warm database loaded from fixture data
    When I run the "gsi_sorted_query" benchmark for 1000 iterations
    Then the output should include p50, p95, and p99 latency values

  Scenario: Incremental refresh benchmark runs
    Given a warm database loaded from fixture data
    When I add 1 file and run the "incremental_refresh" benchmark
    Then the output should include a timing measurement

  Scenario: Benchmark output is valid JSON
    Given generated fixture data
    When I run all benchmark scenarios
    Then the output file should contain valid JSON
    And each scenario should have a "name", "timings", and "metadata" field

  Scenario: Visualization generates charts
    Given valid benchmark JSON output
    When I run the visualization tool
    Then PNG chart files should be generated
    And a REPORT.md file should be generated

  Scenario: Regression detection compares against baseline
    Given benchmark results and a perf_baseline.json
    When I run the regression checker
    Then it should report pass or fail for each scenario against the baseline
