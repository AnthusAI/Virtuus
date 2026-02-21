Feature: CLI query mode
  As a user running one-off queries
  I want to run virtuus query from the command line
  So that I can load data, run a query, and get JSON results

  Scenario: Query a table by index
    Given a data directory with a "users" folder containing JSON files
    When I run virtuus query --dir ./data --table users --index by_email --where email=alice@example.com
    Then the output should be valid JSON
    And the output should contain the matching user record

  Scenario: Query with schema file
    Given a data directory and a schema.yml file
    When I run virtuus query --dir ./data --schema schema.yml --table users --pk user-1
    Then the output should be the user record for "user-1"

  Scenario: Query with no results
    Given a data directory with a "users" folder
    When I run virtuus query --dir ./data --table users --index by_email --where email=nobody@example.com
    Then the output should be an empty JSON array

  Scenario: Query with invalid table name
    Given a data directory with a "users" folder
    When I run virtuus query --dir ./data --table nonexistent
    Then the command should exit with a non-zero status
    And the error message should indicate the table was not found

  Scenario: Query prints results to stdout
    Given a data directory with records
    When I run virtuus query with valid parameters
    Then results should be printed to stdout as JSON
    And the process should exit with status 0
