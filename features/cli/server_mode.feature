@python-only
Feature: CLI server mode
  As a developer running Virtuus in a container
  I want to start a persistent HTTP server
  So that I can serve queries without reloading data for each request

  Scenario: Start server and accept a query
    Given a data directory and a schema.yml file
    When I start virtuus serve --dir ./data --schema schema.yml --port 8080
    And I POST a JSON query {"users": {"pk": "user-1"}} to http://localhost:8080/query
    Then the response should be valid JSON containing the user record

  Scenario: Server responds with correct content type
    Given a running Virtuus server
    When I POST a query
    Then the response Content-Type should be application/json

  Scenario: Server handles invalid query gracefully
    Given a running Virtuus server
    When I POST an invalid JSON body
    Then the response should have a 400 status code
    And the response should include an error message

  Scenario: Server loads data once on startup
    Given a data directory with a schema
    When I start the server and send 10 queries
    Then data should be loaded from disk only once at startup

  Scenario: Server supports warm refresh
    Given a running Virtuus server with loaded data
    When a file is added to a table's directory
    And I POST a query for that table
    Then the response should include the new record via JIT refresh

  Scenario: Health endpoint returns server status
    Given a running Virtuus server
    When I GET /health
    Then the response should have a 200 status code
    And the response should be valid JSON with server status

  Scenario: Describe endpoint returns schema metadata
    Given a running Virtuus server with loaded data
    When I POST to /describe
    Then the response should be valid JSON with table metadata

  Scenario: Validate endpoint checks referential integrity
    Given a running Virtuus server with loaded data
    When I POST to /validate
    Then the response should be valid JSON with validation results

  Scenario: Warm endpoint triggers proactive refresh
    Given a running Virtuus server with loaded data
    When I POST to /warm
    Then the response should have a 200 status code
    And all tables should be refreshed
