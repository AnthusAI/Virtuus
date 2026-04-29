Feature: Library availability
  As a developer integrating Virtuus
  I want the library to load and report its version
  So that I can verify the installation is working

  Scenario: Library loads and reports version
    Given the virtuus library is available
    Then it should report a valid version string
    And the CLI helper should report the same version string

  Scenario: Package version aligns with shared VERSION file
    Given a VERSION file at the repository root
    Then the package version should match the contents of that file

  Scenario: Python backend reads version from package metadata
    Then the Python backend should read version from package metadata

  Scenario: Python and Rust report the same version
    Given the Python virtuus library is available
    And the Rust virtuus binary is available
    Then both should report the same version string

  Scenario: Database loads from an in-memory schema dict
    Given a database schema dictionary:
      """
      {
        "tables": {
          "users": {"primary_key": "id"},
          "posts": {"primary_key": "id"}
        }
      }
      """
    When I create a database from the schema dictionary
    Then the database should contain tables "users" and "posts"
