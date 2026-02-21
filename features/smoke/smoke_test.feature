Feature: Library availability
  As a developer integrating Virtuus
  I want the library to load and report its version
  So that I can verify the installation is working

  Scenario: Library loads and reports version
    Given the virtuus library is available
    Then it should report a valid version string

  Scenario: Version is read from the shared VERSION file
    Given a VERSION file at the repository root
    Then the library version should match the contents of that file

  Scenario: Python and Rust report the same version
    Given the Python virtuus library is available
    And the Rust virtuus binary is available
    Then both should report the same version string
