@rust-only
Feature: Error handling and edge cases

  Scenario: Query with unknown model raises error
    Given a blog contract
    When I open the engine
    And I try to query a non-existent model "UnknownModel" with operation "read"
    Then the error contains "Unknown model"

  Scenario: Create operation with unknown model raises error
    Given a blog contract
    When I open the engine
    And I try to create in non-existent model "UnknownModel"
    Then the error contains "Unknown model"

  Scenario: Get operation with unknown model raises error
    Given a blog contract
    When I open the engine
    And I try to get from non-existent model "UnknownModel" with pk "id1"
    Then the error contains "Unknown model"

  Scenario: Update operation with unknown model raises error
    Given a blog contract
    When I open the engine
    And I try to update in non-existent model "UnknownModel"
    Then the error contains "Unknown model"

  Scenario: Delete operation with unknown model raises error
    Given a blog contract
    When I open the engine
    And I try to delete from non-existent model "UnknownModel"
    Then the error contains "Unknown model"

  Scenario: List operation with unknown model raises error
    Given a blog contract
    When I open the engine
    And I try to list from non-existent model "UnknownModel"
    Then the error contains "Unknown model"
