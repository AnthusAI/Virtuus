Feature: Self-referential associations
  As a developer modeling hierarchical data
  I want tables to have associations pointing to themselves
  So that I can represent parent/child trees

  Scenario: Define a self-referential has_many association
    Given a table "categories" with primary key "id" and a GSI "by_parent" on "parent_id"
    When I define a has_many association "children" on "categories" targeting table "categories" via index "by_parent"
    Then the "categories" table should have an association named "children"

  Scenario: Define a self-referential belongs_to association
    Given a table "categories" with primary key "id"
    When I define a belongs_to association "parent" on "categories" targeting table "categories" via foreign key "parent_id"
    Then the "categories" table should have an association named "parent"

  Scenario: Resolve children of a parent node
    Given a table "categories" with a self-referential has_many "children" via GSI "by_parent"
    And categories:
      | id    | name        | parent_id |
      | cat-1 | Root        |           |
      | cat-2 | Child A     | cat-1     |
      | cat-3 | Child B     | cat-1     |
      | cat-4 | Grandchild  | cat-2     |
    When I resolve the "children" association for category "cat-1"
    Then the result should contain "cat-2" and "cat-3"
    And the result should not contain "cat-4"

  Scenario: Resolve parent of a child node
    Given a table "categories" with a self-referential belongs_to "parent" via "parent_id"
    And categories:
      | id    | name    | parent_id |
      | cat-1 | Root    |           |
      | cat-2 | Child A | cat-1     |
    When I resolve the "parent" association for category "cat-2"
    Then the result should be the category "cat-1"

  Scenario: Root node has no parent
    Given a table "categories" with a self-referential belongs_to "parent" via "parent_id"
    And category {"id": "cat-1", "name": "Root"} with no parent_id
    When I resolve the "parent" association for category "cat-1"
    Then the result should be null
