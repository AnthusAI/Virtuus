@rust-only
Feature: Building the router
  The router is built from the AppSync SDL and the contract. Relationship kinds
  the engine cannot resolve are refused when the router is built, not at request time.

  Scenario: A hasOne relationship is refused
    Given the blog contract with "/models/Post/relationships/0/kind" set to:
      """
      "hasOne"
      """
    When I build the blog router
    Then building the router fails with:
      """
      Failed to build schema: Post.blog: hasOne relationships are not supported
      """

  Scenario: SDL that does not parse is refused
    Given this router SDL:
      """
      type Query {
      """
    When I build the blog router
    Then building the router fails with:
      """
      Failed to parse SDL:  --> 3:1
        |
      3 | 
        | ^---
        |
        = expected field_definition
      """

  Scenario: SDL that references an undefined type is refused
    Given this router SDL:
      """
      type Query {
        thing: Missing
      }
      """
    When I build the blog router
    Then building the router fails with:
      """
      Failed to build schema: Type "Missing" not found
      """
