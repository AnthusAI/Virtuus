@rust-only
Feature: Contract loading, tables and indexes

  Scenario: Blog fixture creates expected tables and indexes
    Given a blog contract
    When I open the engine
    Then the engine has a Blog table
    And the engine has a Post table with postsByBlog index
    And the engine has a Comment table with commentsByPost index
    And the engine has a Tag table with composite key

  Scenario: Apricitus contract opens successfully
    Given an apricitus contract
    When I open the engine
    Then the contract is valid

  Scenario: The blog and Apricitus fixtures are valid contracts
    Given a blog contract
    When I open the engine
    Then the contract is valid

  Scenario Outline: An invalid contract is rejected
    Given the blog contract with <op> at "<pointer>" and value <value>
    When I open the engine
    Then the error contains "<message_fragment>"

    Examples:
      | op | pointer | value | message_fragment |
      | removed | /models |  | is a required property |
      | replaced by: | /enums/PostStatus | 1 | is not of type |
      | replaced by: | /models/Post/fields/status/type | "Nope" | undefined enum |
      | replaced by: | /models/Tag/primaryKey | ["postId","nope"] | does not exist |
      | replaced by: | /models/Post/indexes/0/partitionField | "nope" | does not exist |
      | replaced by: | /models/Comment/relationships/0/target | "Nope" | non-existent model |
      | replaced by: | /models/Post/fields/meta/type | "Nope" | undefined customType |
