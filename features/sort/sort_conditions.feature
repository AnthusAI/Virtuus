Feature: Sort condition operators
  As a developer querying indexed data
  I want to filter records using DynamoDB-style sort conditions
  So that I can express range queries on sorted index keys

  Scenario Outline: Equality operator
    Given a sort condition of "eq" with value "<value>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | value   | input   | result |
      | alice   | alice   | true   |
      | alice   | bob     | false  |
      | 42      | 42      | true   |
      | 42      | 43      | false  |

  Scenario Outline: Not-equal operator
    Given a sort condition of "ne" with value "<value>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | value   | input   | result |
      | alice   | alice   | false  |
      | alice   | bob     | true   |

  Scenario Outline: Less-than operator
    Given a sort condition of "lt" with value "<value>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | value   | input   | result |
      | 10      | 5       | true   |
      | 10      | 10      | false  |
      | 10      | 15      | false  |
      | banana  | apple   | true   |
      | banana  | banana  | false  |
      | banana  | cherry  | false  |

  Scenario Outline: Less-than-or-equal operator
    Given a sort condition of "lte" with value "<value>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | value   | input   | result |
      | 10      | 5       | true   |
      | 10      | 10      | true   |
      | 10      | 15      | false  |

  Scenario Outline: Greater-than operator
    Given a sort condition of "gt" with value "<value>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | value   | input   | result |
      | 10      | 15      | true   |
      | 10      | 10      | false  |
      | 10      | 5       | false  |

  Scenario Outline: Greater-than-or-equal operator
    Given a sort condition of "gte" with value "<value>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | value   | input   | result |
      | 10      | 15      | true   |
      | 10      | 10      | true   |
      | 10      | 5       | false  |

  Scenario Outline: Between operator
    Given a sort condition of "between" with low "<low>" and high "<high>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | low | high | input | result |
      | 5   | 15   | 10    | true   |
      | 5   | 15   | 5     | true   |
      | 5   | 15   | 15    | true   |
      | 5   | 15   | 3     | false  |
      | 5   | 15   | 20    | false  |
      | a   | m    | dog   | true   |
      | a   | m    | zebra | false  |

  Scenario Outline: Begins-with operator
    Given a sort condition of "begins_with" with value "<prefix>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | prefix  | input       | result |
      | user-   | user-abc    | true   |
      | user-   | admin-abc   | false  |
      | user-   | user-       | true   |
      |         | anything    | true   |

  Scenario Outline: Contains operator
    Given a sort condition of "contains" with value "<substring>"
    When evaluated against "<input>"
    Then the result should be <result>

    Examples:
      | substring | input         | result |
      | error     | server error  | true   |
      | error     | all good      | false  |
      | error     | error         | true   |
      |           | anything      | true   |

  Scenario: Null input returns false for all comparison operators
    Given a sort condition of "eq" with value "alice"
    When evaluated against a null value
    Then the result should be false

  Scenario: Null input returns false for between
    Given a sort condition of "between" with low "a" and high "z"
    When evaluated against a null value
    Then the result should be false

  Scenario: Empty string handling
    Given a sort condition of "eq" with value ""
    When evaluated against ""
    Then the result should be true
