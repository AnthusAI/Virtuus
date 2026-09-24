@rust-only
Feature: AWS scalar validation
  AWS AppSync custom scalars should validate input according to AWS specifications.

  Scenario Outline: AWSDateTime scalar accepts valid dates
    When I parse the value "<value>" as AWSDateTime
    Then it should be valid

    Examples:
      | value |
      | 2026-09-24T12:34:56Z |
      | 2026-09-24T12:34:56+00:00 |
      | 2026-09-24T12:34:56.123Z |
      | 2026-09-24T12:34:56.123456Z |

  Scenario Outline: AWSDateTime scalar rejects invalid dates
    When I parse the value "<value>" as AWSDateTime
    Then it should be invalid

    Examples:
      | value |
      | not-a-date |
      | 2026/09/24 |
      | 12:34:56 |
      | 2026-13-45T00:00:00Z |

  Scenario Outline: AWSDate scalar accepts valid dates
    When I parse the value "<value>" as AWSDate
    Then it should be valid

    Examples:
      | value |
      | 2026-09-24 |
      | 2026-01-01 |
      | 2024-02-29 |

  Scenario Outline: AWSDate scalar rejects invalid dates
    When I parse the value "<value>" as AWSDate
    Then it should be invalid

    Examples:
      | value |
      | 2026/09/24 |
      | 09-24-2026 |
      | not-a-date |
      | 2026-02-29 |
      | 2026-04-31 |
      | 2026-13-01 |

  Scenario Outline: AWSTime scalar accepts valid times
    When I parse the value "<value>" as AWSTime
    Then it should be valid

    Examples:
      | value |
      | 12:34:56 |
      | 12:34:56Z |
      | 12:34:56+00:00 |

  Scenario Outline: AWSEmail scalar accepts valid emails
    When I parse the value "<value>" as AWSEmail
    Then it should be valid

    Examples:
      | value |
      | test@example.com |
      | user+tag@domain.co.uk |

  Scenario Outline: AWSEmail scalar rejects invalid emails
    When I parse the value "<value>" as AWSEmail
    Then it should be invalid

    Examples:
      | value |
      | not-an-email |
      | @example.com |
      | test@ |

  Scenario Outline: AWSURL scalar accepts valid URLs
    When I parse the value "<value>" as AWSURL
    Then it should be valid

    Examples:
      | value |
      | https://example.com |
      | http://example.com |
      | https://example.com/path |

  Scenario Outline: AWSPhone scalar accepts valid phone numbers
    When I parse the value "<value>" as AWSPhone
    Then it should be valid

    Examples:
      | value |
      | +1234567890 |
      | +1 (234) 567-8900 |

  Scenario Outline: AWSIPAddress scalar accepts valid IP addresses
    When I parse the value "<value>" as AWSIPAddress
    Then it should be valid

    Examples:
      | value |
      | 192.168.1.1 |
      | ::1 |
      | 2001:db8::1 |

  Scenario Outline: AWSIPAddress scalar rejects invalid IP addresses
    When I parse the value "<value>" as AWSIPAddress
    Then it should be invalid

    Examples:
      | value |
      | not-an-ip |
      | 256.256.256.256 |

  Scenario: The schema accepts valid AWS scalar arguments
    Given this SDL:
      """
      type Query {
        check(date: AWSDate, stamp: AWSTimestamp): String
      }
      """
    When I build the schema
    And I run this query against the schema:
      """
      { check(date: "2026-09-24", stamp: 1790000000) }
      """
    Then the GraphQL response is:
      """
      {"data":{"check":null}}
      """

  Scenario: The schema rejects an invalid AWSDate argument
    Given this SDL:
      """
      type Query {
        check(date: AWSDate, stamp: AWSTimestamp): String
      }
      """
    When I build the schema
    And I run this query against the schema:
      """
      { check(date: "2026-13-45") }
      """
    Then the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Invalid value for argument \"date\", expected type \"AWSDate\"","locations":[{"line":2,"column":9}]}]}
      """

  Scenario: The schema rejects a non-numeric AWSTimestamp argument
    Given this SDL:
      """
      type Query {
        check(date: AWSDate, stamp: AWSTimestamp): String
      }
      """
    When I build the schema
    And I run this query against the schema:
      """
      { check(stamp: true) }
      """
    Then the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Invalid value for argument \"stamp\", expected type \"AWSTimestamp\"","locations":[{"line":2,"column":9}]}]}
      """

  Scenario: A custom scalar declared in the SDL accepts any value
    Given this SDL:
      """
      type Query {
        paint(color: Color): String
      }

      scalar Color
      """
    When I build the schema
    And I run this query against the schema:
      """
      { paint(color: "red") }
      """
    Then the GraphQL response is:
      """
      {"data":{"paint":null}}
      """
