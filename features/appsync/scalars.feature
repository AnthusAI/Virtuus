@rust-only
Feature: AWS scalar validation
  AWS AppSync custom scalars should validate input according to AWS specifications.

  Scenario Outline: AWSDateTime scalar accepts valid dates
    Given a schema with AWSDateTime scalar
    When I parse the value "<value>" as AWSDateTime
    Then it should be valid

    Examples:
      | value |
      | 2026-09-24T12:34:56Z |
      | 2026-09-24T12:34:56+00:00 |
      | 2026-09-24T12:34:56.123Z |
      | 2026-09-24T12:34:56.123456Z |

  Scenario Outline: AWSDateTime scalar rejects invalid dates
    Given a schema with AWSDateTime scalar
    When I parse the value "<value>" as AWSDateTime
    Then it should be invalid

    Examples:
      | value |
      | not-a-date |
      | 2026/09/24 |
      | 12:34:56 |

  Scenario Outline: AWSDate scalar accepts valid dates
    Given a schema with AWSDate scalar
    When I parse the value "<value>" as AWSDate
    Then it should be valid

    Examples:
      | value |
      | 2026-09-24 |
      | 2026-01-01 |

  Scenario Outline: AWSDate scalar rejects invalid dates
    Given a schema with AWSDate scalar
    When I parse the value "<value>" as AWSDate
    Then it should be invalid

    Examples:
      | value |
      | 2026/09/24 |
      | 09-24-2026 |
      | not-a-date |

  Scenario Outline: AWSTime scalar accepts valid times
    Given a schema with AWSTime scalar
    When I parse the value "<value>" as AWSTime
    Then it should be valid

    Examples:
      | value |
      | 12:34:56 |
      | 12:34:56Z |
      | 12:34:56+00:00 |

  Scenario Outline: AWSEmail scalar accepts valid emails
    Given a schema with AWSEmail scalar
    When I parse the value "<value>" as AWSEmail
    Then it should be valid

    Examples:
      | value |
      | test@example.com |
      | user+tag@domain.co.uk |

  Scenario Outline: AWSEmail scalar rejects invalid emails
    Given a schema with AWSEmail scalar
    When I parse the value "<value>" as AWSEmail
    Then it should be invalid

    Examples:
      | value |
      | not-an-email |
      | @example.com |
      | test@ |

  Scenario Outline: AWSURL scalar accepts valid URLs
    Given a schema with AWSURL scalar
    When I parse the value "<value>" as AWSURL
    Then it should be valid

    Examples:
      | value |
      | https://example.com |
      | http://example.com |
      | https://example.com/path |

  Scenario Outline: AWSPhone scalar accepts valid phone numbers
    Given a schema with AWSPhone scalar
    When I parse the value "<value>" as AWSPhone
    Then it should be valid

    Examples:
      | value |
      | +1234567890 |
      | +1 (234) 567-8900 |

  Scenario Outline: AWSIPAddress scalar accepts valid IP addresses
    Given a schema with AWSIPAddress scalar
    When I parse the value "<value>" as AWSIPAddress
    Then it should be valid

    Examples:
      | value |
      | 192.168.1.1 |
      | ::1 |
      | 2001:db8::1 |

  Scenario Outline: AWSIPAddress scalar rejects invalid IP addresses
    Given a schema with AWSIPAddress scalar
    When I parse the value "<value>" as AWSIPAddress
    Then it should be invalid

    Examples:
      | value |
      | not-an-ip |
      | 256.256.256.256 |
