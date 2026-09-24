@rust-only
Feature: Engine CRUD operations

  Scenario: Creating a blog with auto-generated id and timestamps
    Given a blog contract
    When I open the engine
    When I create a Blog with:
      """
      {"id": "b1", "title": "My Blog"}
      """
    Then the operation succeeds
    Then the result data id is "b1"
    Then the result data has __typename "Blog"
    Then the result data has createdAt in ISO-8601 with milliseconds
    Then the result data has updatedAt in ISO-8601 with milliseconds

  Scenario: Creating a duplicate blog fails with ConditionalCheckFailedException
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "First"}
      """
    When I create a Blog with:
      """
      {"id": "b1", "title": "Duplicate"}
      """
    Then the operation fails with errorType "DynamoDB:ConditionalCheckFailedException"

  Scenario: Creating a post with status populates composite sort attribute
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p1", "blogId": "b1", "title": "Post", "status": "DRAFT"}
      """
    Then the operation succeeds
    Then the result data has status#createdAt as "DRAFT#" followed by ISO-8601

  Scenario: Creating a tag with composite identifier
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    Given a Post exists with:
      """
      {"id": "p1", "blogId": "b1", "title": "Post"}
      """
    When I create a Tag with:
      """
      {"postId": "p1", "name": "featured", "value": "yes"}
      """
    Then the operation succeeds
    Then the result data postId is "p1"
    Then the result data name is "featured"

  Scenario: Creating a post with invalid enum value fails
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p1", "blogId": "b1", "title": "Post", "status": "BOGUS"}
      """
    Then the operation fails with error containing "enum"

  Scenario: Getting an existing blog
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    When I get the Blog with id "b1"
    Then the operation succeeds
    Then the result data id is "b1"
    Then the result data title is "Blog"

  Scenario: Getting a missing blog returns null data with no errors
    Given a blog contract
    When I open the engine
    When I get the Blog with id "missing-id"
    Then the operation succeeds
    Then the result data is null
    Then there are no errors

  Scenario: Updating a blog title
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Original"}
      """
    When I update the Blog with:
      """
      {"id": "b1", "title": "Updated"}
      """
    Then the operation succeeds
    Then the result data title is "Updated"

  Scenario: Updating a post status updates composite sort attribute
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    Given a Post exists with:
      """
      {"id": "p1", "blogId": "b1", "title": "Post", "status": "DRAFT"}
      """
    When I update the Post with:
      """
      {"id": "p1", "status": "PUBLISHED"}
      """
    Then the operation succeeds
    Then the result data status is "PUBLISHED"
    Then the result data has status#createdAt as "PUBLISHED#" followed by ISO-8601

  Scenario: Updating a field to null removes it
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    Given a Post exists with:
      """
      {"id": "p1", "blogId": "b1", "title": "Post", "status": "DRAFT"}
      """
    When I update the Post with:
      """
      {"id": "p1", "status": null}
      """
    Then the operation succeeds
    Then the result data status is null or missing

  Scenario: Updating a missing blog fails with ConditionalCheckFailedException
    Given a blog contract
    When I open the engine
    When I update the Blog with:
      """
      {"id": "missing", "title": "Updated"}
      """
    Then the operation fails with errorType "DynamoDB:ConditionalCheckFailedException"

  Scenario: Deleting an existing blog returns the deleted item
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    When I delete the Blog with id "b1"
    Then the operation succeeds
    Then the result data id is "b1"
    Then the result data title is "Blog"

  Scenario: Deleting a missing blog fails with ConditionalCheckFailedException
    Given a blog contract
    When I open the engine
    When I delete the Blog with id "missing-id"
    Then the operation fails with errorType "DynamoDB:ConditionalCheckFailedException"

  Scenario Outline: AWS scalar type validation - AWSDateTime
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p1", "blogId": "b1", "title": "Post", "createdAt": <createdAtValue>}
      """
    Then <result>

    Examples:
      | createdAtValue | result |
      | "2025-01-15T10:30:45.123Z" | the operation succeeds |
      | "not-a-date" | the operation fails with error containing "AWSDateTime" |

  Scenario Outline: AWS scalar type validation - AWSDate
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b2", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p2", "blogId": "b2", "title": "Post", "publishDate": <publishDateValue>}
      """
    Then <result>

    Examples:
      | publishDateValue | result |
      | "2025-01-15" | the operation succeeds |
      | "invalid-date" | the operation fails with error containing "AWSDate" |

  Scenario Outline: AWS scalar type validation - AWSJSON
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b3", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p3", "blogId": "b3", "title": "Post", "content": <contentValue>}
      """
    Then <result>

    Examples:
      | contentValue | result |
      | "{\"text\": \"body\"}" | the operation succeeds |
      | "not-valid-json" | the operation fails with error containing "AWSJSON" |

  Scenario Outline: AWS scalar type validation - AWSEmail
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b4", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p4", "blogId": "b4", "title": "Post", "authorEmail": <emailValue>}
      """
    Then <result>

    Examples:
      | emailValue | result |
      | "author@example.com" | the operation succeeds |
      | "invalid-email" | the operation fails with error containing "AWSEmail" |

  Scenario Outline: AWS scalar type validation - AWSURL
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b5", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p5", "blogId": "b5", "title": "Post", "sourceUrl": <urlValue>}
      """
    Then <result>

    Examples:
      | urlValue | result |
      | "https://example.com/post" | the operation succeeds |
      | "not-a-url" | the operation fails with error containing "AWSURL" |

  Scenario Outline: AWS scalar type validation - Int
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b6", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p6", "blogId": "b6", "title": "Post", "views": <viewsValue>}
      """
    Then <result>

    Examples:
      | viewsValue | result |
      | 42 | the operation succeeds |
      | "not-a-number" | the operation fails with error containing "integer" |

  Scenario Outline: AWS scalar type validation - Float
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b7", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p7", "blogId": "b7", "title": "Post", "rating": <ratingValue>}
      """
    Then <result>

    Examples:
      | ratingValue | result |
      | 4.5 | the operation succeeds |
      | "not-a-number" | the operation fails with error containing "number" |

  Scenario Outline: AWS scalar type validation - Boolean
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b8", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p8", "blogId": "b8", "title": "Post", "published": <publishedValue>}
      """
    Then <result>

    Examples:
      | publishedValue | result |
      | true | the operation succeeds |
      | "not-a-boolean" | the operation fails with error containing "boolean" |

  Scenario Outline: AWS scalar type validation - String
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b9", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p9", "blogId": "b9", "title": <titleValue>}
      """
    Then <result>

    Examples:
      | titleValue | result |
      | "Post Title" | the operation succeeds |
      | 123 | the operation fails with error containing "string" |

  Scenario Outline: AWS scalar type validation - Type mismatches
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b10", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p10", "blogId": "b10", "title": "Post", "createdAt": <invalidType>}
      """
    Then the operation fails with error containing "string"

    Examples:
      | invalidType |
      | 123 |
      | true |
      | {"nested": "object"} |

  Scenario Outline: AWS scalar type validation - Type mismatches for other types
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b11", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p11", "blogId": "b11", "title": "Post", "content": <invalidType>}
      """
    Then the operation fails with error containing "string"

    Examples:
      | invalidType |
      | 123 |
      | true |
      | ["array", "value"] |

  Scenario Outline: AWS scalar type validation - Type mismatches for date fields
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b12", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p12", "blogId": "b12", "title": "Post", "publishDate": <numberValue>, "authorEmail": <numberValue>, "sourceUrl": <numberValue>}
      """
    Then the operation fails with error containing "string"

    Examples:
      | numberValue |
      | 5 |

  Scenario: Required field missing on create
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b13", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p13", "blogId": "b13"}
      """
    Then the operation fails with error containing "Field 'title' is required"

  Scenario: Enum with non-string value
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b14", "title": "Blog"}
      """
    When I create a Post with:
      """
      {"id": "p14", "blogId": "b14", "title": "Post", "status": 5}
      """
    Then the operation fails with error containing "must be a string for enum type"

  Scenario: Update with invalid enum value
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b15", "title": "Blog"}
      """
    Given a Post exists with:
      """
      {"id": "p15", "blogId": "b15", "title": "Post", "status": "DRAFT"}
      """
    When I update the Post with:
      """
      {"id": "p15", "status": "BOGUS"}
      """
    Then the operation fails with error containing "must be one of enum values"
    When I get the Post with id "p15"
    Then the result data status is "DRAFT"

  Scenario: Unknown operation
    Given a blog contract
    When I open the engine
    When I call operation "frobnicate"
    Then the operation fails

  Scenario: Create Vote with three-field key
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b16", "title": "Blog"}
      """
    Given a Post exists with:
      """
      {"id": "p16", "blogId": "b16", "title": "Post"}
      """
    When I create a Vote with:
      """
      {"postId": "p16", "userId": "u1", "kind": "up"}
      """
    Then the operation succeeds
    Then the result data field "userId#kind" is "u1#up"
    When I get the Vote with keys postId "p16" and userId "u1" and kind "up"
    Then the operation succeeds
    Then the result data field "userId#kind" is "u1#up"

  Scenario: Persistence across engine restarts
    Given a blog contract
    When I open the engine with a temporary directory
    Given a Blog exists with:
      """
      {"id": "b17", "title": "Blog"}
      """
    Given a Post exists with:
      """
      {"id": "p17", "blogId": "b17", "title": "Post One"}
      """
    Given a Post exists with:
      """
      {"id": "p18", "blogId": "b17", "title": "Post Two"}
      """
    When I create a Vote with:
      """
      {"postId": "p17", "userId": "u17", "kind": "up"}
      """
    Then the operation succeeds
    When I close and reopen the engine
    When I get the Post with id "p17"
    Then the operation succeeds
    Then the result data title is "Post One"
    Then querying postsByBlog for blogId "b17" returns ids ["p17", "p18"]
    Then the storage directory has per-model folders
