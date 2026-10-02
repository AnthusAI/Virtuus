@rust-only
Feature: Filters and key conditions
  Filters follow DynamoDB rules (design/storage.md §2.2): `contains` on a list checks membership,
  `ne` matches a missing field, and any other comparison against a missing field is false.
  Key conditions apply to index sort keys.

  Background:
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    Given these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Post One", "status": "DRAFT", "views": 0, "rating": 4.5, "tags": ["featured", "popular"], "createdAt": "2026-01-15T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Post Two", "status": "PUBLISHED", "views": 100, "rating": 3.8, "tags": ["news"], "createdAt": "2026-01-20T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Another Draft", "status": "DRAFT", "views": 50, "tags": [], "createdAt": "2026-02-01T10:00:00Z"},
        {"id": "p4", "blogId": "b1", "title": "Archived Post", "status": "ARCHIVED", "views": 200, "tags": ["featured", "archive"], "createdAt": "2026-03-10T10:00:00Z"}
      ]
      """
    Given these Tag records exist:
      """
      [
        {"postId": "p1", "name": "featured", "value": "yes"},
        {"postId": "p1", "name": "popular", "value": "yes"},
        {"postId": "p2", "name": "news", "value": "yes"},
        {"postId": "p4", "name": "featured", "value": "yes"},
        {"postId": "p4", "name": "archive", "value": "yes"}
      ]
      """

  Scenario: Filter with eq on a string field
    When I list all Post with:
      """
      {"filter": {"title": {"eq": "Post One"}}}
      """
    Then the operation succeeds
    Then the result data has exactly 1 record with id "p1"

  Scenario: Filter with eq on an enum
    When I list all Post with:
      """
      {"filter": {"status": {"eq": "PUBLISHED"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2"]

  Scenario: Filter with ne on an enum
    When I list all Post with:
      """
      {"filter": {"status": {"ne": "DRAFT"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2", "p4"]

  Scenario: Filter with ne matches records missing the field
    When I list all Post with:
      """
      {"filter": {"rating": {"ne": 4.5}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2", "p3", "p4"]

  Scenario: Comparisons against a missing field are false
    When I list all Post with:
      """
      {"filter": {"rating": {"gt": 0}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2"]

  Scenario Outline: Numeric comparisons on views
    When I list all Post with:
      """
      {"filter": {"views": <condition>}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids <expected>

    Examples:
      | condition          | expected           |
      | {"lt": 100}        | ["p1", "p3"]       |
      | {"le": 100}        | ["p1", "p2", "p3"] |
      | {"gt": 50}         | ["p2", "p4"]       |
      | {"ge": 50}         | ["p3", "p2", "p4"] |
      | {"between": [50, 100]} | ["p2", "p3"]  |

  Scenario: Filter with beginsWith on a string field
    When I list all Post with:
      """
      {"filter": {"title": {"beginsWith": "Post"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2"]

  Scenario: Filter with contains on a string (substring match)
    When I list all Post with:
      """
      {"filter": {"title": {"contains": "Draft"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3"]

  Scenario: Filter with notContains on a string
    When I list all Post with:
      """
      {"filter": {"title": {"notContains": "Post"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3"]

  Scenario: Filter with attributeExists true
    When I list all Post with:
      """
      {"filter": {"rating": {"attributeExists": true}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2"]

  Scenario: Filter with attributeExists false
    When I list all Post with:
      """
      {"filter": {"rating": {"attributeExists": false}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3", "p4"]

  Scenario: Filter with size on a string
    When I list all Post with:
      """
      {"filter": {"title": {"size": {"eq": 8}}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2"]

  Scenario: Filter combining multiple conditions with and
    When I list all Post with:
      """
      {"filter": {"and": [{"status": {"eq": "DRAFT"}}, {"views": {"ge": 50}}]}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3"]

  Scenario: Filter combining conditions with or
    When I list all Post with:
      """
      {"filter": {"or": [{"status": {"eq": "PUBLISHED"}}, {"status": {"eq": "ARCHIVED"}}]}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2", "p4"]

  Scenario: Filter with not
    When I list all Post with:
      """
      {"filter": {"not": {"status": {"eq": "DRAFT"}}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2", "p4"]

  Scenario: Multiple fields in one filter object are combined with and
    When I list all Post with:
      """
      {"filter": {"status": {"eq": "DRAFT"}, "views": {"lt": 50}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1"]

  Scenario: Nested and/or combinations
    When I list all Post with:
      """
      {"filter": {"and": [{"or": [{"status": {"eq": "DRAFT"}}, {"status": {"eq": "ARCHIVED"}}]}, {"views": {"ge": 50}}]}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3", "p4"]

  Scenario: Index query by partition key only (postsByBlog)
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2", "p3", "p4"]

  Scenario: Index query with sort key condition (lt)
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"lt": "2026-09-24T13:00:00.000Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with sort key condition (between)
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"between": ["2026-09-24T00:00:00.000Z", "2026-09-24T23:59:59.999Z"]}}}
      """
    Then the operation succeeds

  Scenario: Index query with composite sort key condition (eq on first field)
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "eq": {"status": "DRAFT"}}}
      """
    Then the operation succeeds

  Scenario: Index query with filter applied
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "filter": {"status": {"eq": "PUBLISHED"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2"]

  Scenario: Filter with nested not and and
    When I list all Post with:
      """
      {"filter": {"not": {"and": [{"status": {"eq": "DRAFT"}}, {"views": {"lt": 100}}]}}}
      """
    Then the operation succeeds

  # Missing field tests: operators that return false on missing fields
  Scenario Outline: Missing field returns false for comparison operators
    When I list all Post with:
      """
      {"filter": {"missingField": <condition>}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

    Examples:
      | condition           |
      | {"eq": "value"}     |
      | {"lt": 100}         |
      | {"le": 100}         |
      | {"gt": 50}          |
      | {"ge": 50}          |
      | {"between": [1, 5]} |
      | {"beginsWith": "a"} |
      | {"contains": "x"}   |
      | {"size": {"eq": 5}} |

  # Missing field tests: operators that match missing fields
  Scenario: Missing field returns true for ne operator
    When I list all Post with:
      """
      {"filter": {"missingField": {"ne": "anything"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2", "p3", "p4"]

  Scenario: Missing field returns true for notContains operator
    When I list all Post with:
      """
      {"filter": {"missingField": {"notContains": "anything"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2", "p3", "p4"]

  Scenario: attributeExists false on missing field
    When I list all Post with:
      """
      {"filter": {"missingField": {"attributeExists": false}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2", "p3", "p4"]

  Scenario: attributeExists true on missing field
    When I list all Post with:
      """
      {"filter": {"missingField": {"attributeExists": true}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  # String ordering tests with lt/gt/between
  Scenario: String field with lt comparison (alphabetical)
    When I list all Post with:
      """
      {"filter": {"title": {"lt": "Post One"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3", "p4"]

  Scenario: String field with gt comparison (alphabetical)
    When I list all Post with:
      """
      {"filter": {"title": {"gt": "Post"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2"]

  Scenario: String field with between comparison
    When I list all Post with:
      """
      {"filter": {"title": {"between": ["Another", "Post One"]}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3", "p4", "p1"]

  # Type mismatch tests (comparing string field with number)
  Scenario: Type mismatch - comparing string field with number returns no matches
    When I list all Post with:
      """
      {"filter": {"title": {"lt": 100}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  # Size on number field should return no matches
  Scenario: Size on number field returns no matches
    When I list all Post with:
      """
      {"filter": {"views": {"size": {"eq": 1}}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  # Size with greater than on string
  Scenario: Size on string with gt comparison
    When I list all Post with:
      """
      {"filter": {"title": {"size": {"gt": 10}}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p3", "p4"]

  # Wrong type operands - beginsWith with non-string field value
  Scenario: beginsWith on non-string field returns no matches
    When I list all Post with:
      """
      {"filter": {"views": {"beginsWith": "10"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  # Wrong operand type for beginsWith
  Scenario: beginsWith with non-string operand returns no matches
    When I list all Post with:
      """
      {"filter": {"title": {"beginsWith": 123}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  # List field tests with contains (membership)
  Scenario: contains on a list field checks membership
    When I list all Post with:
      """
      {"filter": {"tags": {"contains": "featured"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p4"]

  Scenario: contains on a list field with non-matching value
    When I list all Post with:
      """
      {"filter": {"tags": {"contains": "nonexistent"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  Scenario: notContains on a list field checks non-membership
    When I list all Post with:
      """
      {"filter": {"tags": {"notContains": "featured"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2", "p3"]

  # Size on list fields
  Scenario: Size on a list field with eq
    When I list all Post with:
      """
      {"filter": {"tags": {"size": {"eq": 2}}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p4"]

  Scenario: Size on a list field with gt
    When I list all Post with:
      """
      {"filter": {"tags": {"size": {"gt": 1}}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p4"]

  Scenario: Size on a list field with lt
    When I list all Post with:
      """
      {"filter": {"tags": {"size": {"lt": 2}}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2", "p3"]

  # Additional edge cases for 100% coverage
  Scenario: contains on list field with non-matching number
    When I list all Post with:
      """
      {"filter": {"tags": {"contains": 123}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  Scenario: notContains on list field with non-matching number
    When I list all Post with:
      """
      {"filter": {"tags": {"notContains": 123}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2", "p3", "p4"]

  Scenario: between with empty array
    When I list all Post with:
      """
      {"filter": {"views": {"between": []}}}
      """
    Then the operation has validation error

  Scenario: between with single value array
    When I list all Post with:
      """
      {"filter": {"views": {"between": [50]}}}
      """
    Then the operation has validation error

  Scenario: attributeExists with string operand (not bool)
    When I list all Post with:
      """
      {"filter": {"rating": {"attributeExists": "yes"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  Scenario: contains with non-string/non-array field (number)
    When I list all Post with:
      """
      {"filter": {"views": {"contains": 100}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  Scenario: notContains with non-string/non-array field (number)
    When I list all Post with:
      """
      {"filter": {"views": {"notContains": 100}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  # Composite sort key scenarios
  Scenario: Index query with composite sort key - beginsWith with status only
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "beginsWith": {"status": "DRAFT"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p3"]

  Scenario: Index query with composite sort key - eq missing required field
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "eq": {"status": "DRAFT"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  Scenario: Index query with composite sort key - validation error unknown operator
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "unknown": {"status": "DRAFT", "createdAt": "2026-09-24T00:00:00.000Z"}}}
      """
    Then the operation has validation error

  Scenario: Index query with composite sort key - validation error non-string field
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "eq": {"status": 123, "createdAt": "2026-09-24T00:00:00.000Z"}}}
      """
    Then the operation has validation error

  Scenario: Index query with between on composite sort key
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "between": [{"status": "ARCHIVED", "createdAt": "2026-09-24T00:00:00.000Z"}, {"status": "PUBLISHED", "createdAt": "2026-09-24T23:59:59.999Z"}]}}
      """
    Then the operation succeeds

  Scenario: Index query with gt on composite sort key with both fields
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "gt": {"status": "DRAFT", "createdAt": "2026-09-24T10:00:00.000Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with lt on composite sort key with partial fields
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "lt": {"status": "PUBLISHED"}}}
      """
    Then the operation succeeds

  Scenario: Index query with le on composite sort key
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "le": {"status": "DRAFT", "createdAt": "2026-09-24T23:59:59.999Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with ge on composite sort key
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "ge": {"status": "PUBLISHED", "createdAt": "2026-09-24T00:00:00.000Z"}}}
      """
    Then the operation succeeds

  # Single sort field validation errors
  Scenario: Index query with between and wrong number of bounds
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"between": ["2026-09-24T00:00:00.000Z"]}}}
      """
    Then the operation has validation error

  Scenario: Index query with between and non-array value
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"between": "value"}}}
      """
    Then the operation has validation error

  Scenario: Index query with beginsWith and non-string value
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"beginsWith": 123}}}
      """
    Then the operation has validation error

  Scenario: Index query with unknown operator on sort field
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"invalidOp": "value"}}}
      """
    Then the operation has validation error

  # Composite validation error cases
  Scenario: Composite sort - validation error between with wrong bound count
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "between": [{"status": "DRAFT"}]}}
      """
    Then the operation has validation error

  Scenario: Composite sort - validation error between with non-object bound
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "between": ["value", "value2"]}}
      """
    Then the operation has validation error

  Scenario: Composite sort - validation error non-string field value
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "beginsWith": {"status": 123}}}
      """
    Then the operation has validation error

  Scenario: Single sort key between
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"between": ["2026-01-01", "2026-12-31"]}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2", "p3", "p4"]

  Scenario: Composite sort key between
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "between": [{"status": "DRAFT", "createdAt": "2026-01-01"}, {"status": "DRAFT", "createdAt": "2026-12-31"}]}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p3"]

  Scenario: List with no filter
    When I list all Post with:
      """
      {}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p2", "p3", "p4"]

  Scenario: Contains with number value on string field
    When I list all Post with:
      """
      {"filter": {"title": {"contains": 5}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  Scenario: NotContains with number value on string field
    When I list all Post with:
      """
      {"filter": {"title": {"notContains": 5}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids []

  Scenario: Composite key with two operators
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "status": {"eq": "DRAFT", "lt": "PUBLISHED"}}}
      """
    Then the operation has validation error

  Scenario: Composite key with empty object
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "status": {}}}
      """
    Then the operation has validation error

  Scenario: Composite key with unknown operator
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "ne": {"status": "DRAFT"}}}
      """
    Then the operation has validation error

  Scenario: List filter with index query semantics
    When I list all Post with:
      """
      {"filter": {"status": {"eq": "DRAFT"}}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p3"]

  Scenario: Index query with filter invalid
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "eq": {"status": "DRAFT"}}, "filter": {"views": {"frobnicate": 1}}}
      """
    Then the operation has validation error

  Scenario: Filter with empty object
    When I list all Post with:
      """
      {"filter": {}}
      """
    Then the operation has validation error

  Scenario: Key condition with unknown sort operator
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "superduper": {"status": "DRAFT"}}}
      """
    Then the operation has validation error

  Scenario: Sort key with multiple operators
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"eq": "2026-01-01", "gt": "2026-01-02"}}}
      """
    Then the operation has validation error

  Scenario: Sort key condition with empty object
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {}}}
      """
    Then the operation has validation error

  Scenario: Sort key condition not an object
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": "not-an-object"}}
      """
    Then the operation has validation error

  Scenario: Size operator with multiple conditions
    When I list all Post with:
      """
      {"filter": {"tags": {"size": {"eq": 2, "gt": 1}}}}
      """
    Then the operation has validation error

  Scenario: Size operator with non-object condition
    When I list all Post with:
      """
      {"filter": {"tags": {"size": "invalid"}}}
      """
    Then the operation has validation error

  Scenario: Field condition with non-object value
    When I list all Post with:
      """
      {"filter": {"status": "DRAFT"}}
      """
    Then the operation has validation error

  Scenario: Filter value that is not an object
    When I list all Post with:
      """
      {"filter": "not-an-object"}
      """
    Then the operation has validation error

  Scenario: Key condition that is not an object
    When I query Post by postsByBlog with:
      """
      {"key": "not-an-object"}
      """
    Then the operation has validation error

  Scenario: Composite between with non-string bound field
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "between": [{"status": "DRAFT", "createdAt": 123}, {"status": "DRAFT", "createdAt": 456}]}}
      """
    Then the operation has validation error

  Scenario: Composite between with empty first bound
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "between": [{}, {"status": "DRAFT", "createdAt": "2026-12-31"}]}}
      """
    Then the operation has validation error


  Scenario: Filter mixing field condition with and operator
    When I list all Post with:
      """
      {"filter": {"views": {"gt": 1}, "and": [{"title": {"beginsWith": "P"}}]}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p2"]

  Scenario: Single sort key with beginsWith
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"beginsWith": "2026-01"}}}
      """
    Then the operation succeeds

  Scenario: Composite between with only leading field bounds
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "between": [{"status": "DRAFT"}, {"status": "PUBLISHED"}]}}
      """
    Then the operation succeeds
    Then the result data contains exactly these ids ["p1", "p3"]

  # Validation error rows for uncovered lines
  Scenario Outline: Invalid key conditions
    When I query <model> by <index> with:
      """
      <request>
      """
    Then the operation has validation error

    Examples:
      | model | index                | request |
      | Post  | postsByBlog          | {"key": {}} |
      | Post  | postsByBlog          | {"key": {"blogId": "b1", "createdAt": {}}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "status": 5}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "between": ["a", "b"]}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "between": {"status": "DRAFT"}}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "eq": "DRAFT"}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "eq": {}}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "gt": {}}} |
      | Post  | postsByBlog          | {"key": {"blogId": "b1", "createdAt": {"ne": "x"}}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "between": [{"status": "A"}, {"status": 5}]}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "between": [{"status": "A"}, "b"]}} |
      | Post  | postsByBlogStatus    | {"key": {"blogId": "b1", "ne": {"status": "DRAFT"}}} |

  Scenario: Query index with no sort fields
    When I query Tag by tagsByName with:
      """
      {"key": {"name": "featured"}}
      """
    Then the operation succeeds

  Scenario: Between with single-element array
    When I list all Post with:
      """
      {"filter": {"views": {"between": [1]}}}
      """
    Then the operation has validation error

  Scenario: Index query without key argument
    When I query Post by postsByBlog with:
      """
      {}
      """
    Then the operation has validation error

  Scenario: Index query with missing partition field in key
    When I query Post by postsByBlog with:
      """
      {"key": {"createdAt": "2026-01-01"}}
      """
    Then the operation has validation error

  Scenario: Filter with and not being an array
    When I list all Post with:
      """
      {"filter": {"and": "not-an-array"}}
      """
    Then the operation has validation error

  Scenario: Filter with or not being an array
    When I list all Post with:
      """
      {"filter": {"or": "not-an-array"}}
      """
    Then the operation has validation error

  Scenario: Filter with between not exactly 2 elements
    When I list all Post with:
      """
      {"filter": {"views": {"between": [1, 2, 3]}}}
      """
    Then the operation has validation error

  Scenario: Filter with between having non-array value
    When I list all Post with:
      """
      {"filter": {"views": {"between": 5}}}
      """
    Then the operation has validation error

  Scenario: Composite key with two operator keys
    When I query Post by postsByBlogStatus with:
      """
      {"key": {"blogId": "b1", "ne": {"status": "DRAFT"}, "eq": {"status": "PUBLISHED"}}}
      """
    Then the operation has validation error
