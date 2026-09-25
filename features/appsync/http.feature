@rust-only
Feature: The GraphQL HTTP endpoint
  POST /graphql takes {query, variables?, operationName?}; a body that is not a
  GraphQL request gets 400 MalformedHttpRequestException.

  Scenario: Variables are applied
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p1","title":"Hello","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request with variables {"id":"p1"}:
      """
      query GetPost($id: ID!) { getPost(id: $id) { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"id":"p1","title":"Hello"}}}
      """

  Scenario: operationName selects the operation
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p1","title":"Hello","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request with operation "Second":
      """
      query First { getPost(id: "p1") { id } } query Second { getPost(id: "p1") { title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"title":"Hello"}}}
      """

  Scenario: A GraphQL syntax error has no errorType
    Given a blog engine
    When I send the GraphQL request:
      """
      query { getPost(id: "p1") { id }
      """
    Then the HTTP status is 200
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":" --> 3:1\n  |\n3 | \n  | ^---\n  |\n  = expected selection","locations":[{"line":3,"column":1}]}]}
      """

  Scenario: A body that is not JSON gets 400
    Given a blog engine
    When I post this raw body:
      """
      not json
      """
    Then the HTTP status is 400
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"expected ident at line 2 column 2","errorType":"MalformedHttpRequestException"}]}
      """

  Scenario: A body without a query gets 400
    Given a blog engine
    When I post this raw body:
      """
      {"variables":{}}
      """
    Then the HTTP status is 400
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"The request has no query","errorType":"MalformedHttpRequestException"}]}
      """
