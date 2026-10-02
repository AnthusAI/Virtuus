@rust-only
Feature: Request identity and authorization
  A configured API key must match x-api-key. With test identities enabled,
  x-apricity-identity ({sub, username, groups}) makes the request act as that
  user; malformed identities are rejected with 401.

  Scenario: A request without the API key gets 401
    Given a blog engine with API-key auth
    When I send a request without the x-api-key header to:
      """
      query { getPost(id: "p1") { id } }
      """
    Then the HTTP status is 401
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Unauthorized","errorType":"UnauthorizedException"}]}
      """

  Scenario: A request with the wrong API key gets 401
    Given a blog engine with API-key auth
    When I send a request with x-api-key "wrong-key" to:
      """
      query { getPost(id: "p1") { id } }
      """
    Then the HTTP status is 401
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Unauthorized","errorType":"UnauthorizedException"}]}
      """

  Scenario: A request with the right API key succeeds
    Given a blog engine with API-key auth
    And a Post exists with:
      """
      {"id":"p20","title":"Auth Test","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      query { getPost(id: "p20") { id title } }
      """
    Then the HTTP status is 200
    And the GraphQL response is:
      """
      {"data":{"getPost":{"id":"p20","title":"Auth Test"}}}
      """

  Scenario: Without a configured key, any x-api-key is accepted
    Given a blog engine
    When I send a request with x-api-key "any-key" to:
      """
      query { listPosts { items { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[]}}}
      """

  Scenario: The identity header is rejected unless test identities are enabled
    Given a blog engine
    When I send a request with x-apricity-identity {"sub":"u1","username":"alice"} to:
      """
      query { listPosts { items { id } } }
      """
    Then the HTTP status is 401
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Test identity headers not enabled","errorType":"UnauthorizedException"}]}
      """

  Scenario Outline: A malformed identity header gets 401
    Given a blog engine with test identities
    When I send a request with x-apricity-identity <identity> to:
      """
      query { listPosts { items { id } } }
      """
    Then the HTTP status is 401
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"<message>","errorType":"UnauthorizedException"}]}
      """

    Examples:
      | identity                          | message                                             |
      | {"username":"alice"}              | Identity header missing required 'sub' field        |
      | {"sub":"u1"}                      | Identity header missing required 'username' field   |
      | {"sub":"u1","username":7}         | Identity header missing required 'username' field   |
      | "just a string"                   | Identity header must be a JSON object               |
      | {bad json}                        | Invalid identity header JSON                        |

  Scenario: A non-UTF-8 identity header gets 401
    Given a blog engine with test identities
    When I send a request with a non-UTF-8 x-apricity-identity to:
      """
      query { listPosts { items { id } } }
      """
    Then the HTTP status is 401
    And the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Invalid identity header encoding","errorType":"UnauthorizedException"}]}
      """

  Scenario: Owner rules follow the identity header
    Given a blog engine enforcing authorization, with test identities
    And user "alice" has created a Comment with:
      """
      {"id":"c1","postId":"p1","content":"Mine"}
      """
    When I send a request with x-apricity-identity {"sub":"alice","username":"alice","groups":["writers"]} to:
      """
      query { getComment(id: "c1") { id content } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getComment":{"id":"c1","content":"Mine"}}}
      """
    When I send a request with x-apricity-identity {"sub":"bob","username":"bob"} to:
      """
      query { getComment(id: "c1") { id content } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getComment":null},"errors":[{"message":"Not Authorized to access read on type Comment","errorType":"Unauthorized","path":["getComment"],"locations":[{"line":2,"column":9}]}]}
      """

  Scenario: An API-key caller cannot write an owner-protected model
    Given a blog engine enforcing authorization, with test identities
    When I send the GraphQL request:
      """
      mutation { createComment(input: {id: "c2", postId: "p1", content: "Anonymous"}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":{"createComment":null},"errors":[{"message":"Not Authorized to access create on type Comment","errorType":"Unauthorized","path":["createComment"],"locations":[{"line":2,"column":12}]}]}
      """
