@rust-only
Feature: Query and Mutation Resolvers
  Query and Mutation resolvers should bind to the Virtuus storage engine and return records.

  Scenario: getPost resolver returns a post record
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p1","title":"Hello World","blogId":"b1","content":"{}","createdAt":"2026-09-24T00:00:00Z","updatedAt":"2026-09-24T00:00:00Z"}
      """
    When I send the GraphQL request:
      """
      query { getPost(id: "p1") { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"id":"p1","title":"Hello World"}}}
      """

  Scenario: createPost mutation creates a post record
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p2", title: "New Post", content: "{}", blogId: "b1"}) { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"createPost":{"id":"p2","title":"New Post"}}}
      """

  Scenario: updatePost mutation updates a post record
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p3","title":"Old Title","blogId":"b1","content":"{}","createdAt":"2026-09-24T00:00:00Z","updatedAt":"2026-09-24T00:00:00Z"}
      """
    When I send the GraphQL request:
      """
      mutation { updatePost(input: {id: "p3", title: "New Title"}) { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"updatePost":{"id":"p3","title":"New Title"}}}
      """

  Scenario: deletePost mutation deletes a post record
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p4","title":"To Delete","blogId":"b1","content":"{}","createdAt":"2026-09-24T00:00:00Z","updatedAt":"2026-09-24T00:00:00Z"}
      """
    When I send the GraphQL request:
      """
      mutation { deletePost(input: {id: "p4"}) { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"deletePost":{"id":"p4","title":"To Delete"}}}
      """

  Scenario: getPost resolver returns null for missing record
    Given a blog engine
    When I send the GraphQL request:
      """
      query { getPost(id: "missing") { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":null}}
      """

  @wip
  Scenario: listPosts query returns an array of posts
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p5","title":"Post 1","blogId":"b1"}
      """
    And a Post exists with:
      """
      {"id":"p6","title":"Post 2","blogId":"b1"}
      """
    When I send the GraphQL request:
      """
      query { listPosts { items { id title } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[{"id":"p5","title":"Post 1"},{"id":"p6","title":"Post 2"}]}}}
      """

  @wip
  Scenario: listPosts query with filter returns filtered posts
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p7","title":"Post A","blogId":"b1"}
      """
    And a Post exists with:
      """
      {"id":"p8","title":"Post B","blogId":"b2"}
      """
    When I send the GraphQL request:
      """
      query { listPosts(filter: {blogId: {eq: "b1"}}) { items { id title } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[{"id":"p7","title":"Post A"}]}}}
      """

  @wip
  Scenario: postsByBlog index query with sortDirection
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p9","title":"Recent","blogId":"b1"}
      """
    And a Post exists with:
      """
      {"id":"p10","title":"Older","blogId":"b1"}
      """
    When I send the GraphQL request:
      """
      query { postsByBlog(blogId: "b1", sortDirection: DESC) { items { id title } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlog":{"items":[{"id":"p10","title":"Older"},{"id":"p9","title":"Recent"}]}}}
      """

  @wip
  Scenario: listPosts query with pagination returns paginated results
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p11","title":"Page 1","blogId":"b1"}
      """
    And a Post exists with:
      """
      {"id":"p12","title":"Page 2","blogId":"b1"}
      """
    When I send the GraphQL request:
      """
      query { listPosts(limit: 1) { items { id } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[{"id":"p11"}],"nextToken":null}}}
      """

  Scenario: Request without API-key returns 401 Unauthorized
    Given a blog engine with API-key auth
    When I send a request without the x-api-key header to:
      """
      query { getPost(id: "p1") { id } }
      """
    Then the HTTP status is 401
    And the response contains UnauthorizedException

  Scenario: Request with wrong API-key returns 401 Unauthorized
    Given a blog engine with API-key auth
    When I send a request with wrong x-api-key header to:
      """
      query { getPost(id: "p1") { id } }
      """
    Then the HTTP status is 401
    And the response contains UnauthorizedException

  Scenario: Request with correct API-key succeeds
    Given a blog engine with API-key auth
    And a Post exists with:
      """
      {"id":"p20","title":"Auth Test","blogId":"b1","content":"{}","createdAt":"2026-09-24T00:00:00Z","updatedAt":"2026-09-24T00:00:00Z"}
      """
    When I send a request with correct x-api-key header to:
      """
      query { getPost(id: "p20") { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"id":"p20","title":"Auth Test"}}}
      """

  Scenario: Duplicate create returns ConditionalCheckFailedException
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p21","title":"Unique Post","blogId":"b1","content":"{}","createdAt":"2026-09-24T00:00:00Z","updatedAt":"2026-09-24T00:00:00Z"}
      """
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p21", title: "Duplicate", content: "{}", blogId: "b1"}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"An item with this id already exists","errorType":"DynamoDB:ConditionalCheckFailedException","locations":[{"line":2,"column":12}]}]}
      """

  Scenario: Update of missing record returns ConditionalCheckFailedException
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { updatePost(input: {id: "missing", title: "No Post"}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"An item with this id does not exist","errorType":"DynamoDB:ConditionalCheckFailedException","locations":[{"line":2,"column":12}]}]}
      """

  @wip
  Scenario: Create with invalid enum returns ValidationException
    Given a blog engine with enum constraints
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p22", title: "Invalid Status", content: "{}", blogId: "b1"}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Invalid enum value","errorType":"ValidationException","locations":[{"line":2,"column":12}]}]}
      """
