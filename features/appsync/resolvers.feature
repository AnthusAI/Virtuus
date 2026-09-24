@rust-only
Feature: Query and Mutation resolvers
  Root Query and Mutation fields bind to the Virtuus engine through the contract:
  get/create/update/delete per model, list fields returning Model<X>Connection,
  and one field per index queryField.

  Scenario: getPost returns a post
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p1","title":"Hello World","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      query { getPost(id: "p1") { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"id":"p1","title":"Hello World"}}}
      """

  Scenario: getPost returns null for a missing post
    Given a blog engine
    When I send the GraphQL request:
      """
      query { getPost(id: "missing") { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":null}}
      """

  Scenario: createPost creates a post
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p2", title: "New Post", content: "{}", blogId: "b1", status: DRAFT}) { id title status } }
      """
    Then the GraphQL response is:
      """
      {"data":{"createPost":{"id":"p2","title":"New Post","status":"DRAFT"}}}
      """
    When I send the GraphQL request:
      """
      query { getPost(id: "p2") { title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"title":"New Post"}}}
      """

  Scenario: updatePost updates a post
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p3","title":"Old Title","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      mutation { updatePost(input: {id: "p3", title: "New Title"}) { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"updatePost":{"id":"p3","title":"New Title"}}}
      """

  Scenario: deletePost deletes a post and returns it
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p4","title":"To Delete","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      mutation { deletePost(input: {id: "p4"}) { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"deletePost":{"id":"p4","title":"To Delete"}}}
      """
    When I send the GraphQL request:
      """
      query { getPost(id: "p4") { id } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":null}}
      """

  Scenario: createBlog creates a blog
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { createBlog(input: {id: "b2", title: "My Blog"}) { id title } }
      """
    Then the GraphQL response is:
      """
      {"data":{"createBlog":{"id":"b2","title":"My Blog"}}}
      """

  Scenario: Creating a duplicate post returns ConditionalCheckFailedException
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p21","title":"Unique Post","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p21", title: "Duplicate", content: "{}", blogId: "b1"}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":{"createPost":null},"errors":[{"message":"An item with this id already exists","errorType":"DynamoDB:ConditionalCheckFailedException","path":["createPost"],"locations":[{"line":2,"column":12}]}]}
      """

  Scenario: Updating a missing post returns ConditionalCheckFailedException
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { updatePost(input: {id: "missing", title: "No Post"}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":{"updatePost":null},"errors":[{"message":"An item with this id does not exist","errorType":"DynamoDB:ConditionalCheckFailedException","path":["updatePost"],"locations":[{"line":2,"column":12}]}]}
      """

  Scenario: A mutation condition is rejected, not ignored
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p5", title: "T", content: "{}", blogId: "b1"}, condition: {title: {eq: "T"}}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":{"createPost":null},"errors":[{"message":"condition expressions are not supported","errorType":"BadRequest","path":["createPost"],"locations":[{"line":2,"column":12}]}]}
      """

  Scenario: A null mutation condition is the same as none
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p5", title: "T", content: "{}", blogId: "b1"}, condition: null) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":{"createPost":{"id":"p5"}}}
      """

  Scenario: A missing required input field is a GraphQL validation error without errorType
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p22", content: "{}", blogId: "b1"}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Invalid value for argument \"input\", field \"title\" of type \"String!\" is required but not provided","locations":[{"line":2,"column":23}]}]}
      """

  Scenario: An unknown enum value is a GraphQL validation error without errorType
    Given a blog engine
    When I send the GraphQL request:
      """
      mutation { createPost(input: {id: "p22", title: "T", content: "{}", blogId: "b1", status: LOST}) { id } }
      """
    Then the GraphQL response is:
      """
      {"data":null,"errors":[{"message":"Invalid value for argument \"input.status\", enumeration type \"PostStatus\" does not contain the value \"LOST\"","locations":[{"line":2,"column":23}]}]}
      """

  Scenario: listPosts returns every post
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p5","title":"Post 1","blogId":"b1","content":"{}"}
      """
    And a Post exists with:
      """
      {"id":"p6","title":"Post 2","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      query { listPosts { items { id title } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[{"id":"p5","title":"Post 1"},{"id":"p6","title":"Post 2"}],"nextToken":null}}}
      """

  Scenario: listPosts with a filter
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p7","title":"Post A","blogId":"b1","content":"{}"}
      """
    And a Post exists with:
      """
      {"id":"p8","title":"Post B","blogId":"b2","content":"{}"}
      """
    When I send the GraphQL request:
      """
      query { listPosts(filter: {blogId: {eq: "b1"}}) { items { id title } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[{"id":"p7","title":"Post A"}],"nextToken":null}}}
      """

  Scenario: listPosts with an invalid filter returns ValidationException
    Given a blog engine
    When I send the GraphQL request:
      """
      query { listPosts(filter: {title: {between: ["a"]}}) { items { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":null},"errors":[{"message":"between requires a 2-element array","errorType":"ValidationException","path":["listPosts"],"locations":[{"line":2,"column":9}]}]}
      """

  Scenario: listPosts pages with limit and nextToken
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p11","title":"Page 1","blogId":"b1","content":"{}"}
      """
    And a Post exists with:
      """
      {"id":"p12","title":"Page 2","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      query { listPosts(limit: 1) { items { id } nextToken } }
      """
    Then the GraphQL response, with the nextToken at "data.listPosts.nextToken", is:
      """
      {"data":{"listPosts":{"items":[{"id":"p11"}],"nextToken":"$nextToken"}}}
      """
    When I send the GraphQL request using the previous nextToken:
      """
      query { listPosts(limit: 1, nextToken: "$nextToken") { items { id } nextToken } }
      """
    Then the GraphQL response, with the nextToken at "data.listPosts.nextToken", is:
      """
      {"data":{"listPosts":{"items":[{"id":"p12"}],"nextToken":"$nextToken"}}}
      """
    When I send the GraphQL request using the previous nextToken:
      """
      query { listPosts(limit: 1, nextToken: "$nextToken") { items { id } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[],"nextToken":null}}}
      """

  Scenario: A null nextToken starts from the first page
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p11","title":"Page 1","blogId":"b1","content":"{}"}
      """
    When I send the GraphQL request:
      """
      query { listPosts(limit: 5, nextToken: null, filter: null) { items { id } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":{"items":[{"id":"p11"}],"nextToken":null}}}
      """

  Scenario: A garbage nextToken returns an error
    Given a blog engine
    When I send the GraphQL request:
      """
      query { listPosts(nextToken: "not-a-token") { items { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":null},"errors":[{"message":"Invalid token encoding","errorType":"ValidationException","path":["listPosts"],"locations":[{"line":2,"column":9}]}]}
      """

  Scenario: A negative limit returns BadRequest
    Given a blog engine
    When I send the GraphQL request:
      """
      query { listPosts(limit: -1) { items { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"listPosts":null},"errors":[{"message":"limit must be non-negative","errorType":"BadRequest","path":["listPosts"],"locations":[{"line":2,"column":9}]}]}
      """

  Scenario: postsByBlog sorts by the index sort key, descending
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p9","title":"Recent","blogId":"b1","content":"{}","createdAt":"2026-09-24T10:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p10","title":"Older","blogId":"b1","content":"{}","createdAt":"2026-09-24T09:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p99","title":"Other blog","blogId":"b2","content":"{}","createdAt":"2026-09-24T11:00:00Z"}
      """
    When I send the GraphQL request:
      """
      query { postsByBlog(blogId: "b1", sortDirection: DESC) { items { id title } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlog":{"items":[{"id":"p9","title":"Recent"},{"id":"p10","title":"Older"}],"nextToken":null}}}
      """

  Scenario: postsByBlog with a sort key condition
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p15","title":"Early","blogId":"b1","content":"{}","createdAt":"2026-09-24T08:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p16","title":"Late","blogId":"b1","content":"{}","createdAt":"2026-09-24T12:00:00Z"}
      """
    When I send the GraphQL request:
      """
      query { postsByBlog(blogId: "b1", createdAt: {gt: "2026-09-24T09:30:00Z"}) { items { id } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlog":{"items":[{"id":"p16"}],"nextToken":null}}}
      """

  Scenario: postsByBlog with a filter, and paging with nextToken
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p_f1","title":"F1","blogId":"b1","content":"{}","status":"PUBLISHED","createdAt":"2026-09-24T08:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p_f2","title":"F2","blogId":"b1","content":"{}","status":"DRAFT","createdAt":"2026-09-24T09:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p_f3","title":"F3","blogId":"b1","content":"{}","status":"PUBLISHED","createdAt":"2026-09-24T10:00:00Z"}
      """
    When I send the GraphQL request:
      """
      query { postsByBlog(blogId: "b1", filter: {status: {eq: "PUBLISHED"}}) { items { id } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlog":{"items":[{"id":"p_f1"},{"id":"p_f3"}],"nextToken":null}}}
      """
    When I send the GraphQL request:
      """
      query { postsByBlog(blogId: "b1", sortDirection: DESC, limit: 2) { items { id } nextToken } }
      """
    Then the GraphQL response, with the nextToken at "data.postsByBlog.nextToken", is:
      """
      {"data":{"postsByBlog":{"items":[{"id":"p_f3"},{"id":"p_f2"}],"nextToken":"$nextToken"}}}
      """
    When I send the GraphQL request using the previous nextToken:
      """
      query { postsByBlog(blogId: "b1", sortDirection: DESC, limit: 2, nextToken: "$nextToken") { items { id } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlog":{"items":[{"id":"p_f1"}],"nextToken":null}}}
      """

  Scenario: An index query with a negative limit returns BadRequest
    Given a blog engine
    When I send the GraphQL request:
      """
      query { postsByBlog(blogId: "b1", limit: -1) { items { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlog":null},"errors":[{"message":"limit must be non-negative","errorType":"BadRequest","path":["postsByBlog"],"locations":[{"line":2,"column":9}]}]}
      """

  Scenario: postsByBlogStatus composite key with beginsWith
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p17","title":"Draft","blogId":"b1","status":"DRAFT","content":"{}","createdAt":"2026-09-24T08:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p18","title":"Published","blogId":"b1","status":"PUBLISHED","content":"{}","createdAt":"2026-09-24T09:00:00Z"}
      """
    When I send the GraphQL request:
      """
      query { postsByBlogStatus(blogId: "b1", statusCreatedAt: {beginsWith: {status: "PUBLISHED"}}) { items { id title } nextToken } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlogStatus":{"items":[{"id":"p18","title":"Published"}],"nextToken":null}}}
      """

  Scenario: postsByBlogStatus composite key with between
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p19","title":"Early","blogId":"b1","status":"PUBLISHED","content":"{}","createdAt":"2026-09-24T08:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p20","title":"Late","blogId":"b1","status":"PUBLISHED","content":"{}","createdAt":"2026-09-24T10:00:00Z"}
      """
    When I send the GraphQL request:
      """
      query { postsByBlogStatus(blogId: "b1", statusCreatedAt: {between: [{status: "PUBLISHED", createdAt: "2026-09-24T07:00:00Z"}, {status: "PUBLISHED", createdAt: "2026-09-24T09:00:00Z"}]}) { items { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlogStatus":{"items":[{"id":"p19"}]}}}
      """

  Scenario: postsByBlogStatus composite key with gt
    Given a blog engine
    And a Post exists with:
      """
      {"id":"p21","title":"Early","blogId":"b1","status":"PUBLISHED","content":"{}","createdAt":"2026-09-24T08:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p22","title":"Late","blogId":"b1","status":"PUBLISHED","content":"{}","createdAt":"2026-09-24T10:00:00Z"}
      """
    When I send the GraphQL request:
      """
      query { postsByBlogStatus(blogId: "b1", statusCreatedAt: {gt: {status: "PUBLISHED", createdAt: "2026-09-24T09:00:00Z"}}) { items { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"postsByBlogStatus":{"items":[{"id":"p22"}]}}}
      """

  Scenario: commentsByPost, an index without a sort key
    Given a blog engine
    And a Comment exists with:
      """
      {"id":"c1","postId":"p29","content":"Great post"}
      """
    And a Comment exists with:
      """
      {"id":"c2","postId":"p30","content":"Elsewhere"}
      """
    When I send the GraphQL request:
      """
      query { commentsByPost(postId: "p29") { items { id content } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"commentsByPost":{"items":[{"id":"c1","content":"Great post"}]}}}
      """
