@rust-only
Feature: Relationship fields
  hasMany fields query the target model's child index with the parent's key;
  belongsTo fields get the target record by the foreign key.

  Background:
    Given a blog engine
    And a Blog exists with:
      """
      {"id":"b1","title":"Blog One"}
      """
    And a Post exists with:
      """
      {"id":"p1","title":"Hello","blogId":"b1","content":"{}","createdAt":"2026-09-24T08:00:00Z"}
      """
    And a Post exists with:
      """
      {"id":"p2","title":"Later","blogId":"b1","content":"{}","createdAt":"2026-09-24T09:00:00Z"}
      """
    And a Comment exists with:
      """
      {"id":"c1","postId":"p1","content":"First"}
      """
    And a Comment exists with:
      """
      {"id":"c2","postId":"p1","content":"Second"}
      """
    And a Comment exists with:
      """
      {"id":"c3","postId":"p1","content":"Third"}
      """
    And a Comment exists with:
      """
      {"id":"c9","postId":"p2","content":"On another post"}
      """

  Scenario: Post.comments returns the post's comments
    When I send the GraphQL request:
      """
      query { getPost(id: "p1") { id comments { items { id content } nextToken } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"id":"p1","comments":{"items":[{"id":"c1","content":"First"},{"id":"c2","content":"Second"},{"id":"c3","content":"Third"}],"nextToken":null}}}}
      """

  Scenario: Post.comments pages with limit and nextToken
    When I send the GraphQL request:
      """
      query { getPost(id: "p1") { comments(limit: 2) { items { id } nextToken } } }
      """
    Then the GraphQL response, with the nextToken at "data.getPost.comments.nextToken", is:
      """
      {"data":{"getPost":{"comments":{"items":[{"id":"c1"},{"id":"c2"}],"nextToken":"$nextToken"}}}}
      """
    When I send the GraphQL request using the previous nextToken:
      """
      query { getPost(id: "p1") { comments(limit: 2, nextToken: "$nextToken") { items { id } nextToken } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"comments":{"items":[{"id":"c3"}],"nextToken":null}}}}
      """

  Scenario: Post.comments with a filter
    When I send the GraphQL request:
      """
      query { getPost(id: "p1") { comments(filter: {content: {beginsWith: "S"}}) { items { id } } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"comments":{"items":[{"id":"c2"}]}}}}
      """

  Scenario: Blog.posts sorted by the child index sort key, descending
    When I send the GraphQL request:
      """
      query { getBlog(id: "b1") { title posts(sortDirection: DESC) { items { id } } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getBlog":{"title":"Blog One","posts":{"items":[{"id":"p2"},{"id":"p1"}]}}}}
      """

  Scenario: A relationship field with a negative limit returns BadRequest with its path
    When I send the GraphQL request:
      """
      query { getPost(id: "p1") { id comments(limit: -1) { items { id } } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getPost":{"id":"p1","comments":null}},"errors":[{"message":"limit must be non-negative","errorType":"BadRequest","path":["getPost","comments"],"locations":[{"line":2,"column":32}]}]}
      """

  Scenario: Comment.post returns the parent post
    When I send the GraphQL request:
      """
      query { getComment(id: "c1") { id post { id title blog { title } } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getComment":{"id":"c1","post":{"id":"p1","title":"Hello","blog":{"title":"Blog One"}}}}}
      """

  Scenario: A dangling foreign key resolves to null
    Given a Comment exists with:
      """
      {"id":"c4","postId":"missing-post","content":"Orphan"}
      """
    When I send the GraphQL request:
      """
      query { getComment(id: "c4") { id post { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getComment":{"id":"c4","post":null}}}
      """

  Scenario: An absent foreign key resolves to null
    Given a Comment exists with:
      """
      {"id":"c5","content":"No post"}
      """
    When I send the GraphQL request:
      """
      query { getComment(id: "c5") { id post { id } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getComment":{"id":"c5","post":null}}}
      """

  Scenario: Relationships nest
    When I send the GraphQL request:
      """
      query { getBlog(id: "b1") { posts { items { id comments(limit: 1) { items { id post { title } } } } } } }
      """
    Then the GraphQL response is:
      """
      {"data":{"getBlog":{"posts":{"items":[{"id":"p1","comments":{"items":[{"id":"c1","post":{"title":"Hello"}}]}},{"id":"p2","comments":{"items":[{"id":"c9","post":{"title":"Later"}}]}}]}}}}
      """
