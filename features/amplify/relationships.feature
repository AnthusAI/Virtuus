@rust-only
Feature: selectionSet and relationships (belongsTo, hasMany)
  Relationships allow resolving related data through foreign keys (belongsTo) or indexes (hasMany).
  selectionSet filters which fields are returned; without it, all scalar fields and no relationships are included.
  hasMany returns a connection {items, nextToken} supporting pagination.

  Background:
    Given a blog contract
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Tech Blog"}
      """
    Given these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Rust Tips", "status": "PUBLISHED", "createdAt": "2026-01-15T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Async Await", "status": "PUBLISHED", "createdAt": "2026-01-20T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Type System", "status": "DRAFT", "createdAt": "2026-02-01T10:00:00Z"}
      ]
      """
    Given these Comment records exist:
      """
      [
        {"id": "c1", "postId": "p1", "content": "Great tips!", "status": "PUBLISHED", "createdAt": "2026-01-15T11:00:00Z"},
        {"id": "c2", "postId": "p1", "content": "Very helpful", "status": "PUBLISHED", "createdAt": "2026-01-15T12:00:00Z"},
        {"id": "c2b", "postId": "p1", "content": "Draft comment", "status": "DRAFT", "createdAt": "2026-01-15T13:00:00Z"},
        {"id": "c3", "postId": "p2", "content": "Excellent!", "status": "PUBLISHED", "createdAt": "2026-01-20T11:00:00Z"},
        {"id": "c4", "postId": "p3", "content": "Looking forward", "status": "DRAFT", "createdAt": "2026-02-01T11:00:00Z"}
      ]
      """
    Given these Tag records exist:
      """
      [
        {"postId": "p1", "name": "rust"},
        {"postId": "p1", "name": "tips"}
      ]
      """

  Scenario: Post with comments in index order via hasMany
    When I get the Post with id "p1" and selectionSet:
      """
      ["id", "title", "comments.*"]
      """
    Then the operation succeeds
    Then the result has:
      """
      {"id": "p1", "title": "Rust Tips"}
      """
    Then the result.comments has field "items"
    Then the result.comments.items has exactly 3 records with ids ["c1", "c2", "c2b"]
    Then the result.comments.nextToken is null

  Scenario: Comment with belongsTo post via nested selection
    When I get the Comment with id "c1" and selectionSet:
      """
      ["id", "post.title"]
      """
    Then the operation succeeds
    Then the result has:
      """
      {"id": "c1", "post": {"title": "Rust Tips"}}
      """

  Scenario: Comment whose post doesn't exist returns null for relationship
    Given these Comment records exist:
      """
      [
        {"id": "c_orphan", "postId": "p_missing", "content": "Orphaned comment", "status": "PUBLISHED", "createdAt": "2026-02-01T11:00:00Z"}
      ]
      """
    When I get the Comment with id "c_orphan" and selectionSet:
      """
      ["id", "postId", "post.id"]
      """
    Then the operation succeeds
    Then the result has:
      """
      {"id": "c_orphan", "postId": "p_missing", "post": null}
      """

  Scenario: hasMany with pagination limit and nextToken
    When I get the Post with id "p1" and selectionSet and relationshipArgs:
      """
      {"selectionSet": ["id", "comments.*"], "relationshipArgs": {"comments": {"limit": 2}}}
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 2 records
    Then the result.comments.nextToken is not null
    And I store the nextToken for comments

    When I get the Post with id "p1" and selectionSet and relationshipArgs with stored nextToken:
      """
      {"selectionSet": ["id", "comments.*"], "relationshipArgs": {"comments": {"limit": 2}}}
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 1 record
    Then the result.comments.nextToken is null

  Scenario: selectionSet trims fields to selected scalars only
    When I get the Post with id "p1" and selectionSet:
      """
      ["id", "title"]
      """
    Then the operation succeeds
    Then the result has:
      """
      {"id": "p1", "title": "Rust Tips"}
      """
    Then the result does not have field "status"

  Scenario: no selectionSet returns all scalars and no relationships
    When I get the Post with id "p1"
    Then the operation succeeds
    Then the result has field "id"
    Then the result has field "title"
    Then the result has field "status"
    Then the result does not have field "comments"

  Scenario: hasMany with filter and limit on relationship
    When I get the Post with id "p1" and selectionSet and relationshipArgs:
      """
      {"selectionSet": ["id", "comments.*"], "relationshipArgs": {"comments": {"filter": {"status": {"eq": "PUBLISHED"}}, "limit": 1}}}
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 1 record
    Then the result.comments.nextToken is not null

  Scenario: hasMany filter skips non-matching records
    When I get the Post with id "p1" and selectionSet and relationshipArgs:
      """
      {"selectionSet": ["id", "comments.*"], "relationshipArgs": {"comments": {"filter": {"status": {"eq": "DRAFT"}}, "limit": 10}}}
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 1 record
    Then the result.comments.nextToken is null

  Scenario: hasMany with DESC sort direction
    When I get the Post with id "p1" and selectionSet and relationshipArgs:
      """
      {"selectionSet": ["id", "comments.*"], "relationshipArgs": {"comments": {"sortDirection": "DESC"}}}
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 3 records
    Then the result.comments.nextToken is null

  Scenario: selectionSet with wildcard returns all scalars
    When I get the Post with id "p1" and selectionSet:
      """
      ["*"]
      """
    Then the operation succeeds
    Then the result has field "id"
    Then the result has field "title"
    Then the result has field "status"

  Scenario: belongsTo with wildcard returns full related object
    When I get the Comment with id "c1" and selectionSet:
      """
      ["id", "post.*"]
      """
    Then the operation succeeds
    Then the result has field "id"
    Then the result has field "post"

  Scenario: belongsTo with missing foreign key returns null
    Given these Comment records exist:
      """
      [
        {"id": "c_orphan2", "postId": "p_missing", "content": "No post comment", "status": "PUBLISHED", "createdAt": "2026-02-01T11:00:00Z"}
      ]
      """
    When I get the Comment with id "c_orphan2" and selectionSet:
      """
      ["id", "post.id"]
      """
    Then the operation succeeds
    Then the result has:
      """
      {"id": "c_orphan2", "post": null}
      """

  Scenario: hasMany DESC with pagination across pages
    When I get the Post with id "p1" and selectionSet and relationshipArgs:
      """
      {"selectionSet": ["id", "comments.*"], "relationshipArgs": {"comments": {"sortDirection": "DESC", "limit": 1}}}
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 1 record
    Then the result.comments.nextToken is not null
    And I store the nextToken for comments

    When I get the Post with id "p1" and selectionSet and relationshipArgs with stored nextToken:
      """
      {"selectionSet": ["id", "comments.*"], "relationshipArgs": {"comments": {"sortDirection": "DESC", "limit": 1}}}
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 1 record

  Scenario: list operation with selectionSet trims fields
    When I list all Post with:
      """
      {"selectionSet": ["id", "title"], "limit": 1}
      """
    Then the operation succeeds
    Then the result data has exactly 1 record with id "p1"

  Scenario: index query with selectionSet filters fields
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "selectionSet": ["id", "title"], "limit": 1}
      """
    Then the operation succeeds
    Then the result data has exactly 1 record with id "p1"

  Scenario: implicit hasMany index for tags
    When I get the Post with id "p1" and selectionSet:
      """
      ["id", "tags.*"]
      """
    Then the operation succeeds
    Then the result has field "id"
    Then the result has field "tags"

  Scenario: belongsTo returns null when foreign key is absent
    Given these Comment records exist:
      """
      [
        {"id": "c_no_post", "content": "Comment without postId", "status": "PUBLISHED", "createdAt": "2026-02-02T11:00:00Z"}
      ]
      """
    When I get the Comment with id "c_no_post" and selectionSet:
      """
      ["id", "post.title"]
      """
    Then the operation succeeds
    Then the result has:
      """
      {"id": "c_no_post", "post": null}
      """

  Scenario: sparse GSI skips records missing index key
    Given these Comment records exist:
      """
      [
        {"id": "c_unindexed", "content": "Comment without postId", "status": "PUBLISHED", "createdAt": "2026-02-03T11:00:00Z"}
      ]
      """
    When I query Comment by commentsByPost with:
      """
      {"key": {"postId": "p1"}}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["c1", "c2", "c2b"]

  Scenario: create with explicit createdAt preserves the value
    When I create a Post with:
      """
      {"id": "p_explicit", "blogId": "b1", "title": "Backdated Post", "createdAt": "2025-01-01T10:00:00Z"}
      """
    Then the operation succeeds
    Then the result.createdAt is "2025-01-01T10:00:00Z"

  Scenario: create without createdAt generates timestamp
    When I create a Post with:
      """
      {"id": "p_generated", "blogId": "b1", "title": "New Post"}
      """
    Then the operation succeeds
    Then the result.createdAt matches ISO-8601 timestamp

  Scenario: update without updatedAt refreshes timestamp
    When I update the Post with:
      """
      {"id": "p1", "title": "Updated Title"}
      """
    Then the operation succeeds
    Then the result.updatedAt matches ISO-8601 timestamp

  Scenario: create with explicit updatedAt preserves the value
    When I create a Post with:
      """
      {"id": "p_explicit_both", "blogId": "b1", "title": "Post with explicit timestamps", "createdAt": "2025-01-01T10:00:00Z", "updatedAt": "2025-01-02T11:00:00Z"}
      """
    Then the operation succeeds
    Then the result.createdAt is "2025-01-01T10:00:00Z"
    Then the result.updatedAt is "2025-01-02T11:00:00Z"

  Scenario: update with explicit updatedAt preserves the value
    When I update the Post with:
      """
      {"id": "p1", "title": "Updated with explicit timestamp", "updatedAt": "2025-01-03T12:00:00Z"}
      """
    Then the operation succeeds
    Then the result.updatedAt is "2025-01-03T12:00:00Z"

  Scenario: create with null updatedAt falls back to createdAt
    When I create a Post with:
      """
      {"id": "p_null_updated", "blogId": "b1", "title": "Post with null updatedAt", "createdAt": "2025-01-05T10:00:00Z", "updatedAt": null}
      """
    Then the operation succeeds
    Then the result.createdAt is "2025-01-05T10:00:00Z"
    Then the result.updatedAt is "2025-01-05T10:00:00Z"

  Scenario: index query DESC sorts by explicit createdAt values
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "sortDirection": "DESC", "limit": 1}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p3"]

  Scenario: create rejects invalid createdAt datetime
    When I create a Post with:
      """
      {"id": "p_invalid_dt", "blogId": "b1", "title": "Invalid Date", "createdAt": "not-a-date"}
      """
    Then the operation fails with errorType "ValidationException"

