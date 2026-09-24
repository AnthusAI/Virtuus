@rust-only
Feature: Pagination with limit before filter, nextToken, and sortDirection
  Pagination is key-based using the last evaluated key as an opaque base64url token.
  Limit is applied before the filter, so a page can be short or empty while nextToken is non-null.
  Order is by primary key for list, by index sort key for index queries, ASC by default or DESC with sortDirection.

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
        {"id": "p4", "blogId": "b1", "title": "Archived Post", "status": "ARCHIVED", "views": 200, "tags": ["featured", "archive"], "createdAt": "2026-03-10T10:00:00Z"},
        {"id": "p5", "blogId": "b1", "title": "Another Published", "status": "PUBLISHED", "views": 150, "tags": [], "createdAt": "2026-03-15T10:00:00Z"}
      ]
      """

  Scenario: limit applies before the filter
    # With limit 2, we read p1 and p2, but filter matches only p2.
    # First page returns [p2], second page reads p3 and p4, neither matches, so empty.
    # Third page reads p5, which matches, so returns [p5]. Fourth page has no more items.
    When I list all Post with:
      """
      {"limit": 2, "filter": {"status": {"eq": "PUBLISHED"}}}
      """
    Then the operation succeeds
    Then the result has exactly 1 item with id "p2"
    Then the nextToken is not null

    When I list all Post with the previous nextToken:
      """
      {"limit": 2, "filter": {"status": {"eq": "PUBLISHED"}}}
      """
    Then the operation succeeds
    Then the result has exactly 0 items
    Then the nextToken is not null

    When I list all Post with the previous nextToken:
      """
      {"limit": 2, "filter": {"status": {"eq": "PUBLISHED"}}}
      """
    Then the operation succeeds
    Then the result has exactly 1 item with id "p5"
    Then the nextToken is null

  Scenario: collecting all pages of a list with limit 2 equals the full list
    When I collect all pages of Post with:
      """
      {"limit": 2}
      """
    Then the result contains exactly these ids ["p1", "p2", "p3", "p4", "p5"]

  Scenario: an empty page with a non-null token
    When I list all Post with:
      """
      {"limit": 2, "filter": {"status": {"eq": "PUBLISHED"}}}
      """
    Then the operation succeeds
    Then the result has exactly 1 item with id "p2"
    Then the nextToken is not null

    When I list all Post with the previous nextToken:
      """
      {"limit": 2, "filter": {"status": {"eq": "PUBLISHED"}}}
      """
    Then the operation succeeds
    Then the result has exactly 0 items
    Then the nextToken is not null

  Scenario: DESC order on index query across pages
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "limit": 2, "sortDirection": "DESC"}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p5", "p4"]
    Then the nextToken is not null

    When I query Post by postsByBlog with the previous nextToken:
      """
      {"key": {"blogId": "b1"}, "limit": 2, "sortDirection": "DESC"}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p3", "p2"]
    Then the nextToken is not null

    When I query Post by postsByBlog with the previous nextToken:
      """
      {"key": {"blogId": "b1"}, "limit": 2, "sortDirection": "DESC"}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p1"]
    Then the nextToken is null

  Scenario Outline: invalid tokens
    When I list all Post with:
      """
      {"nextToken": "<token>"}
      """
    Then the operation has validation error

    Examples:
      | token |
      | not-base64url |
      | aW52YWxpZCBqc29u |
      | eyJpbnZhbGlkIjoiZmllbGQifQ |

  Scenario: token past end of list
    When I list all Post with:
      """
      {"limit": 5}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p1", "p2", "p3", "p4", "p5"]
    Then the nextToken is not null

    When I list all Post with the previous nextToken:
      """
      {}
      """
    Then the operation succeeds
    Then the result contains exactly these ids []
    Then the nextToken is null

  Scenario: non-string nextToken is validation error
    When I list all Post with:
      """
      {"nextToken": 5}
      """
    Then the operation has validation error

  Scenario: index query with invalid token
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "limit": 2}
      """
    Then the operation succeeds
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "nextToken": "garbage"}
      """
    Then the operation has validation error

  Scenario: index query token past end
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "limit": 5}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p1", "p2", "p3", "p4", "p5"]
    Then the nextToken is not null

    When I query Post by postsByBlog with the previous nextToken:
      """
      {"key": {"blogId": "b1"}}
      """
    Then the operation succeeds
    Then the result contains exactly these ids []
    Then the nextToken is null

  Scenario: explicit null nextToken is same as no token
    When I list all Post with:
      """
      {"limit": 2, "nextToken": null}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p1", "p2"]

  Scenario: token that is a JSON array instead of object
    When I list all Post with:
      """
      {"limit": 2, "nextToken": "WzEsIDIsIDNd"}
      """
    Then the operation has validation error

