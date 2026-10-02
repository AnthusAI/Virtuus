@rust-only
Feature: Sort key operators in index queries

  Scenario: Index query with eq operator on sort key
    Given a blog contract
    And a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    And these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Post One", "createdAt": "2026-01-01T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Post Two", "createdAt": "2026-01-02T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Post Three", "createdAt": "2026-01-03T10:00:00Z"}
      ]
      """
    When I open the engine
    And I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"eq": "2026-01-02T10:00:00Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with lt operator on sort key
    Given a blog contract
    And a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    And these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Post One", "createdAt": "2026-01-01T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Post Two", "createdAt": "2026-01-02T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Post Three", "createdAt": "2026-01-03T10:00:00Z"}
      ]
      """
    When I open the engine
    And I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"lt": "2026-01-03T10:00:00Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with le operator on sort key
    Given a blog contract
    And a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    And these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Post One", "createdAt": "2026-01-01T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Post Two", "createdAt": "2026-01-02T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Post Three", "createdAt": "2026-01-03T10:00:00Z"}
      ]
      """
    When I open the engine
    And I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"le": "2026-01-02T10:00:00Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with gt operator on sort key
    Given a blog contract
    And a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    And these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Post One", "createdAt": "2026-01-01T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Post Two", "createdAt": "2026-01-02T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Post Three", "createdAt": "2026-01-03T10:00:00Z"}
      ]
      """
    When I open the engine
    And I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"gt": "2026-01-01T10:00:00Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with ge operator on sort key
    Given a blog contract
    And a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    And these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Post One", "createdAt": "2026-01-01T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Post Two", "createdAt": "2026-01-02T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Post Three", "createdAt": "2026-01-03T10:00:00Z"}
      ]
      """
    When I open the engine
    And I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"ge": "2026-01-02T10:00:00Z"}}}
      """
    Then the operation succeeds

  Scenario: Index query with beginsWith operator on sort key
    Given a blog contract
    And a Blog exists with:
      """
      {"id": "b1", "title": "Blog"}
      """
    And these Post records exist:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Apple", "createdAt": "2026-01-01T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Apply", "createdAt": "2026-01-02T10:00:00Z"},
        {"id": "p3", "blogId": "b1", "title": "Banana", "createdAt": "2026-01-03T10:00:00Z"}
      ]
      """
    When I open the engine
    And I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1", "createdAt": {"beginsWith": "2026-01-0"}}}
      """
    Then the operation succeeds
