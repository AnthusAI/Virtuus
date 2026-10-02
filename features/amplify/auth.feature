@rust-only
Feature: Identity, owner fields and authorization rules
  Authorization rules control who can perform operations (create, read, list, update, delete).
  Owner fields are filled on create as sub::username, and ownership determines read/write access.
  Authorization rules: owner (record owner only), groups (user in group), authenticated (any user), public/apiKey (API key only).
  An operation is allowed if ANY rule allows it. For list/index, only readable records are returned.

  Background:
    Given a blog contract
    Given I open the engine
    Given a Blog exists with:
      """
      {"id": "b1", "title": "Tech Blog"}
      """

  Scenario: create fills owner field as sub::username
    When I create a Post with:
      """
      {"id": "p1", "blogId": "b1", "title": "My Post"}
      """
    Then the operation succeeds
    Then the result.owner is "test-sub::testuser"

  Scenario: user u2 cannot update user u1's Post
    When I create a Post with:
      """
      {"id": "p_u1", "blogId": "b1", "title": "U1's Post"}
      """
    Then the operation succeeds
    When I switch to user "u2" with sub "u2-sub" and username "u2"
    When I update the Post with:
      """
      {"id": "p_u1", "title": "Hacked"}
      """
    Then the operation fails with errorType "Unauthorized"

  Scenario: user u1 can update their own Post
    When I create a Post with:
      """
      {"id": "p_u1", "blogId": "b1", "title": "U1's Post"}
      """
    Then the operation succeeds
    When I update the Post with:
      """
      {"id": "p_u1", "title": "Updated by U1"}
      """
    Then the operation succeeds

  Scenario: owner-scoped list shows only your own records
    When I create a Post with:
      """
      {"id": "p_u1", "blogId": "b1", "title": "U1's Post"}
      """
    Then the operation succeeds
    When I switch to user "u2" with sub "u2-sub" and username "u2"
    When I create a Post with:
      """
      {"id": "p_u2", "blogId": "b1", "title": "U2's Post"}
      """
    Then the operation succeeds
    When I switch to user "u1" with sub "test-sub" and username "testuser"
    When I list all Post with:
      """
      {}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p_u1"]

  Scenario: group member can read, not write without curator role
    When I switch to user "curator" with sub "curator-sub" and username "curator" in groups ["curators"]
    When I create a Catalog with:
      """
      {"id": "c1", "title": "Shared Catalog", "description": "A shared resource"}
      """
    Then the operation succeeds
    When I switch to user "member" with sub "member-sub" and username "member" in groups ["members"]
    When I get the Catalog with id "c1"
    Then the operation succeeds
    When I update the Catalog with:
      """
      {"id": "c1", "title": "Hacked"}
      """
    Then the operation fails with errorType "Unauthorized"

  Scenario: curator can read and write
    When I switch to user "curator" with sub "curator-sub" and username "curator" in groups ["curators"]
    When I create a Catalog with:
      """
      {"id": "c1", "title": "Shared Catalog", "description": "Initial description"}
      """
    Then the operation succeeds
    When I update the Catalog with:
      """
      {"id": "c1", "title": "Updated by Curator"}
      """
    Then the operation succeeds

  Scenario: apiKey identity can access public/apiKey rule
    When I switch to apiKey
    When I get the Blog with id "b1"
    Then the operation succeeds

  Scenario: enforce_auth false allows all operations and fills owner
    When I open the engine with enforce_auth disabled
    When I create a Post with:
      """
      {"id": "p1", "blogId": "b1", "title": "Public Post"}
      """
    Then the operation succeeds
    Then the result.owner is "test-sub::testuser"

  Scenario: spoofing with different owner is rejected when enforce_auth enabled
    When I create a Post with:
      """
      {"id": "p_spoofed", "blogId": "b1", "title": "Spoofed Post", "owner": "hacker::malicious"}
      """
    Then the operation fails with errorType "Unauthorized"

  Scenario: spoofing with different owner is allowed when enforce_auth disabled
    When I open the engine with enforce_auth disabled
    When I create a Post with:
      """
      {"id": "p_migrated", "blogId": "b1", "title": "Migrated Post", "owner": "legacy::user"}
      """
    Then the operation succeeds
    Then the result.owner is "legacy::user"

  Scenario: article uses author field instead of owner
    When I create a Article with:
      """
      {"id": "a1", "title": "My Article", "content": "Hello world"}
      """
    Then the operation succeeds
    Then the result.author is "test-sub::testuser"

  Scenario: user cannot update another user's article
    When I create a Article with:
      """
      {"id": "a_u1", "title": "U1 Article", "content": "Content by U1"}
      """
    Then the operation succeeds
    When I switch to user "u2" with sub "u2-sub" and username "u2"
    When I update the Article with:
      """
      {"id": "a_u1", "title": "Hacked"}
      """
    Then the operation fails with errorType "Unauthorized"

  Scenario: apiKey cannot create when enforce_auth is enabled
    When I switch to apiKey
    When I create a Post with:
      """
      {"id": "p_api", "blogId": "b1", "title": "API Post"}
      """
    Then the operation fails with errorType "Unauthorized"

  Scenario: apiKey can list (Blog has public/apiKey rule)
    When I switch to apiKey
    When I list all Blog with:
      """
      {}
      """
    Then the operation succeeds

  Scenario: regular user cannot delete another's Post (delete rule is owner only)
    When I create a Post with:
      """
      {"id": "p_to_delete", "blogId": "b1", "title": "To Delete"}
      """
    Then the operation succeeds
    When I switch to user "u2" with sub "u2-sub" and username "u2"
    When I delete the Post with id "p_to_delete"
    Then the operation fails with errorType "Unauthorized"

  Scenario: user can delete their own Post
    When I create a Post with:
      """
      {"id": "p_del", "blogId": "b1", "title": "Deletable"}
      """
    Then the operation succeeds
    When I delete the Post with id "p_del"
    Then the operation succeeds

  Scenario: regular user cannot read Catalog with only members read access
    When I switch to user "curator" with sub "curator-sub" and username "curator" in groups ["curators"]
    When I create a Catalog with:
      """
      {"id": "c_protected", "title": "Protected Catalog", "description": "Only for members"}
      """
    Then the operation succeeds
    When I switch to user "regular" with sub "reg-sub" and username "regular"
    When I get the Catalog with id "c_protected"
    Then the operation fails with errorType "Unauthorized"

  Scenario: apiKey cannot read Catalog (no public/apiKey rule)
    When I switch to user "curator" with sub "curator-sub" and username "curator" in groups ["curators"]
    When I create a Catalog with:
      """
      {"id": "c_api", "title": "API Catalog"}
      """
    Then the operation succeeds
    When I switch to apiKey
    When I get the Catalog with id "c_api"
    Then the operation fails with errorType "Unauthorized"

  Scenario: apiKey can read Public record with public allow rule
    When I switch to apiKey
    When I create a Public with:
      """
      {"id": "pub1", "content": "Public data"}
      """
    Then the operation succeeds
    When I get the Public with id "pub1"
    Then the operation succeeds

  Scenario: regular user cannot read Public record (public/apiKey rule)
    When I switch to apiKey
    When I create a Public with:
      """
      {"id": "pub2", "content": "Only for API keys"}
      """
    Then the operation succeeds
    When I switch to user "regular" with sub "reg-sub" and username "regular"
    When I get the Public with id "pub2"
    Then the operation fails with errorType "Unauthorized"

  Scenario: apiKey cannot read Private record (private/authenticated rule)
    When I switch to user "testuser" with sub "test-sub" and username "testuser"
    When I create a Private with:
      """
      {"id": "priv1", "content": "Private data"}
      """
    Then the operation succeeds
    When I switch to apiKey
    When I get the Private with id "priv1"
    Then the operation fails with errorType "Unauthorized"

  Scenario: owner-scoped index query returns only readable records
    When I create a Post with:
      """
      {"id": "p_index1", "blogId": "b1", "title": "U1 Post"}
      """
    Then the operation succeeds
    When I switch to user "u2" with sub "u2-sub" and username "u2"
    When I create a Post with:
      """
      {"id": "p_index2", "blogId": "b1", "title": "U2 Post"}
      """
    Then the operation succeeds
    When I switch to user "testuser" with sub "test-sub" and username "testuser"
    When I query Post by postsByBlog with:
      """
      {"key": {"blogId": "b1"}, "limit": 10}
      """
    Then the operation succeeds
    Then the result contains exactly these ids ["p_index1"]

  Scenario: hasMany filters unreadable items when selecting relationship
    When I create a Post with:
      """
      {"id": "p_with_comments", "blogId": "b1", "title": "Post with Comments"}
      """
    Then the operation succeeds
    When I create a Comment with:
      """
      {"id": "c_u1", "postId": "p_with_comments", "content": "Comment by U1", "owner": "test-sub::testuser"}
      """
    Then the operation succeeds
    When I switch to user "u2" with sub "u2-sub" and username "u2"
    When I create a Comment with:
      """
      {"id": "c_u2", "postId": "p_with_comments", "content": "Comment by U2", "owner": "u2-sub::u2"}
      """
    Then the operation succeeds
    When I switch to user "u1" with sub "test-sub" and username "testuser"
    When I get the Post with id "p_with_comments" and selectionSet:
      """
      ["id", "title", "comments.*"]
      """
    Then the operation succeeds
    Then the result.comments.items has exactly 1 record with ids ["c_u1"]

  Scenario: belongsTo returns null for unreadable parent
    When I switch to user "u1" with sub "test-sub" and username "testuser"
    When I create a Post with:
      """
      {"id": "p_u1_post", "blogId": "b1", "title": "U1 Post"}
      """
    Then the operation succeeds
    When I switch to user "u2" with sub "u2-sub" and username "u2"
    When I create a Comment with:
      """
      {"id": "c_u2_comment", "postId": "p_u1_post", "content": "Comment by U2 on U1 post", "owner": "u2-sub::u2"}
      """
    Then the operation succeeds
    When I get the Comment with id "c_u2_comment" and selectionSet:
      """
      ["id", "content", "post.id"]
      """
    Then the operation succeeds
    Then the result.post is null

  Scenario: model with no auth rules denies access when enforce_auth enabled
    When I create a Restricted with:
      """
      {"id": "r1", "content": "Restricted data"}
      """
    Then the operation fails with errorType "Unauthorized"

  Scenario: custom auth type is not supported
    Given the blog contract with replaced by: at "/models/Private/authRules/0/allow" and value "custom"
    When I open the engine
    Then the error contains "custom (lambda) authorization is not supported"

