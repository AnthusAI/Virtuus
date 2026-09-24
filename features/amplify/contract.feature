@rust-only
Feature: Contract loading, tables and indexes

  Scenario: The blog contract opens into these tables
    Given a blog contract
    When I open the engine
    Then the engine description is:
      """
      {
        "tables": [
          {
            "name": "Article",
            "primaryKey": "id",
            "indexes": []
          },
          {
            "name": "Blog",
            "primaryKey": "id",
            "indexes": []
          },
          {
            "name": "Catalog",
            "primaryKey": "id",
            "indexes": []
          },
          {
            "name": "Comment",
            "primaryKey": "id",
            "indexes": [
              {
                "name": "commentsByPost",
                "partitionKey": "postId"
              }
            ]
          },
          {
            "name": "Post",
            "primaryKey": "id",
            "indexes": [
              {
                "name": "postsByBlog",
                "partitionKey": "blogId",
                "sortKey": "createdAt"
              },
              {
                "name": "postsByBlogStatus",
                "partitionKey": "blogId",
                "sortKey": "status#createdAt"
              }
            ]
          },
          {
            "name": "Private",
            "primaryKey": "id",
            "indexes": []
          },
          {
            "name": "Public",
            "primaryKey": "id",
            "indexes": []
          },
          {
            "name": "Restricted",
            "primaryKey": "id",
            "indexes": []
          },
          {
            "name": "Tag",
            "partitionKey": "postId",
            "sortKey": "name",
            "indexes": [
              {
                "name": "tagsByName",
                "partitionKey": "name"
              },
              {
                "name": "tagsByPost",
                "partitionKey": "postId",
                "implicit": true
              }
            ]
          },
          {
            "name": "Vote",
            "partitionKey": "postId",
            "sortKey": "userId#kind",
            "indexes": []
          }
        ]
      }
      """

  Scenario: The Apricity contract opens
    Given an apricity contract
    When I open the engine
    Then the engine has tables:
      """
      Recording, Clip, Slice, Marker, Candidate, Verdict, Crate, CrateItem, Score, ScoreRef, Job
      """
    And the tables have these indexes:
      """
      Candidate|candidatesByClip, candidatesByKind, candidatesByRecording
      Clip|clipsByCollection, clipsByParent, clipsByPath, clipsByRecording
      CrateItem|crateItemsByCandidate, crateItemsByCrate
      Crate|cratesByOwner
      Job|jobsByState
      Marker|markersByClip
      Recording|recordingsByCollection
      ScoreRef|refsByClip, refsByScore, refsBySlice
      Score|scoresByFolder
      Slice|slicesByCandidate, slicesByClip, slicesByClipAndName
      Verdict|verdictsByJudge
      """

  Scenario: Table Verdict has partition candidateId and sort judge
    Given an apricity contract
    When I open the engine
    Then table "Verdict" has partition "candidateId" and sort "judge"

  Scenario: Real tables can be written and queried through the Virtuus API
    Given a blog contract
    When I open the engine
    And I put these Post records:
      """
      [
        {"id": "p1", "blogId": "b1", "title": "Newer", "status": "DRAFT", "createdAt": "2026-01-02T10:00:00Z", "status#createdAt": "DRAFT#2026-01-02T10:00:00Z"},
        {"id": "p2", "blogId": "b1", "title": "Older", "status": "PUBLISHED", "createdAt": "2026-01-01T10:00:00Z", "status#createdAt": "PUBLISHED#2026-01-01T10:00:00Z"}
      ]
      """
    Then querying postsByBlog for blogId "b1" returns ids ["p2", "p1"]

  Scenario Outline: An invalid contract is rejected
    Given the blog contract with <op> at "<pointer>" and value <value>
    When I open the engine
    Then the error contains "<message_fragment>"

    Examples:
      | op | pointer | value | message_fragment |
      | removed | /models |  | is a required property |
      | replaced by: | /enums/PostStatus | 1 | is not of type |
      | replaced by: | /models/Post/fields/status/type | "Nope" | undefined enum |
      | replaced by: | /models/Tag/primaryKey | ["postId","nope"] | does not exist |
      | replaced by: | /models/Post/indexes/0/partitionField | "nope" | does not exist |
      | replaced by: | /models/Post/indexes/0/sortFields/0 | "nope" | does not exist |
      | replaced by: | /models/Comment/relationships/0/target | "Nope" | non-existent model |
      | replaced by: | /models/Post/fields/meta/type | "Nope" | undefined customType |
