@rust-only
Feature: AppSync SDL to dynamic GraphQL schema
  Parse an AppSync SDL string and build a dynamic GraphQL schema
  that can be introspected to verify it matches the input.

  Scenario: Parse blog.graphql fixture and build schema
    Given the blog SDL
    When I build the schema
    Then the schema builds successfully
    And introspection of the schema matches the input SDL after normalization

  Scenario: Parse Apricity's real AppSync SDL and build schema
    Given the apricity SDL
    When I build the schema
    Then the schema builds successfully
    And introspection of the schema matches the input SDL after normalization

  Scenario: Schema contains all object types from SDL
    Given the blog SDL
    When I build the schema
    Then the schema has these types: Blog, Post, Query, Mutation, Subscription
    And the Blog type has fields: id, title, posts, createdAt, updatedAt
    And the Post type has fields: id, title, content, blogId, status, blog, comments, createdAt, updatedAt

  Scenario: Schema contains all input types from SDL
    Given the blog SDL
    When I build the schema
    Then the schema has these input types: CreateBlogInput, UpdateBlogInput, DeleteBlogInput, CreatePostInput, UpdatePostInput, DeletePostInput, ModelBlogFilterInput, ModelPostFilterInput, ModelPostConditionInput

  Scenario: Schema contains all enum types from SDL
    Given the blog SDL
    When I build the schema
    Then the schema has these enum types: PostStatus, ModelSortDirection, ModelAttributeTypes

  Scenario: AWS scalars are defined as custom scalars
    Given the blog SDL
    When I build the schema
    Then the schema has these scalar types: AWSDateTime, AWSDate, AWSTime, AWSTimestamp, AWSJSON, AWSEmail, AWSURL, AWSPhone, AWSIPAddress

  Scenario: Schema builds with implicit Query type
    Given a minimal schema with only types and no Query type
    When I build the schema
    Then the schema builds successfully

  Scenario: Schema handles interface types
    Given a schema with an interface type
    When I build the schema
    Then the schema builds successfully
