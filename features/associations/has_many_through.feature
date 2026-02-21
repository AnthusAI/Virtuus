Feature: Has-many-through associations
  As a developer querying many-to-many relationships
  I want to define has_many_through associations using junction tables
  So that I can traverse many-to-many relationships

  Scenario: Define a has_many_through association
    Given a table "jobs" with primary key "id"
    And a junction table "job_assignments" with a GSI "by_job" on "job_id"
    And a table "workers" with primary key "id"
    When I define a has_many_through association "workers" on "jobs" through "job_assignments" via index "by_job" targeting "workers" via foreign key "worker_id"
    Then the "jobs" table should have an association named "workers"

  Scenario: Resolve has_many_through returns target records
    Given a has_many_through association from "jobs" to "workers" through "job_assignments"
    And job {"id": "job-1"}
    And workers:
      | id       | name  |
      | worker-1 | Alice |
      | worker-2 | Bob   |
      | worker-3 | Carol |
    And job_assignments:
      | id   | job_id | worker_id |
      | ja-1 | job-1  | worker-1  |
      | ja-2 | job-1  | worker-2  |
      | ja-3 | job-2  | worker-3  |
    When I resolve the "workers" association for job "job-1"
    Then the result should contain workers "worker-1" and "worker-2"
    And the result should not contain "worker-3"

  Scenario: Resolve has_many_through with no junction records returns empty
    Given a has_many_through association from "jobs" to "workers" through "job_assignments"
    And job {"id": "job-1"}
    And no job_assignments for "job-1"
    When I resolve the "workers" association for job "job-1"
    Then the result should be empty
