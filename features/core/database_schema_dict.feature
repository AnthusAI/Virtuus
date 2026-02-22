Feature: Database from schema dict
  As a developer loading schemas programmatically
  I want to build a database from an in-memory schema dictionary
  So that I can avoid writing YAML to disk

  Scenario: Load database from schema dict with directories, GSIs, and associations
    Given a temporary data root with user fixture files
    And a database schema dictionary:
      """
      {
        "tables": {
          "users": {
            "primary_key": "id",
            "directory": "users",
            "gsis": {"by_status": {"partition_key": "status"}},
            "associations": {
              "posts": {"type": "has_many", "table": "posts", "index": "by_user"}
            }
          },
          "posts": {
            "primary_key": "id",
            "gsis": {"by_user": {"partition_key": "user_id"}},
            "associations": {
              "author": {"type": "belongs_to", "table": "users", "foreign_key": "user_id"}
            }
          },
          "jobs": {
            "primary_key": "id",
            "associations": {
              "workers": {
                "type": "has_many_through",
                "through": "job_assignments",
                "index": "by_job",
                "table": "workers",
                "foreign_key": "worker_id"
              }
            }
          },
          "job_assignments": {
            "primary_key": "id",
            "gsis": {"by_job": {"partition_key": "job_id"}}
          },
          "workers": {"primary_key": "id"}
        }
      }
      """
    When I create a database from the schema dictionary
    Then the database should have loaded 1 user record from disk
    And the database describe output should include stale flag for "users"
    And the database should have GSIs and associations configured from the schema dict
