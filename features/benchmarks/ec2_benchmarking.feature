Feature: EC2 benchmarking harness
  As a maintainer I want to run the storage-mode benchmark suite on EC2
  so that we can publish performance ranges across common instance types,
  up to 1,000,000 records, and compare index_only vs memory with RSS.

  @bench @ec2 @manual
  Scenario: Document EC2 benchmark workflow
    Given the EC2 params file "tools/ec2_params.example.json" is filled out with real values
    When I run "python tools/run_ec2_from_params.py --params tools/ec2_params.example.json --no-dry-run"
    Then EC2 instances are launched for each instance type
    And storage-mode benchmarks run to completion with results uploaded or pulled back
    And charts and summaries include instance_type metadata
