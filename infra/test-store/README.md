# Real AWS storage proof fixture

This fixture exercises the Virtuus store adapters independently of Amplify, REST, and Prospectus. `fixture.yaml` contains only a private encrypted versioned S3 bucket and an encrypted on-demand DynamoDB table with scope/path keys and type/status GSIs. It creates no IAM roles, network infrastructure, or public access. Both resources have Retain policies. Resource removal or version purging requires separate human authorization.

## Recorded fixture

- Account: `682849160744`; profile: `cyclotron-production`; region: `us-east-1`
- Verified principal: existing Ryan `AWSReservedSSO_AdministratorAccess_87b13a8969895e81` session
- Stack: `virtuus-store-proof-20261014-aws01`
- Stack ARN: `arn:aws:cloudformation:us-east-1:682849160744:stack/virtuus-store-proof-20261014-aws01/4d6bdef0-c767-11f1-a202-0affcaee7ba3`
- Bucket: `virtuus-store-proof-20261014-aws01-canonicalbucket-wdxzesz9pihb`
- Table: `virtuus-store-proof-20261014-aws01-CatalogTable-14GEGYK7A6UPN`
- Tags: `purpose=virtuus-disposable-store-proof`, `owner=Ryan`, `task=virt-bbb22448`
- CloudFormation reached `CREATE_COMPLETE` on 2026-10-14.

## Run

From the repository root, with a valid existing authorized SSO session:

```sh
VIRTUUS_TEST_RUN=unique-run-id python3 infra/test-store/run.py
```

The runner verifies the account, reads only this known stack, requires `CREATE_COMPLETE`, and passes temporary CLI-exported credentials directly to the child process environment. It does not print or save credentials. It uses the installed Cargo/rustc/rustdoc in `~/.cargo/bin` because the system compiler is older than the AWS SDK requirements. Set `VIRTUUS_TEST_FILTER` to an exact test name for a focused regression; omit it to run all live tests. Never reuse a run ID: fixture object creation is intentionally conditional.

The ignored `aws_real.rs` test verifies exact same-path S3 raw bytes including unknown fields/comments/history/config, a 450 KB event and binary attachment, one winner from competing ETag writers, an injected catalog failure after real S3 publication, same-operation retry and request mismatch, tenant/project isolation, scoped DynamoDB pagination, targeted index candidate/request counts, catalog removal and rebuild, wrong-revision/missing-object versus successful object deletion, deletion-operation replay, all three GSI selections, and authorized two-project board selection with residual filters and deterministic merged pagination. The test uses a wrapper to inject the failure; no IAM policies are changed. GSI reads wait for eventual consistency with a bounded retry.

Fixture test rows are conditionally deleted and rebuilt to prove recovery. One fixture canonical object is conditionally deleted to prove CRUD, preserving S3 versions. Resources and other fixture data remain inventoried for inspection. Emulator tests and source-only checks are separate evidence and do not count as live AWS proof.

Focused live deltas also cover 3,000-byte imported type/status values, a 17 KB title with explicit omitted-field metadata, native bounded S3 reads, and exact adjacent integer order/range comparisons above 2^53. See EVIDENCE.md for separately executed results and source-delta limits.
