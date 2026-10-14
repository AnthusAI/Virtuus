# Executed real AWS proof, 2026-10-14

The account guard was executed through `aws sts get-caller-identity --profile cyclotron-production` and returned account `682849160744` with Ryan's existing AdministratorAccess SSO principal. CloudFormation `create-stack` accepted the minimal fixture and `describe-stacks` confirmed `CREATE_COMPLETE`. See README.md for exact stack/resource names.

Actual resource reads independently confirmed all four S3 public access blocks true, versioning Enabled, AES256 default encryption, DynamoDB ACTIVE/PAY_PER_REQUEST/SSE ENABLED, and `by_type`, `by_status`, `by_type_status` indexes. No IAM roles/grants or networking resources were created.

## First live run

```sh
VIRTUUS_TEST_RUN=proof-20261014-01 python3 infra/test-store/run.py
```

The ignored real AWS test compiled with Rust 1.98.1 and ran against the fixture. Earlier assertions passed, but the final assertion failed: catalog rebuild retained a stale row after its canonical S3 object was deleted. This finding led to the library's stale-row cleanup fix. This run is failed evidence, not a passing proof.

## Passing expanded live run

```sh
VIRTUUS_TEST_RUN=proof-20261014-02 python3 infra/test-store/run.py
```

Executed output:

```text
LIVE_AWS bucket=virtuus-store-proof-20261014-aws01-canonicalbucket-wdxzesz9pihb table=virtuus-store-proof-20261014-aws01-CatalogTable-14GEGYK7A6UPN scope=proof-20261014-02/project-a all_candidates=25 all_requests=1 targeted_candidates=2 targeted_requests=1 large_event_bytes=450015 single_cas_winner=true recovery=true rebuilt=25 final_cards=24 ordinary_prefix_copy=true folder_import=true
test real_aws_canonical_cas_catalog_and_recovery ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 93.68s
```

Assertions include canonical raw bytes and ordinary same relative S3 paths; unknown JSON fields, comments/history, YAML config comments, binary attachments; large event above DynamoDB's item limit; two competing S3 ETag writes with exactly one success; tenant and project isolation; create/get/list/update/delete behavior; stale revision conflict; journal write retry/request mismatch; injected catalog outage after real canonical publication; ordinary prefix-copy export and local-folder import; 25-card catalog removal/rebuild; stale row cleanup after canonical deletion; logical deletion with a catalog outage and same-operation repair retry; lexically ordered base-query pagination; and targeted type/status GSI candidate counts after bounded convergence waits.

The measured targeted query was `type=bug AND status=open`, matching 2 of 25 canonical issue cards. This proof measures adapter query candidates/requests, not a full application endpoint or UI latency benchmark. Failure injection came from a Catalog wrapper; underlying successful operations used real S3 and DynamoDB.

Source formatting checks passed: rustfmt on `aws_real.rs`, Black `--check` and Ruff `check` on `run.py`. Broader repository checks are recorded by the owning lead/root; they must not be inferred from this live result.

## Remaining inventory

Resources remain provisioned for inspection. Fixture data lives under `proof-20261014-01/` and `proof-20261014-02/` canonical prefixes and corresponding `_virtuus/` control prefixes. Expanded run also created `proof-20261014-02/project-b/` (ordinary prefix copy) and `proof-20261014-02/folder-import/` (ordinary folder import). Conditional test object deletes preserve bucket versions; test catalog rows were removed/rebuilt as part of the proof. No bucket purge, stack deletion, or preexisting-resource mutation occurred. Any cleanup requires separate authorization.

## Final expanded source proof

After the adapter's publication ownership, uncertainty classification, and single-attempt mutation retry changes, the full live test was recompiled and explicitly executed:

```sh
VIRTUUS_TEST_RUN=proof-20261014-03 python3 infra/test-store/run.py
```

```text
LIVE_BOARD all_projects=2 all_cards=50 all_candidates=50 all_requests=2 targeted_candidates=4 targeted_requests=2 residual_matches=2 deterministic_pages=true selected_candidates=2 selected_requests=1
LIVE_AWS bucket=virtuus-store-proof-20261014-aws01-canonicalbucket-wdxzesz9pihb table=virtuus-store-proof-20261014-aws01-CatalogTable-14GEGYK7A6UPN scope=proof-20261014-03/project-a all_candidates=25 all_requests=1 targeted_candidates=2 targeted_requests=1 large_event_bytes=450015 single_cas_winner=true recovery=true rebuilt=25 final_cards=24 ordinary_prefix_copy=true folder_import=true
test real_aws_canonical_cas_catalog_and_recovery ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 103.46s
```

Additional assertions explicitly verified missing-object conditional S3 delete maps to Conflict; `deletion_operation` replays a completed deletion and rejects a request hash mismatch; all three GSIs converge correctly; type-index pagination remains lexical; authorized all-project loading considers 50 cards across two projects; targeted type/status queries read four candidates across those projects before a residual `id=0` predicate leaves two cards; scalar ordering and scope/path ties yield deterministic two-page results; one selected project reads two candidates in one request; unauthorized projects and a cursor reused under different project selection are rejected. This is a bounded 50-card adapter proof, not a 1,000-card application benchmark.

The fixture inventory also includes canonical/control prefixes for `proof-20261014-03/` and its ordinary-copy/import projects. No cleanup was performed.

## Latest oversized imported scalar regression

The last source change made DynamoDB GSI keys sparse for imported type/status strings above the supported indexed-selection bound, retaining those scalar values in ordinary card metadata. This focused test recompiled that latest source after the full proof above:

```sh
VIRTUUS_TEST_RUN=proof-20261014-04 VIRTUUS_TEST_FILTER=long_scalar_catalog_remains_rebuildable python3 infra/test-store/run.py
```

```text
LIVE_LONG_SCALAR scope=proof-20261014-04/long-scalar type_bytes=3000 status_bytes=3000 canonical_preserved=true rebuilt=1 direct_lookup=true all_query=true oversized_gsi_keys_omitted=true
test long_scalar_catalog_remains_rebuildable ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 2.57s
```

The test imported an ordinary canonical issue with 3,000-byte type and status strings, rebuilt its card, retrieved all scalar values by direct lookup and ordinary project catalog query, verified unchanged S3 bytes, and read the real DynamoDB row to confirm no oversized GSI keys were sent. Data remains under `proof-20261014-04/long-scalar/`. Run 03 covers the complete normal-enum flow; run 04 covers the sole later sparse-key source delta. Both are live AWS results.

## Bounded summary/read and exact integer delta proof

After adding explicit omitted-field card metadata, native bounded S3 reads, and exact decimal scalar comparison, the new focused live test was compiled against those changes:

```sh
VIRTUUS_TEST_RUN=proof-20261014-05 VIRTUUS_TEST_FILTER=bounded_import_and_exact_large_integer_queries python3 infra/test-store/run.py
```

```text
LIVE_BOUNDED_EXACT scope=proof-20261014-05/bounded-import giant_title_bytes=17000 canonical_preserved=true omitted_title_reported=true unprojected_query_rejected=true bounded_limit=1024 bounded_event_bytes=450000 full_budget_success=true adjacent_integers=9007199254740992,9007199254740993 exact_order_and_lt_gt=true numeric_candidates=2 numeric_requests=1
test bounded_import_and_exact_large_integer_queries ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 4.99s
```

An ordinary canonical issue containing a 17,000-byte title and unknown data imported/rebuilt successfully. Its bounded card retained raw hash, byte count and pointer, explicitly reported omitted title, and ordinary board loading succeeded. Title ordering and residual predicates rejected the unprojected field rather than returning misleading results. Native S3 reads of the issue and a 450,000-byte event rejected a 1,024-byte budget with `TooLarge`; exact full-size budgets returned unchanged bytes. Two canonical issues with reversed lexical paths and priorities 9007199254740992 and 9007199254740993 retained distinct integers through DynamoDB card serialization and sorted numerically, while strict less-than/greater-than predicates selected exactly the appropriate issue. The two-card scoped query reported two candidates and one request.

Fixture data remains in `proof-20261014-05/bounded-import/` and `proof-20261014-05/exact-numbers/`; no cleanup occurred. Run 05 covers these latest source deltas; the earlier full and focused live results remain separately recorded above. The full normal-flow test was not rerun after these deltas.
