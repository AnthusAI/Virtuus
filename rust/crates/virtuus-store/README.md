# Virtuus portable storage foundation

`virtuus-store` adds protocol-independent object and card-catalog interfaces alongside the existing `virtuus::Table`. It does not replace the existing filesystem table or impose a REST/GraphQL contract.

Canonical files are ordinary objects at `<tenant>/<project>/<relative-path>` with exactly the same relative paths and raw bytes as a local project: `.kanbus.yml`, `project/issues/<id>.json`, immutable `project/events/<timestamp>__<uuid>.json`, and arbitrary attachments. Unknown JSON fields, whitespace, IDs, links, configuration and large history files stay in these objects. DynamoDB holds only small scalar card summaries, revision/hash/length metadata and canonical-path pointers. There are no inline payloads, overflow thresholds, chunks, envelopes or content-addressed canonical filenames.

## Backends and API

- `ObjectStore`: exact-path get and explicit byte-budget reads, bounded lexicographic list, create/update with `WriteCondition::Absent` or opaque expected revision, conditional delete, separate supplemental control objects.
- `Catalog`: exact-path get, conditional small-card put/delete, scoped type/status candidate query, counters for actual requests and examined candidates.
- `FileStore`: namespace folders, or `for_project` binding an existing ordinary project folder. Control state lives outside the project. Content hashes provide revisions; cooperating writers use advisory flock and atomic rename.
- `S3Store`: native ETag conditions and normal S3 object bodies. Mutating calls override SDK retries to one attempt: blindly replaying an ambiguous create/delete can otherwise produce same-byte ABA races.
- `MemoryCatalog`: rebuildable local metadata, scoped BTreeMap ranges. Local type/status selections examine scoped cards and report that cost; this is not a distributed index.
- `DynamoCatalog`: `scope` partition key and `path` sort key, plus `by_type(type_scope,path)`, `by_status(status_scope,path)`, and `by_type_status(type_status_scope,path)`, all projected with `ALL`. Queries use bounded, delimiter-safe partition fanout, never a table scan. GSIs are eventually consistent; exact card reads and the base scope query are strongly consistent.
- `JournaledStore`: raw reads/list, conditional idempotent issue create/update/delete, immutable history publication, catalog repair/rebuild and authorized-project query.

Scope identifiers are explicitly **lowercase ASCII** letters, digits, `-`, `_`, or `.`, up to 128 bytes, excluding empty, `.` and `..`; `_virtuus` is reserved as a tenant. Both constructors and deserialization validate this contract. Lowercase identifiers prevent case-insensitive filesystem aliasing. `Scope` is a namespace, not an authorization credential: the embedding API must verify tenant/project grants before using it. `AuthorizedProjects` validates an explicit caller-authorized project set within one tenant and rejects unauthorized selections; it never discovers grants by scanning storage.

Filesystem roots must be trusted and writable only by cooperating application/import processes. Native filesystem bounded reads check descriptor metadata and read at most the budget plus one sentinel byte; native S3 reads reject an excessive Content-Length and enforce the budget while streaming chunks. The ObjectStore trait requires a bounded method; wrappers must forward it, and custom adapters must enforce the budget before allocating a complete body. JournaledStore.with_canonical_read_limit opts API consumers into that budget for every internal canonical read, including replay, publication recovery and projection; public bounded reads cannot loosen the policy. Administration defaults to unlimited reads. Static symlinks in namespace ancestors or object paths are rejected, but filesystem path checks are not a defense against a malicious process swapping directory entries concurrently. Local listing walks/sorts the selected subtree; each page is bounded in output size, while local enumeration work depends on subtree size. S3 listing is backend-paged.

## Publication and recovery

A mutation's durable intent, execution claim and receipt live at `_virtuus/<tenant>/<project>/...`, outside the canonical prefix. A path's active command is advanced with CAS. A command acquires a separate, non-expiring execution claim before publication; concurrent retries cannot issue a second canonical put/delete. A successor first resolves the prior command's receipt. Successful issue publication is followed by immutable history objects and a projection of the **current** canonical bytes. Incomplete follow-up returns `repair_pending=true`; retry the same operation to converge without overwriting a newer issue. Rebuild derives metadata from ordinary imported issue JSON, preserves configured field paths and removes stale catalog rows.

This is **not an S3+DynamoDB transaction**, and readers may observe a published issue before history/catalog follow-up. Exact issue reads use S3 directly. Application-generated commands must look up an existing operation before generating new IDs/timestamps, and use a stable request digest. A reused operation ID with a different request digest or action is rejected; callers must compute digests from the complete logical request.

`Conflict` means the expected condition definitively failed. `Unavailable` on a canonical mutation must mean the adapter proves it was not applied; the owning worker may make that attempt retryable after the request has returned. `Uncertain` means it may have applied. A receipt can be reconstructed only from surviving own-operation metadata (put) or witnessed absence under the cooperating-writer protocol (delete). If publication cannot be inferred, its ownership is retained, the intent is kept, and successors remain blocked. **There is no lease timeout that fences a late native S3 request.** Operator recovery requires stopping the owning worker, establishing that no request remains in flight, inspecting canonical bytes/metadata and intent, and deciding whether to repair the receipt or allow a fresh attempt. This foundation deliberately provides no unsafe automatic claim-reset endpoint. Direct writes/imports must be offline: overwriting canonical data during an uncertain operation can destroy the evidence needed to recover it.

Supplemental receipts currently retain the original published bytes in S3 control state so retries can return their original result after later updates. They are not required to rebuild a current catalog from plain files; retention/compaction is future operational policy.

## Board queries

`query_authorized` handles one selected project, several selected projects, or all explicitly authorized projects; all/selected types and statuses use primary candidate indexes. Residual scalar predicates use the existing `virtuus::SortCondition` API shape. JSON numeric predicates and ordering use a shared exact decimal digit comparator, preserving adjacent integers above 2^53, integer/decimal equivalence, signed zero and exponent values without binary float rounding. Other scalar semantics reuse the existing comparator. Scope/path provides a deterministic tie-breaker. Custom fields must be included among the configured projected dotted paths. Type/status strings above 512 bytes are retained in small metadata when they fit, but excluded from bounded GSI keys; all-project and exact-path lookup still work. Projection prioritizes ordinary index fields, then optional scalar fields within the 16 KB metadata budget. Optional fields too large to fit are omitted, identified in Card.omitted_fields, and never truncated or rewritten in canonical JSON. A query requiring such an omitted field explicitly fails; an embedding API may hydrate canonical bytes under its read budgets instead. No CRM-specific tables, APIs or indexes are created.

Arbitrary residual ordering materializes all selected **card summaries** before sorting; it never downloads canonical payloads. Candidate acceptance defaults to 10,000, with at most 32 total project/type/status query streams, 32 predicates and eight scalar order fields. Above the accepted candidate count, the operation fails explicitly rather than returning incomplete ordering. The final bounded backend page can overshoot that acceptance threshold (one selected row plus lookahead per stream at minimum); the threshold is not a hard read quota. Local candidate examination can cover the selected project. Request/candidate counters report actual work. Keyset cursors are bound to the authorization selection and query; concurrent mutations can move rows, so paging is not snapshot isolation.

## Ordinary copy export/import

1. Quiesce mutations and direct folder/S3 writes. Repair all pending history/catalog follow-up and resolve uncertain publication before declaring a complete export; a raw prefix copy alone does not prove complete history or give snapshot isolation.
2. Copy only `<tenant>/<project>/` to an ordinary folder, preserving each relative path and bytes. Supplemental `_virtuus/` control state is excluded. No special decoder or manifest is required.
3. Import ordinary files into a quiesced destination using create-only publication. An identical existing file is a no-op; a conflicting duplicate must fail explicitly. Do not select a winner by timestamp or rewrite unknown fields.
4. Call `rebuild_catalog` (or `rebuild_catalog_with_fields` for configured custom scalars). This derives current cards solely from ordinary canonical JSON and removes stale rows; no source receipts are required.

The real AWS fixture test exercises direct export/import of configuration, unknown JSON, binary attachments and a 450 KB history file, and confirms all copied raw paths/bytes match. The embedding application remains responsible for the quiescence/preflight protocol.

## Verification

Use a recent Rust toolchain: current AWS SDK dependencies require Rust 1.94.1 or newer; the local verification uses 1.98.1. Make sure both Cargo and its `clippy-driver` resolve to that toolchain.

```sh
cd rust
cargo test -p virtuus-store
cargo clippy -p virtuus-store --all-targets --features aws -- -D warnings
cargo fmt --check
```

The Cucumber executable runs `features/store/portable_objects.feature`. Integration tests also cover conditional deletion/recreation, deterministic concurrent duplicate barriers, receipt/history failures, definitely unapplied versus ambiguous upload failure, bounded native reads and replay reads, oversized optional projection fields, exact numeric queries, custom-field preservation, namespace validation/symlinks and authorized multi-project query paging. Real AWS provisioning and evidence are in [`infra/test-store`](../../../infra/test-store/README.md); fixtures use unique disposable resources, not preexisting production data.

The final default-backend coverage measurement is 86.29% of lines and 83.31% of regions (not 100%). The existing core/Python coverage gates remain unchanged; new storage behavioral checks, Clippy, format, independent review and live AWS proofs are additional validation, not a claim of complete line coverage.


Repository validation on 2026-10-14: the full `make check` passed using Rust 1.98.1, a local editable Python 0.5.1 environment and `CARGO_INCREMENTAL=0`. Python passed 235 scenarios with 100% coverage; Rust core passed 213 tests and 100% prescribed Tarpaulin coverage (1584/1584 lines); Amplify retained 100% (1360/1360), AppSync retained 100% (507/507); all format/Clippy gates and parity passed (435 paired steps, five preexisting pending). Storage passed 20 Rust tests plus four Cucumber scenarios/14 steps; external fixture tests are explicitly invoked separately in the live evidence. The final focused numeric regression also passed, including fail-closed rejection of an unrepresentable exponent. Peer-crate coverage excludes the new store just as it excludes existing peers; no existing 100% threshold was reduced.
