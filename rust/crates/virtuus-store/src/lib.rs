//! Portable canonical objects with conditional publication and repairable catalogs.
#[cfg(feature = "aws")]
pub mod aws;
mod catalog;
mod filesystem;
mod journal;
mod numeric;
mod query;

use async_trait::async_trait;
pub use catalog::MemoryCatalog;
pub use filesystem::FileStore;
pub use journal::JournaledStore;
pub use numeric::compare_numbers;
pub use query::{
    AuthorizedProjects, BoardQuery, ProjectSelection, QueryOrder, ScalarPredicate, ScopedCard,
    ScopedCatalogPage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Storage failures distinguish conflicts from unavailable infrastructure.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A supplied revision or create-only condition did not match.
    #[error("revision conflict")]
    Conflict,
    /// A path, namespace, cursor, or request was invalid.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// An operation identifier was reused with a different request.
    #[error("operation identifier already used for a different mutation")]
    OperationMismatch,
    /// A prior publication cannot safely be inferred; manual repair is required.
    #[error("publication outcome is uncertain: {0}")]
    Uncertain(String),
    /// Infrastructure definitively rejected an operation before applying a mutation.
    /// Adapters must use `Uncertain` when a canonical mutation may have applied.
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    /// A filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Canonical object exceeds an explicit bounded-read budget.
    #[error("object exceeds read budget of {limit} bytes")]
    TooLarge {
        /// Maximum accepted canonical byte count.
        limit: usize,
    },
    /// A supplemental journal or card could not be decoded.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Result returned by portable storage operations.
pub type Result<T> = std::result::Result<T, StoreError>;

/// An authorized tenant and project namespace; authorization is supplied by callers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "ScopeInput")]
pub struct Scope {
    tenant: String,
    project: String,
}

#[derive(Deserialize)]
struct ScopeInput {
    tenant: String,
    project: String,
}

impl TryFrom<ScopeInput> for Scope {
    type Error = StoreError;
    fn try_from(value: ScopeInput) -> Result<Self> {
        Self::new(value.tenant, value.project)
    }
}

impl Scope {
    /// Validate two ordinary namespace segments.
    pub fn new(tenant: impl Into<String>, project: impl Into<String>) -> Result<Self> {
        let scope = Self {
            tenant: tenant.into(),
            project: project.into(),
        };
        for segment in [&scope.tenant, &scope.project] {
            if segment.is_empty()
                || segment.len() > 128
                || segment == "."
                || segment == ".."
                || !segment
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"-_.".contains(&c))
            {
                return Err(StoreError::Invalid("invalid scope segment".into()));
            }
        }
        if scope.tenant == "_virtuus" {
            return Err(StoreError::Invalid("reserved tenant namespace".into()));
        }
        Ok(scope)
    }

    /// Return the canonical S3 prefix without its trailing slash.
    pub fn namespace(&self) -> String {
        format!("{}/{}", self.tenant, self.project)
    }

    /// Tenant identifier used to validate an authorized project selection.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Project identifier within the tenant.
    pub fn project(&self) -> &str {
        &self.project
    }
}

/// Conditional publication; unconditional canonical writes are intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteCondition {
    /// Publish only when the path has no current object.
    Absent,
    /// Publish only when the current opaque revision matches.
    Revision(String),
}

/// Exact object bytes and backend-native revision metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredObject {
    /// Unmodified canonical bytes.
    pub bytes: Vec<u8>,
    /// Opaque ETag in S3, content hash on the filesystem.
    pub revision: String,
    /// Supplemental idempotency identifier, never injected into canonical JSON.
    pub operation_id: Option<String>,
    /// Object content type.
    pub content_type: String,
}

/// A bounded, lexically ordered page of canonical relative paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPage {
    /// Paths relative to the selected project root.
    pub paths: Vec<String>,
    /// Exclusive last-path cursor, usable only with the same scope and prefix.
    pub next_cursor: Option<String>,
}

/// Conditional object access with control objects isolated from canonical exports.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Fetch a canonical file without changing bytes.
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<StoredObject>>;
    /// Read with an explicit byte budget, bounding body reads and allocation before
    /// returning bytes. Wrappers must forward the bounded method, never call unbounded get.
    async fn get_bounded(
        &self,
        scope: &Scope,
        path: &str,
        max_bytes: usize,
    ) -> Result<Option<StoredObject>>;
    /// List a bounded page of canonical paths.
    async fn list(
        &self,
        scope: &Scope,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ObjectPage>;
    /// Atomically replace one canonical object if its condition holds.
    /// Return `Unavailable` only for proven rejection before application. Timeouts,
    /// ambiguous service failures, or possibly applied IO failures must retain
    /// uncertainty (`Uncertain` or `Io`), so callers cannot blindly republish.
    async fn put(
        &self,
        scope: &Scope,
        path: &str,
        bytes: &[u8],
        content_type: &str,
        condition: WriteCondition,
        operation_id: Option<&str>,
    ) -> Result<StoredObject>;
    /// Delete one canonical file only if its current revision matches.
    /// The same definite-rejection versus ambiguous-outcome contract as `put` applies.
    async fn delete(&self, scope: &Scope, path: &str, expected_revision: &str) -> Result<()>;
    /// Fetch supplemental state outside the canonical project prefix.
    async fn get_control(&self, scope: &Scope, path: &str) -> Result<Option<StoredObject>>;
    /// Conditionally write supplemental state outside the canonical project prefix.
    async fn put_control(
        &self,
        scope: &Scope,
        path: &str,
        bytes: &[u8],
        condition: WriteCondition,
    ) -> Result<StoredObject>;
    /// List a bounded, lexically ordered page of supplemental control paths.
    /// Paging follows `list`: the cursor is the exclusive last path of the prior page.
    async fn list_control(
        &self,
        scope: &Scope,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ObjectPage>;
    /// Delete one supplemental control object only if its current revision matches.
    /// The same definite-rejection versus ambiguous-outcome contract as `delete` applies.
    async fn delete_control(
        &self,
        scope: &Scope,
        path: &str,
        expected_revision: &str,
    ) -> Result<()>;
}

/// Rebuildable card metadata. Canonical content never belongs here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Card {
    /// Canonical issue path, also the scoped catalog sort key.
    pub path: String,
    /// Revision of the canonical object used to derive this card.
    pub revision: String,
    /// SHA-256 of the original bytes.
    pub hash: String,
    /// Original byte count.
    pub byte_length: u64,
    /// Generic scalar query fields such as type, status, assignee, custom.region.
    pub fields: BTreeMap<String, Value>,
    /// Configured scalar fields omitted because bounded supplemental metadata cannot hold them.
    #[serde(default)]
    pub omitted_fields: Vec<String>,
}

/// A targeted board candidate query within one authorized project.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogQuery {
    /// Selected issue types; empty means all types.
    pub issue_types: Vec<String>,
    /// Selected statuses; empty means all statuses.
    pub statuses: Vec<String>,
}

/// A bounded deterministic card page and observable query cost.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogPage {
    /// Cards ordered by canonical path.
    pub cards: Vec<Card>,
    /// Exclusive last-path cursor for the same scope and query.
    pub next_cursor: Option<String>,
    /// Backend requests needed for this page.
    pub requests: usize,
    /// Candidate rows read by the backend, before residual predicates.
    pub candidates: usize,
}

/// Supplemental catalog which can always be rebuilt from canonical objects.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Fetch one card strongly consistently when the backend supports it.
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<Card>>;
    /// Project a card only if the prior catalog revision still matches.
    async fn put(&self, scope: &Scope, card: &Card, condition: WriteCondition) -> Result<()>;
    /// Remove a stale projection only if its revision still matches.
    async fn delete(&self, scope: &Scope, path: &str, expected_revision: &str) -> Result<()>;
    /// Read targeted type/status candidates using bounded project-scoped queries.
    async fn query(
        &self,
        scope: &Scope,
        query: &CatalogQuery,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage>;
}

/// An immutable canonical event or attachment to publish after an issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventObject {
    /// Ordinary canonical relative path.
    pub path: String,
    /// Unmodified bytes.
    pub bytes: Vec<u8>,
    /// MIME content type.
    pub content_type: String,
}

/// One logical mutation with explicit cross-object repair semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mutation {
    /// Canonical issue path.
    pub path: String,
    /// Caller-supplied create or expected revision condition.
    pub condition: WriteCondition,
    /// Raw canonical issue bytes, retained exactly.
    pub bytes: Vec<u8>,
    /// MIME content type.
    pub content_type: String,
    /// Immutable canonical history and related files.
    pub event_objects: Vec<EventObject>,
    /// Unique retry identifier within the project scope.
    pub operation_id: String,
    /// Digest of the caller request before generated timestamps or identifiers.
    pub request_hash: String,
    /// Generic scalar card fields derived separately from raw bytes.
    pub fields: BTreeMap<String, Value>,
}

/// Publication outcome; pending repair is never reported as an atomic transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    /// The object published by this operation, even if subsequently superseded.
    pub stored: StoredObject,
    /// Events or supplemental catalog still need a retry to converge.
    pub repair_pending: bool,
}

/// Conditional deletion with immutable history and durable retry identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteMutation {
    /// Ordinary canonical issue path.
    pub path: String,
    /// Revision which must still be current before deletion.
    pub expected_revision: String,
    /// Immutable deletion history and related files.
    pub event_objects: Vec<EventObject>,
    /// Unique retry identifier shared with create and update operations.
    pub operation_id: String,
    /// Caller request digest, before generated fields.
    pub request_hash: String,
}

/// A completed conditional deletion with repairable follow-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteResult {
    /// History or catalog convergence still needs a retry.
    pub repair_pending: bool,
}

/// Counts of what one completed scope purge removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeResult {
    /// Canonical objects deleted from the scope, including history and attachments.
    pub canonical_objects_deleted: usize,
    /// Catalog rows deleted from the scope.
    pub catalog_rows_deleted: usize,
    /// Control objects deleted, excluding the purge intent and purge receipt.
    pub control_objects_deleted: usize,
}

/// Validate a portable relative file path.
pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > 1024
        || path.contains(['\\', '\0'])
        || path
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
    {
        return Err(StoreError::Invalid("invalid relative path".into()));
    }
    Ok(())
}

/// Validate a list prefix, allowing the project root and a trailing slash.
pub fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        Ok(())
    } else {
        validate_path(prefix.trim_end_matches('/'))
    }
}

/// Bound page sizes so a caller cannot accidentally request an unbounded scan.
pub fn validate_limit(limit: usize) -> Result<()> {
    if (1..=1000).contains(&limit) {
        Ok(())
    } else {
        Err(StoreError::Invalid(
            "limit must be between 1 and 1000".into(),
        ))
    }
}

/// Compute a raw-byte SHA-256, independent of JSON formatting.
pub fn content_hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Validate scalar-only catalog metadata independently of canonical payloads.
pub fn validate_fields(fields: &BTreeMap<String, Value>) -> Result<()> {
    if fields.len() > 64
        || fields.iter().any(|(key, value)| {
            key.is_empty() || key.len() > 128 || value.is_array() || value.is_object()
        })
        || serde_json::to_vec(fields)?.len() > 16_384
    {
        return Err(StoreError::Invalid(
            "catalog fields must be small named scalars".into(),
        ));
    }
    Ok(())
}

fn validate_omitted_fields(fields: &[String]) -> Result<()> {
    if fields.len() > 256
        || fields
            .iter()
            .any(|field| field.is_empty() || field.len() > 128)
    {
        return Err(StoreError::Invalid(
            "omitted field metadata must contain bounded named paths".into(),
        ));
    }
    Ok(())
}
