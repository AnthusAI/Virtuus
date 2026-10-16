use crate::{
    content_hash, validate_fields, validate_path, Card, Catalog, CatalogPage, CatalogQuery,
    CommitResult, DeleteMutation, DeleteResult, EventObject, Mutation, ObjectPage, ObjectStore,
    Result, Scope, StoreError, StoredObject, WriteCondition,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const DEFAULT_FIELDS: &[&str] = &[
    "type",
    "status",
    "id",
    "identifier",
    "assignee",
    "priority",
    "parent",
    "updated_at",
    "title",
];

#[derive(Serialize, Deserialize)]
enum Receipt {
    Published(StoredObject),
    Deleted,
    Conflict,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", content = "mutation")]
enum Intent {
    Write(Mutation),
    Delete(DeleteMutation),
}

impl Intent {
    fn path(&self) -> &str {
        match self {
            Self::Write(m) => &m.path,
            Self::Delete(m) => &m.path,
        }
    }
    fn operation_id(&self) -> &str {
        match self {
            Self::Write(m) => &m.operation_id,
            Self::Delete(m) => &m.operation_id,
        }
    }
    fn request_hash(&self) -> &str {
        match self {
            Self::Write(m) => &m.request_hash,
            Self::Delete(m) => &m.request_hash,
        }
    }
    fn events(&self) -> &[EventObject] {
        match self {
            Self::Write(m) => &m.event_objects,
            Self::Delete(m) => &m.event_objects,
        }
    }
    fn matches_current(&self, current: &Option<StoredObject>) -> bool {
        match self {
            Self::Write(m) => match &m.condition {
                WriteCondition::Absent => current.is_none(),
                WriteCondition::Revision(r) => current.as_ref().is_some_and(|o| &o.revision == r),
            },
            Self::Delete(m) => current
                .as_ref()
                .is_some_and(|o| o.revision == m.expected_revision),
        }
    }
}

/// Coordinate conditional issue publication and repairable event/catalog follow-up.
/// The separate control journal is durable; publication across objects is not atomic.
#[derive(Clone)]
pub struct JournaledStore {
    objects: Arc<dyn ObjectStore>,
    catalog: Arc<dyn Catalog>,
    canonical_read_limit: Option<usize>,
}

impl JournaledStore {
    /// Combine canonical object storage and an independently rebuildable catalog.
    pub fn new(objects: Arc<dyn ObjectStore>, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            objects,
            catalog,
            canonical_read_limit: None,
        }
    }

    /// Opt in to a canonical byte budget for every public and internal read, including replay repair.
    /// The default remains unlimited for trusted import/export administration.
    pub fn with_canonical_read_limit(mut self, max_bytes: usize) -> Result<Self> {
        if max_bytes == 0 {
            return Err(StoreError::Invalid(
                "canonical read limit must be positive".into(),
            ));
        }
        self.canonical_read_limit = Some(max_bytes);
        Ok(self)
    }

    async fn read_canonical(
        &self,
        scope: &Scope,
        path: &str,
    ) -> Result<Option<crate::StoredObject>> {
        match self.canonical_read_limit {
            Some(limit) => self.objects.get_bounded(scope, path, limit).await,
            None => self.objects.get(scope, path).await,
        }
    }

    /// Read unmodified canonical bytes directly from the selected backend.
    pub async fn get(&self, scope: &Scope, path: &str) -> Result<Option<StoredObject>> {
        self.read_canonical(scope, path).await
    }

    /// Read canonical bytes within an explicit caller byte budget.
    pub async fn get_bounded(
        &self,
        scope: &Scope,
        path: &str,
        max_bytes: usize,
    ) -> Result<Option<StoredObject>> {
        self.objects
            .get_bounded(
                scope,
                path,
                self.canonical_read_limit
                    .map_or(max_bytes, |limit| limit.min(max_bytes)),
            )
            .await
    }

    /// Enumerate ordinary canonical paths, excluding all supplemental journal files.
    pub async fn list(
        &self,
        scope: &Scope,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ObjectPage> {
        self.objects.list(scope, prefix, cursor, limit).await
    }

    /// Query generic type/status card metadata within one authorized project.
    pub async fn query(
        &self,
        scope: &Scope,
        query: &CatalogQuery,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage> {
        self.catalog.query(scope, query, cursor, limit).await
    }

    /// Query candidates for selected types; callers may hydrate and apply residual predicates.
    pub async fn query_candidates(
        &self,
        scope: &Scope,
        issue_types: &[String],
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage> {
        self.query(
            scope,
            &CatalogQuery {
                issue_types: issue_types.to_vec(),
                statuses: Vec::new(),
            },
            cursor,
            limit,
        )
        .await
    }

    /// Look up and repair a prior command before generating new identifiers or timestamps.
    /// A different caller request digest is rejected even when its operation id matches.
    pub async fn operation(
        &self,
        scope: &Scope,
        operation_id: &str,
        request_hash: &str,
    ) -> Result<Option<CommitResult>> {
        let path = intent_path(operation_id)?;
        let Some(intent) = self.objects.get_control(scope, &path).await? else {
            return Ok(None);
        };
        let intent: Intent = serde_json::from_slice(&intent.bytes)?;
        if intent.request_hash() != request_hash {
            return Err(StoreError::OperationMismatch);
        }
        match intent {
            Intent::Write(_) => self.resume(scope, &intent).await.map(Some),
            Intent::Delete(_) => Err(StoreError::OperationMismatch),
        }
    }

    /// Replay a prior deletion before requiring the now-absent issue or generating history.
    pub async fn deletion_operation(
        &self,
        scope: &Scope,
        operation_id: &str,
        request_hash: &str,
    ) -> Result<Option<DeleteResult>> {
        let Some(object) = self
            .objects
            .get_control(scope, &intent_path(operation_id)?)
            .await?
        else {
            return Ok(None);
        };
        let intent: Intent = serde_json::from_slice(&object.bytes)?;
        if intent.request_hash() != request_hash || !matches!(intent, Intent::Delete(_)) {
            return Err(StoreError::OperationMismatch);
        }
        match self.ensure_receipt(scope, &intent).await? {
            Receipt::Deleted => Ok(Some(DeleteResult {
                repair_pending: self.repair(scope, &intent).await,
            })),
            Receipt::Conflict => Err(StoreError::Conflict),
            Receipt::Published(_) => Err(StoreError::OperationMismatch),
        }
    }

    /// Persist intent before issue CAS, then publish immutable history and a derived card.
    pub async fn commit(&self, scope: &Scope, mutation: &Mutation) -> Result<CommitResult> {
        validate_mutation(mutation)?;
        let intent = self
            .save_intent(scope, Intent::Write(mutation.clone()))
            .await?;
        self.resume(scope, &intent).await
    }

    /// Delete with a durable preflight witness and repairable history/catalog follow-up.
    /// Cooperating journaled writers serialize publication; direct imports must be offline.
    pub async fn delete(&self, scope: &Scope, mutation: &DeleteMutation) -> Result<DeleteResult> {
        validate_path(&mutation.path)?;
        validate_identity(&mutation.operation_id, &mutation.request_hash)?;
        validate_events(&mutation.path, &mutation.event_objects)?;
        if mutation.expected_revision.is_empty() {
            return Err(StoreError::Invalid("delete requires a revision".into()));
        }
        let intent = self
            .save_intent(scope, Intent::Delete(mutation.clone()))
            .await?;
        match self.ensure_receipt(scope, &intent).await? {
            Receipt::Deleted => {}
            Receipt::Conflict => return Err(StoreError::Conflict),
            Receipt::Published(_) => return Err(StoreError::OperationMismatch),
        }
        let repair_pending = self.repair(scope, &intent).await;
        Ok(DeleteResult { repair_pending })
    }

    async fn save_intent(&self, scope: &Scope, intent: Intent) -> Result<Intent> {
        let path = intent_path(intent.operation_id())?;
        match self
            .objects
            .put_control(
                scope,
                &path,
                &serde_json::to_vec(&intent)?,
                WriteCondition::Absent,
            )
            .await
        {
            Ok(_) => Ok(intent),
            Err(StoreError::Conflict) => {
                let prior = self
                    .objects
                    .get_control(scope, &path)
                    .await?
                    .ok_or_else(|| StoreError::Uncertain("intent disappeared".into()))?;
                let prior: Intent = serde_json::from_slice(&prior.bytes)?;
                if prior.request_hash() != intent.request_hash()
                    || std::mem::discriminant(&prior) != std::mem::discriminant(&intent)
                {
                    return Err(StoreError::OperationMismatch);
                }
                Ok(prior)
            }
            Err(error) => Err(error),
        }
    }

    async fn resume(&self, scope: &Scope, intent: &Intent) -> Result<CommitResult> {
        let stored = match self.ensure_receipt(scope, intent).await? {
            Receipt::Published(stored) => stored,
            Receipt::Conflict => return Err(StoreError::Conflict),
            Receipt::Deleted => return Err(StoreError::OperationMismatch),
        };
        Ok(CommitResult {
            stored,
            repair_pending: self.repair(scope, intent).await,
        })
    }

    async fn repair(&self, scope: &Scope, intent: &Intent) -> bool {
        let mut repair_pending = false;
        for event in intent.events() {
            match self
                .objects
                .put(
                    scope,
                    &event.path,
                    &event.bytes,
                    &event.content_type,
                    WriteCondition::Absent,
                    Some(intent.operation_id()),
                )
                .await
            {
                Ok(_) => {}
                Err(StoreError::Conflict) => {
                    if !matches!(self.read_canonical(scope, &event.path).await, Ok(Some(existing)) if existing.bytes == event.bytes)
                    {
                        repair_pending = true;
                    }
                }
                Err(_) => repair_pending = true,
            }
        }
        let mut field_paths = DEFAULT_FIELDS
            .iter()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>();
        if let Intent::Write(mutation) = intent {
            field_paths.extend(mutation.fields.keys().cloned());
        }
        if self
            .project_current(scope, intent.path(), &field_paths)
            .await
            .is_err()
        {
            repair_pending = true;
        }
        repair_pending
    }

    async fn receipt(&self, scope: &Scope, operation_id: &str) -> Result<Option<Receipt>> {
        self.objects
            .get_control(scope, &receipt_path(operation_id))
            .await?
            .map(|object| serde_json::from_slice(&object.bytes).map_err(StoreError::from))
            .transpose()
    }

    async fn ensure_receipt(&self, scope: &Scope, intent: &Intent) -> Result<Receipt> {
        if let Some(receipt) = self.receipt(scope, intent.operation_id()).await? {
            return Ok(receipt);
        }
        let active_path = format!("active/{}.json", content_hash(intent.path().as_bytes()));
        for _ in 0..16 {
            if let Some(receipt) = self.receipt(scope, intent.operation_id()).await? {
                return Ok(receipt);
            }
            let active = self.objects.get_control(scope, &active_path).await?;
            if let Some(active) = &active {
                let active_id: String = serde_json::from_slice(&active.bytes)?;
                if active_id == intent.operation_id() {
                    return self.publish_owned(scope, intent).await;
                }
                if self.receipt(scope, &active_id).await?.is_none() {
                    let prior = self
                        .objects
                        .get_control(scope, &intent_path(&active_id)?)
                        .await?
                        .ok_or_else(|| {
                            StoreError::Uncertain("active command has no intent".into())
                        })?;
                    let prior: Intent = serde_json::from_slice(&prior.bytes)?;
                    self.publish_owned(scope, &prior).await?;
                }
            }
            let condition = active
                .map(|value| WriteCondition::Revision(value.revision))
                .unwrap_or(WriteCondition::Absent);
            match self
                .objects
                .put_control(
                    scope,
                    &active_path,
                    &serde_json::to_vec(&intent.operation_id())?,
                    condition,
                )
                .await
            {
                Ok(_) => return self.publish_owned(scope, intent).await,
                Err(StoreError::Conflict) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(StoreError::Unavailable(
            "publication contention; retry the same operation".into(),
        ))
    }

    async fn publish_owned(&self, scope: &Scope, intent: &Intent) -> Result<Receipt> {
        if let Some(receipt) = self.receipt(scope, intent.operation_id()).await? {
            return Ok(receipt);
        }
        let attempt_path = format!(
            "attempts/{}.json",
            content_hash(intent.operation_id().as_bytes())
        );
        let attempt = self.objects.get_control(scope, &attempt_path).await?;
        if let Some(attempt) = &attempt {
            if attempt.bytes != b"retryable" {
                if attempt.bytes != b"publishing" {
                    return Err(StoreError::Uncertain(
                        "another execution owns this command; retry its receipt after it completes"
                            .into(),
                    ));
                }
                let current = self.read_canonical(scope, intent.path()).await?;
                let recovered = match intent {
                    Intent::Write(mutation) => current
                        .filter(|o| {
                            o.operation_id.as_deref() == Some(intent.operation_id())
                                && o.bytes == mutation.bytes
                        })
                        .map(Receipt::Published),
                    Intent::Delete(_) if current.is_none() => Some(Receipt::Deleted),
                    Intent::Delete(_) => None,
                };
                if let Some(receipt) = self.receipt(scope, intent.operation_id()).await? {
                    return Ok(receipt);
                }
                return match recovered {
                    Some(receipt) => self.save_receipt(scope, intent.operation_id(), receipt).await,
                    None => Err(StoreError::Uncertain("publication is owned by another execution and cannot yet be inferred; do not supersede this path".into())),
                };
            }
        }
        let condition = attempt
            .map(|a| WriteCondition::Revision(a.revision))
            .unwrap_or(WriteCondition::Absent);
        let checking = match self
            .objects
            .put_control(scope, &attempt_path, b"checking", condition)
            .await
        {
            Ok(attempt) => attempt,
            Err(StoreError::Conflict) => {
                return Err(StoreError::Uncertain(
                    "another execution acquired this command; retry after it completes".into(),
                ))
            }
            Err(error) => return Err(error),
        };
        match self.receipt(scope, intent.operation_id()).await {
            Ok(Some(receipt)) => return Ok(receipt),
            Ok(None) => {}
            Err(error) => {
                self.release_checking(scope, &attempt_path, &checking.revision)
                    .await?;
                return Err(error);
            }
        }
        let current = match self.read_canonical(scope, intent.path()).await {
            Ok(current) => current,
            Err(error) => {
                self.release_checking(scope, &attempt_path, &checking.revision)
                    .await?;
                return Err(error);
            }
        };
        if !intent.matches_current(&current) {
            let result = self
                .save_receipt(scope, intent.operation_id(), Receipt::Conflict)
                .await;
            if result.is_err() {
                self.release_checking(scope, &attempt_path, &checking.revision)
                    .await?;
            }
            return result;
        }
        let publishing = match self
            .objects
            .put_control(
                scope,
                &attempt_path,
                b"publishing",
                WriteCondition::Revision(checking.revision.clone()),
            )
            .await
        {
            Ok(publishing) => publishing,
            Err(error) => {
                let current = self
                    .objects
                    .get_control(scope, &attempt_path)
                    .await?
                    .ok_or_else(|| StoreError::Uncertain("execution claim disappeared".into()))?;
                self.release_checking(scope, &attempt_path, &current.revision)
                    .await?;
                return Err(error);
            }
        };
        let result = match intent {
            Intent::Write(mutation) => self
                .objects
                .put(
                    scope,
                    intent.path(),
                    &mutation.bytes,
                    &mutation.content_type,
                    mutation.condition.clone(),
                    Some(intent.operation_id()),
                )
                .await
                .map(Receipt::Published),
            Intent::Delete(mutation) => self
                .objects
                .delete(scope, intent.path(), &mutation.expected_revision)
                .await
                .map(|()| Receipt::Deleted),
        };
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Some(receipt) = self.receipt(scope, intent.operation_id()).await? {
                    return Ok(receipt);
                }
                let current = self.read_canonical(scope, intent.path()).await?;
                let recovered = match intent {
                    Intent::Write(mutation) => current
                        .filter(|o| {
                            o.operation_id.as_deref() == Some(intent.operation_id())
                                && o.bytes == mutation.bytes
                        })
                        .map(Receipt::Published),
                    Intent::Delete(_) if current.is_none() => Some(Receipt::Deleted),
                    Intent::Delete(_) => None,
                };
                if let Some(receipt) = recovered {
                    receipt
                } else if matches!(error, StoreError::Conflict) {
                    Receipt::Conflict
                } else if matches!(error, StoreError::Unavailable(_)) {
                    self.objects
                        .put_control(
                            scope,
                            &attempt_path,
                            b"retryable",
                            WriteCondition::Revision(publishing.revision),
                        )
                        .await?;
                    return Err(error);
                } else {
                    return Err(StoreError::Uncertain(format!(
                        "publication cannot be inferred; retain execution ownership: {error}"
                    )));
                }
            }
        };
        self.save_receipt(scope, intent.operation_id(), receipt)
            .await
    }

    async fn save_receipt(
        &self,
        scope: &Scope,
        operation_id: &str,
        receipt: Receipt,
    ) -> Result<Receipt> {
        match self
            .objects
            .put_control(
                scope,
                &receipt_path(operation_id),
                &serde_json::to_vec(&receipt)?,
                WriteCondition::Absent,
            )
            .await
        {
            Ok(_) => Ok(receipt),
            Err(StoreError::Conflict) => self
                .receipt(scope, operation_id)
                .await?
                .ok_or_else(|| StoreError::Uncertain("receipt disappeared".into())),
            Err(error) => Err(StoreError::Uncertain(format!(
                "canonical object may be changed; retry receipt: {error}"
            ))),
        }
    }

    async fn release_checking(&self, scope: &Scope, path: &str, revision: &str) -> Result<()> {
        self.objects
            .put_control(
                scope,
                path,
                b"retryable",
                WriteCondition::Revision(revision.into()),
            )
            .await?;
        Ok(())
    }

    /// Rebuild generic card metadata after an ordinary folder copy/import.
    pub async fn rebuild_catalog(&self, scope: &Scope, issue_prefix: &str) -> Result<usize> {
        let fields = DEFAULT_FIELDS
            .iter()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>();
        self.rebuild_catalog_with_fields(scope, issue_prefix, &fields)
            .await
    }

    /// Rebuild using configured generic scalar paths, without rewriting canonical bytes.
    pub async fn rebuild_catalog_with_fields(
        &self,
        scope: &Scope,
        issue_prefix: &str,
        field_paths: &[String],
    ) -> Result<usize> {
        let mut cursor = None;
        let mut count = 0;
        loop {
            let page = self
                .objects
                .list(scope, issue_prefix, cursor.as_deref(), 100)
                .await?;
            for path in page.paths {
                if path.ends_with(".json") {
                    self.project_current(scope, &path, field_paths).await?;
                    count += 1;
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let mut cursor = None;
        loop {
            let page = self
                .catalog
                .query(scope, &CatalogQuery::default(), cursor.as_deref(), 100)
                .await?;
            for card in page.cards {
                if card.path.starts_with(issue_prefix) {
                    self.project_current(scope, &card.path, field_paths).await?;
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(count);
            }
        }
    }

    async fn project_current(
        &self,
        scope: &Scope,
        path: &str,
        field_paths: &[String],
    ) -> Result<()> {
        for _ in 0..8 {
            let prior = self.catalog.get(scope, path).await?;
            let Some(object) = self.read_canonical(scope, path).await? else {
                if let Some(card) = prior {
                    match self.catalog.delete(scope, path, &card.revision).await {
                        Ok(_) | Err(StoreError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                }
                if self.read_canonical(scope, path).await?.is_none()
                    && self.catalog.get(scope, path).await?.is_none()
                {
                    return Ok(());
                }
                continue;
            };
            let mut configured_fields = field_paths.to_vec();
            if let Some(prior) = &prior {
                configured_fields.extend(prior.fields.keys().cloned());
                configured_fields.extend(prior.omitted_fields.iter().cloned());
            }
            let (fields, omitted_fields) = derive_projection(&object.bytes, &configured_fields)?;
            let card = Card {
                omitted_fields,
                path: path.into(),
                revision: object.revision.clone(),
                hash: content_hash(&object.bytes),
                byte_length: object.bytes.len() as u64,
                fields,
            };
            let condition = prior
                .map(|card| WriteCondition::Revision(card.revision))
                .unwrap_or(WriteCondition::Absent);
            match self.catalog.put(scope, &card, condition).await {
                Ok(_) => {
                    if self
                        .read_canonical(scope, path)
                        .await?
                        .is_some_and(|current| current.revision == object.revision)
                    {
                        return Ok(());
                    }
                }
                Err(StoreError::Conflict) => {}
                Err(error) => return Err(error),
            }
        }
        Err(StoreError::Unavailable(
            "catalog changed during projection; retry".into(),
        ))
    }
}

fn validate_mutation(mutation: &Mutation) -> Result<()> {
    validate_path(&mutation.path)?;
    validate_identity(&mutation.operation_id, &mutation.request_hash)?;
    validate_fields(&mutation.fields)?;
    validate_events(&mutation.path, &mutation.event_objects)
}

fn validate_identity(operation_id: &str, request_hash: &str) -> Result<()> {
    intent_path(operation_id)?;
    if request_hash.is_empty() || request_hash.len() > 256 {
        return Err(StoreError::Invalid(
            "request hash must be 1 to 256 bytes".into(),
        ));
    }
    Ok(())
}

fn validate_events(path: &str, events: &[EventObject]) -> Result<()> {
    let mut paths = BTreeSet::from([path]);
    for event in events {
        validate_path(&event.path)?;
        if !paths.insert(&event.path) {
            return Err(StoreError::Invalid(
                "mutation paths must be distinct".into(),
            ));
        }
    }
    Ok(())
}

fn intent_path(operation_id: &str) -> Result<String> {
    if operation_id.is_empty() || operation_id.len() > 256 {
        return Err(StoreError::Invalid(
            "operation id must be 1 to 256 bytes".into(),
        ));
    }
    Ok(format!(
        "intents/{}.json",
        content_hash(operation_id.as_bytes())
    ))
}

fn receipt_path(operation_id: &str) -> String {
    format!("receipts/{}.json", content_hash(operation_id.as_bytes()))
}

/// Derive configured dotted scalar query fields without reserializing source bytes.
fn derive_projection(
    bytes: &[u8],
    field_paths: &[String],
) -> Result<(BTreeMap<String, Value>, Vec<String>)> {
    if field_paths.len() > 256
        || field_paths
            .iter()
            .any(|path| path.is_empty() || path.len() > 128)
    {
        return Err(StoreError::Invalid(
            "configured projection paths must be bounded named fields".into(),
        ));
    }
    let value: Value = serde_json::from_slice(bytes)?;
    let mut fields = BTreeMap::new();
    let mut omitted_fields = Vec::new();
    let mut seen = BTreeSet::new();
    let prioritized = ["type", "status"]
        .iter()
        .filter_map(|key| field_paths.iter().find(|path| path.as_str() == *key))
        .chain(field_paths.iter());
    for path in prioritized {
        if !seen.insert(path) {
            continue;
        }
        let mut current = &value;
        let mut present = true;
        for segment in path.split('.') {
            if let Some(next) = current.get(segment) {
                current = next;
            } else {
                present = false;
                break;
            }
        }
        if !present || current.is_array() || current.is_object() {
            continue;
        }
        fields.insert(path.clone(), current.clone());
        if validate_fields(&fields).is_err() {
            fields.remove(path);
            omitted_fields.push(path.clone());
        }
    }
    Ok((fields, omitted_fields))
}

#[cfg(test)]
#[path = "../tests/internal/journal.rs"]
mod failure_tests;
