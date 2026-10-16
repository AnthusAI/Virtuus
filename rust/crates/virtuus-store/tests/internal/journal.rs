use super::*;
use crate::{FileStore, MemoryCatalog};
use async_trait::async_trait;
use std::sync::Mutex;

struct InterruptedCatalog {
    inner: MemoryCatalog,
    put_failure: Option<Failure>,
    delete_failure: Option<Failure>,
}

#[tokio::test]
async fn projection_rechecks_the_canonical_revision_after_catalog_publication() {
    let (_root, objects, store, scope) = fixture();
    let path = "project/issues/a.json";
    let object = objects
        .inner
        .put(
            &scope,
            path,
            b"{}",
            "application/json",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    objects.hook("get", "project/issues/", 1, Action::Reply(None));
    store.project_current(&scope, path, &[]).await.unwrap();
    assert_eq!(
        store
            .catalog
            .get(&scope, path)
            .await
            .unwrap()
            .unwrap()
            .revision,
        object.revision
    );
}

#[async_trait]
impl Catalog for InterruptedCatalog {
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<Card>> {
        self.inner.get(scope, path).await
    }
    async fn put(&self, scope: &Scope, card: &Card, condition: WriteCondition) -> Result<()> {
        if let Some(failure) = self.put_failure {
            return Err(failure.error());
        }
        self.inner.put(scope, card, condition).await
    }
    async fn delete(&self, scope: &Scope, path: &str, revision: &str) -> Result<()> {
        if let Some(failure) = self.delete_failure {
            return Err(failure.error());
        }
        self.inner.delete(scope, path, revision).await
    }
    async fn query(
        &self,
        scope: &Scope,
        query: &CatalogQuery,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage> {
        self.inner.query(scope, query, cursor, limit).await
    }
}

#[tokio::test]
async fn projection_failures_and_continuous_catalog_races_are_bounded_and_retryable() {
    for failure in [Failure::Conflict, Failure::Unavailable] {
        let (_root, objects, _store, scope) = fixture();
        objects
            .inner
            .put(
                &scope,
                "project/issues/a.json",
                b"{}",
                "application/json",
                WriteCondition::Absent,
                None,
            )
            .await
            .unwrap();
        let catalog = Arc::new(InterruptedCatalog {
            inner: MemoryCatalog::default(),
            put_failure: Some(failure),
            delete_failure: None,
        });
        let store = JournaledStore::new(objects.clone(), catalog);
        assert!(matches!(
            store
                .project_current(&scope, "project/issues/a.json", &[])
                .await,
            Err(StoreError::Unavailable(_))
        ));
        assert_eq!(
            objects
                .inner
                .get(&scope, "project/issues/a.json")
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"{}"
        );
    }
    for failure in [Failure::Conflict, Failure::Unavailable] {
        let (_root, objects, _store, scope) = fixture();
        let catalog = Arc::new(InterruptedCatalog {
            inner: MemoryCatalog::default(),
            put_failure: None,
            delete_failure: Some(failure),
        });
        let card = Card {
            path: "project/issues/a.json".into(),
            revision: "old".into(),
            hash: "hash".into(),
            byte_length: 2,
            fields: BTreeMap::new(),
            omitted_fields: vec![],
        };
        catalog
            .inner
            .put(&scope, &card, WriteCondition::Absent)
            .await
            .unwrap();
        let store = JournaledStore::new(objects, catalog.clone());
        assert!(matches!(
            store.project_current(&scope, &card.path, &[]).await,
            Err(StoreError::Unavailable(_))
        ));
        assert_eq!(
            catalog
                .inner
                .get(&scope, &card.path)
                .await
                .unwrap()
                .unwrap()
                .revision,
            "old"
        );
    }
}

#[tokio::test]
async fn rebuild_follows_object_and_catalog_pages_and_skips_unrelated_paths() {
    let (_root, objects, store, scope) = fixture();
    for i in 0..101 {
        let path = format!("project/issues/{i:03}.json");
        objects
            .inner
            .put(
                &scope,
                &path,
                b"{}",
                "application/json",
                WriteCondition::Absent,
                None,
            )
            .await
            .unwrap();
    }
    objects
        .inner
        .put(
            &scope,
            "project/issues/README.md",
            b"ordinary file",
            "text/plain",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    let unrelated = Card {
        path: "other/outside.json".into(),
        revision: "other".into(),
        hash: "hash".into(),
        byte_length: 2,
        fields: BTreeMap::new(),
        omitted_fields: vec![],
    };
    store
        .catalog
        .put(&scope, &unrelated, WriteCondition::Absent)
        .await
        .unwrap();
    assert_eq!(
        store
            .rebuild_catalog(&scope, "project/issues/")
            .await
            .unwrap(),
        101
    );
    assert_eq!(
        store
            .catalog
            .get(&scope, &unrelated.path)
            .await
            .unwrap()
            .unwrap()
            .revision,
        "other"
    );
    assert!(store
        .catalog
        .get(&scope, "project/issues/100.json")
        .await
        .unwrap()
        .is_some());
}

#[derive(Clone, Copy)]
enum Failure {
    Conflict,
    Unavailable,
}
impl Failure {
    fn error(self) -> StoreError {
        match self {
            Self::Conflict => StoreError::Conflict,
            Self::Unavailable => StoreError::Unavailable("synthetic interruption".into()),
        }
    }
}
enum Action {
    Fail(Failure),
    Reply(Option<Vec<u8>>),
    AppliedThenError,
    OtherWriter,
}
struct Hook {
    method: &'static str,
    prefix: &'static str,
    skip: usize,
    action: Action,
}
struct InterruptedStore {
    inner: FileStore,
    hooks: Mutex<Vec<Hook>>,
}
impl InterruptedStore {
    fn hook(&self, method: &'static str, prefix: &'static str, skip: usize, action: Action) {
        self.hooks.lock().unwrap().push(Hook {
            method,
            prefix,
            skip,
            action,
        });
    }
    fn take(&self, method: &str, path: &str) -> Option<Action> {
        let mut hooks = self.hooks.lock().unwrap();
        let index = hooks
            .iter()
            .position(|hook| hook.method == method && path.starts_with(hook.prefix))?;
        if hooks[index].skip > 0 {
            hooks[index].skip -= 1;
            return None;
        }
        Some(hooks.remove(index).action)
    }
}
fn stored(bytes: Vec<u8>) -> StoredObject {
    StoredObject {
        revision: content_hash(&bytes),
        bytes,
        operation_id: None,
        content_type: "application/json".into(),
    }
}
#[async_trait]
impl ObjectStore for InterruptedStore {
    async fn get(&self, s: &Scope, p: &str) -> Result<Option<StoredObject>> {
        match self.take("get", p) {
            Some(Action::Fail(f)) => Err(f.error()),
            Some(Action::Reply(v)) => Ok(v.map(stored)),
            _ => self.inner.get(s, p).await,
        }
    }
    async fn get_bounded(&self, s: &Scope, p: &str, l: usize) -> Result<Option<StoredObject>> {
        self.inner.get_bounded(s, p, l).await
    }
    async fn list(&self, s: &Scope, p: &str, c: Option<&str>, l: usize) -> Result<ObjectPage> {
        self.inner.list(s, p, c, l).await
    }
    async fn put(
        &self,
        s: &Scope,
        p: &str,
        b: &[u8],
        t: &str,
        c: WriteCondition,
        o: Option<&str>,
    ) -> Result<StoredObject> {
        match self.take("put", p) {
            Some(Action::Fail(f)) => Err(f.error()),
            Some(Action::AppliedThenError) => {
                self.inner.put(s, p, b, t, c, o).await?;
                Err(StoreError::Unavailable(
                    "lost response after publication".into(),
                ))
            }
            Some(Action::OtherWriter) => {
                self.inner
                    .put(s, p, b"{}", t, WriteCondition::Absent, Some("other-writer"))
                    .await?;
                Err(StoreError::Conflict)
            }
            _ => self.inner.put(s, p, b, t, c, o).await,
        }
    }
    async fn delete(&self, s: &Scope, p: &str, r: &str) -> Result<()> {
        match self.take("delete", p) {
            Some(Action::Fail(failure)) => Err(failure.error()),
            Some(Action::AppliedThenError) => {
                self.inner.delete(s, p, r).await?;
                Err(StoreError::Unavailable("lost deletion response".into()))
            }
            _ => self.inner.delete(s, p, r).await,
        }
    }
    async fn get_control(&self, s: &Scope, p: &str) -> Result<Option<StoredObject>> {
        match self.take("get_control", p) {
            Some(Action::Fail(f)) => Err(f.error()),
            Some(Action::Reply(v)) => Ok(v.map(stored)),
            _ => self.inner.get_control(s, p).await,
        }
    }
    async fn put_control(
        &self,
        s: &Scope,
        p: &str,
        b: &[u8],
        c: WriteCondition,
    ) -> Result<StoredObject> {
        match self.take("put_control", p) {
            Some(Action::Fail(f)) => Err(f.error()),
            Some(Action::AppliedThenError) => {
                self.inner.put_control(s, p, b, c).await?;
                Err(StoreError::Unavailable("lost claim response".into()))
            }
            _ => self.inner.put_control(s, p, b, c).await,
        }
    }
}

#[tokio::test]
async fn receipts_completed_by_another_worker_win_during_publication_recovery() {
    let (_root, objects, store, scope) = fixture();
    let write = mutation("write");
    let intent = Intent::Write(write.clone());
    control(
        &objects,
        &scope,
        &format!("attempts/{}.json", content_hash(b"write")),
        b"publishing",
    )
    .await;
    objects.hook(
        "get_control",
        "receipts/",
        1,
        Action::Reply(Some(serde_json::to_vec(&Receipt::Conflict).unwrap())),
    );
    assert!(matches!(
        store.publish_owned(&scope, &intent).await.unwrap(),
        Receipt::Conflict
    ));
    assert!(objects
        .inner
        .get(&scope, &write.path)
        .await
        .unwrap()
        .is_none());

    let (_root, objects, store, scope) = fixture();
    objects.hook(
        "put",
        "project/issues/",
        0,
        Action::Fail(Failure::Unavailable),
    );
    objects.hook(
        "get_control",
        "receipts/",
        2,
        Action::Reply(Some(serde_json::to_vec(&Receipt::Conflict).unwrap())),
    );
    assert!(matches!(
        store.publish_owned(&scope, &intent).await.unwrap(),
        Receipt::Conflict
    ));
    assert!(objects
        .inner
        .get(&scope, &write.path)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn rejected_delete_keeps_its_canonical_object_and_can_retry_the_same_operation() {
    let (_root, objects, store, scope) = fixture();
    let write = store.commit(&scope, &mutation("write")).await.unwrap();
    let delete = deletion("delete", &write.stored.revision);
    objects.hook(
        "delete",
        "project/issues/",
        0,
        Action::Fail(Failure::Unavailable),
    );
    assert!(matches!(
        store.delete(&scope, &delete).await,
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(
        objects
            .inner
            .get(&scope, &delete.path)
            .await
            .unwrap()
            .unwrap()
            .revision,
        write.stored.revision
    );
    assert!(!store.delete(&scope, &delete).await.unwrap().repair_pending);
}
fn fixture() -> (
    tempfile::TempDir,
    Arc<InterruptedStore>,
    JournaledStore,
    Scope,
) {
    let root = tempfile::tempdir().unwrap();
    let objects = Arc::new(InterruptedStore {
        inner: FileStore::new(root.path()).unwrap(),
        hooks: Mutex::new(vec![]),
    });
    let store = JournaledStore::new(objects.clone(), Arc::new(MemoryCatalog::default()));
    (root, objects, store, Scope::new("brand", "board").unwrap())
}
fn mutation(id: &str) -> Mutation {
    Mutation {
        operation_id: id.into(),
        request_hash: format!("request-{id}"),
        path: "project/issues/a.json".into(),
        bytes: br#"{"type":"task","status":"open"}"#.to_vec(),
        content_type: "application/json".into(),
        condition: WriteCondition::Absent,
        event_objects: vec![],
        fields: BTreeMap::new(),
    }
}
fn deletion(id: &str, revision: &str) -> DeleteMutation {
    DeleteMutation {
        operation_id: id.into(),
        request_hash: format!("request-{id}"),
        path: "project/issues/a.json".into(),
        expected_revision: revision.into(),
        event_objects: vec![],
    }
}
async fn control(objects: &InterruptedStore, scope: &Scope, path: &str, bytes: &[u8]) {
    objects
        .inner
        .put_control(scope, path, bytes, WriteCondition::Absent)
        .await
        .unwrap();
}

#[tokio::test]
async fn deletion_replay_is_bound_to_its_original_operation_kind_and_request() {
    let (_root, _objects, store, scope) = fixture();
    let write = mutation("write");
    assert!(store
        .operation(&scope, "missing", "hash")
        .await
        .unwrap()
        .is_none());
    assert!(store
        .deletion_operation(&scope, "missing", "hash")
        .await
        .unwrap()
        .is_none());
    let committed = store.commit(&scope, &write).await.unwrap();
    let deleted = deletion("delete", &committed.stored.revision);
    store.delete(&scope, &deleted).await.unwrap();
    assert!(
        !store
            .deletion_operation(&scope, "delete", &deleted.request_hash)
            .await
            .unwrap()
            .unwrap()
            .repair_pending
    );
    assert!(matches!(
        store
            .operation(&scope, "delete", &deleted.request_hash)
            .await,
        Err(StoreError::OperationMismatch)
    ));
    assert!(matches!(
        store
            .deletion_operation(&scope, "write", &write.request_hash)
            .await,
        Err(StoreError::OperationMismatch)
    ));
    assert!(matches!(
        store.deletion_operation(&scope, "delete", "other").await,
        Err(StoreError::OperationMismatch)
    ));
}

#[tokio::test]
async fn the_same_request_hash_cannot_change_a_persisted_command_kind() {
    let (_root, _objects, store, scope) = fixture();
    let write = mutation("write");
    let committed = store.commit(&scope, &write).await.unwrap();
    let mut delete = deletion("write", &committed.stored.revision);
    delete.request_hash = write.request_hash.clone();
    assert!(matches!(
        store.delete(&scope, &delete).await,
        Err(StoreError::OperationMismatch)
    ));
    let delete = deletion("delete", &committed.stored.revision);
    store.delete(&scope, &delete).await.unwrap();
    let mut changed_kind = mutation("delete");
    changed_kind.request_hash = delete.request_hash;
    assert!(matches!(
        store.commit(&scope, &changed_kind).await,
        Err(StoreError::OperationMismatch)
    ));
}

#[tokio::test]
async fn corrupt_receipt_kinds_fail_closed_without_repeating_canonical_publication() {
    for receipt in [
        Receipt::Conflict,
        Receipt::Published(stored(b"{}".to_vec())),
    ] {
        let (_root, objects, store, scope) = fixture();
        let delete = deletion("delete", "revision");
        store
            .save_intent(&scope, Intent::Delete(delete.clone()))
            .await
            .unwrap();
        control(
            &objects,
            &scope,
            &receipt_path("delete"),
            &serde_json::to_vec(&receipt).unwrap(),
        )
        .await;
        assert!(store
            .deletion_operation(&scope, "delete", &delete.request_hash)
            .await
            .is_err());
        assert!(store.delete(&scope, &delete).await.is_err());
        assert!(objects
            .inner
            .get(&scope, &delete.path)
            .await
            .unwrap()
            .is_none());
    }
    let (_root, objects, store, scope) = fixture();
    let write = mutation("write");
    control(
        &objects,
        &scope,
        &receipt_path("write"),
        &serde_json::to_vec(&Receipt::Deleted).unwrap(),
    )
    .await;
    assert!(matches!(
        store.resume(&scope, &Intent::Write(write)).await,
        Err(StoreError::OperationMismatch)
    ));
}

#[tokio::test]
async fn incomplete_or_contended_execution_claims_never_publish_an_issue() {
    for prefix in ["intents/", "active/", "attempts/"] {
        let (_root, objects, store, scope) = fixture();
        objects.hook("put_control", prefix, 0, Action::Fail(Failure::Unavailable));
        assert!(matches!(
            store.commit(&scope, &mutation("write")).await,
            Err(StoreError::Unavailable(_))
        ));
        assert!(objects
            .inner
            .get(&scope, "project/issues/a.json")
            .await
            .unwrap()
            .is_none());
    }
    let (_root, objects, store, scope) = fixture();
    for _ in 0..16 {
        objects.hook("put_control", "active/", 0, Action::Fail(Failure::Conflict));
    }
    assert!(matches!(
        store.commit(&scope, &mutation("write")).await,
        Err(StoreError::Unavailable(_))
    ));
    let (_root, objects, store, scope) = fixture();
    objects.hook(
        "put_control",
        "attempts/",
        0,
        Action::Fail(Failure::Conflict),
    );
    assert!(matches!(
        store.commit(&scope, &mutation("write")).await,
        Err(StoreError::Uncertain(_))
    ));
    let (_root, objects, store, scope) = fixture();
    let write = mutation("write");
    control(
        &objects,
        &scope,
        &format!("active/{}.json", content_hash(write.path.as_bytes())),
        br#""missing-intent""#,
    )
    .await;
    assert!(matches!(
        store.commit(&scope, &write).await,
        Err(StoreError::Uncertain(_))
    ));
    let (_root, objects, store, scope) = fixture();
    control(
        &objects,
        &scope,
        &format!("attempts/{}.json", content_hash(b"write")),
        b"checking",
    )
    .await;
    assert!(matches!(
        store.commit(&scope, &mutation("write")).await,
        Err(StoreError::Uncertain(_))
    ));
}

#[tokio::test]
async fn interrupted_preflight_and_claim_writes_release_retryable_ownership() {
    for (method, prefix, skip, action) in [
        (
            "get_control",
            "receipts/",
            3,
            Action::Fail(Failure::Unavailable),
        ),
        ("put_control", "attempts/", 1, Action::AppliedThenError),
    ] {
        let (_root, objects, store, scope) = fixture();
        objects.hook(method, prefix, skip, action);
        assert!(matches!(
            store.commit(&scope, &mutation("write")).await,
            Err(StoreError::Unavailable(_))
        ));
        assert!(objects
            .inner
            .get(&scope, "project/issues/a.json")
            .await
            .unwrap()
            .is_none());
        assert!(
            !store
                .commit(&scope, &mutation("write"))
                .await
                .unwrap()
                .repair_pending
        );
    }
}

#[tokio::test]
async fn lost_success_responses_recover_the_canonical_write_and_delete() {
    let (_root, objects, store, scope) = fixture();
    objects.hook("put", "project/issues/", 0, Action::AppliedThenError);
    let committed = store.commit(&scope, &mutation("write")).await.unwrap();
    assert!(!committed.repair_pending);
    objects.hook("delete", "project/issues/", 0, Action::AppliedThenError);
    assert!(
        !store
            .delete(&scope, &deletion("delete", &committed.stored.revision))
            .await
            .unwrap()
            .repair_pending
    );
    assert!(objects
        .inner
        .get(&scope, "project/issues/a.json")
        .await
        .unwrap()
        .is_none());
    let (_root, objects, store, scope) = fixture();
    objects.hook("put", "project/issues/", 0, Action::OtherWriter);
    assert!(matches!(
        store.commit(&scope, &mutation("write")).await,
        Err(StoreError::Conflict)
    ));
    assert_eq!(
        objects
            .inner
            .get(&scope, "project/issues/a.json")
            .await
            .unwrap()
            .unwrap()
            .operation_id
            .as_deref(),
        Some("other-writer")
    );
}

#[tokio::test]
async fn receipts_arriving_during_retry_and_corrupt_history_are_handled_deterministically() {
    let (_root, objects, store, scope) = fixture();
    let write = mutation("write");
    let receipt = serde_json::to_vec(&Receipt::Published(stored(write.bytes.clone()))).unwrap();
    objects.hook(
        "get_control",
        "receipts/",
        1,
        Action::Reply(Some(receipt.clone())),
    );
    assert!(matches!(
        store
            .ensure_receipt(&scope, &Intent::Write(write.clone()))
            .await
            .unwrap(),
        Receipt::Published(_)
    ));
    objects.hook(
        "get_control",
        "receipts/",
        0,
        Action::Reply(Some(receipt.clone())),
    );
    assert!(matches!(
        store
            .publish_owned(&scope, &Intent::Write(write.clone()))
            .await
            .unwrap(),
        Receipt::Published(_)
    ));
    objects.hook("get_control", "receipts/", 1, Action::Reply(Some(receipt)));
    assert!(matches!(
        store
            .publish_owned(&scope, &Intent::Write(write.clone()))
            .await
            .unwrap(),
        Receipt::Published(_)
    ));
    store
        .save_receipt(&scope, "saved", Receipt::Deleted)
        .await
        .unwrap();
    assert!(matches!(
        store
            .save_receipt(&scope, "saved", Receipt::Conflict)
            .await
            .unwrap(),
        Receipt::Deleted
    ));
    objects.hook("get_control", "receipts/", 0, Action::Reply(None));
    assert!(matches!(
        store.save_receipt(&scope, "saved", Receipt::Conflict).await,
        Err(StoreError::Uncertain(_))
    ));
    let (_root, objects, store, scope) = fixture();
    let mut write = mutation("history");
    write.event_objects.push(EventObject {
        path: "project/events/event.json".into(),
        bytes: b"expected".to_vec(),
        content_type: "application/json".into(),
    });
    objects
        .inner
        .put(
            &scope,
            "project/events/event.json",
            b"different",
            "application/json",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    assert!(store.commit(&scope, &write).await.unwrap().repair_pending);
}

#[tokio::test]
async fn mutation_validation_and_import_projection_limits_apply_before_changes() {
    let (_root, objects, store, scope) = fixture();
    assert!(store.clone().with_canonical_read_limit(0).is_err());
    let mut write = mutation("write");
    write.request_hash.clear();
    assert!(store.commit(&scope, &write).await.is_err());
    write.request_hash = "hash".into();
    write.operation_id = "x".repeat(257);
    assert!(store.commit(&scope, &write).await.is_err());
    write.operation_id = "write".into();
    write.event_objects.push(EventObject {
        path: write.path.clone(),
        bytes: vec![],
        content_type: "application/json".into(),
    });
    assert!(store.commit(&scope, &write).await.is_err());
    assert!(store.delete(&scope, &deletion("delete", "")).await.is_err());
    assert!(derive_projection(b"{}", &[String::new()]).is_err());
    assert!(objects
        .inner
        .get(&scope, &write.path)
        .await
        .unwrap()
        .is_none());
}
