mod support;
use cucumber::{given, then, when, World};
use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};
use support::FaultStore;
use virtuus_store::{
    Card, Catalog, CatalogQuery, EventObject, FileStore, JournaledStore, MemoryCatalog, Mutation,
    ObjectStore, PurgeResult, Scope, StoreError, StoredObject, WriteCondition,
};

const PURGE_OPERATION_IDENTIFIER: &str = "purge-board";

#[derive(Debug, PartialEq)]
struct ScopeSnapshot {
    canonical: BTreeMap<String, StoredObject>,
    catalog: Vec<Card>,
    control: BTreeMap<String, StoredObject>,
}

#[derive(Default, World)]
struct StorageWorld {
    root: Option<tempfile::TempDir>,
    objects: Option<Arc<FaultStore>>,
    store: Option<JournaledStore>,
    scope: Option<Scope>,
    original: Option<Mutation>,
    current: Option<StoredObject>,
    newer: Option<Vec<u8>>,
    catalog: Option<Arc<MemoryCatalog>>,
    other_scope: Option<Scope>,
    other_before: Option<ScopeSnapshot>,
    purge_result: Option<PurgeResult>,
}
impl std::fmt::Debug for StorageWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageWorld").finish()
    }
}
impl StorageWorld {
    fn setup(&mut self) {
        let root = tempfile::tempdir().unwrap();
        let objects = Arc::new(FaultStore::new(Arc::new(
            FileStore::new(root.path()).unwrap(),
        )));
        let catalog = Arc::new(MemoryCatalog::default());
        self.store = Some(JournaledStore::new(objects.clone(), catalog.clone()));
        self.catalog = Some(catalog);
        self.objects = Some(objects);
        self.scope = Some(Scope::new("brand", "app").unwrap());
        self.other_scope = Some(Scope::new("brand", "other-app").unwrap());
        self.root = Some(root);
    }
    fn scope(&self) -> &Scope {
        self.scope.as_ref().unwrap()
    }
    fn store(&self) -> &JournaledStore {
        self.store.as_ref().unwrap()
    }
    async fn snapshot(&self, scope: &Scope) -> ScopeSnapshot {
        let objects = self.objects.as_ref().unwrap();
        let mut canonical = BTreeMap::new();
        for path in objects.list(scope, "", None, 1000).await.unwrap().paths {
            canonical.insert(
                path.clone(),
                objects.get(scope, &path).await.unwrap().unwrap(),
            );
        }
        let mut control = BTreeMap::new();
        for path in objects
            .list_control(scope, "", None, 1000)
            .await
            .unwrap()
            .paths
        {
            control.insert(
                path.clone(),
                objects.get_control(scope, &path).await.unwrap().unwrap(),
            );
        }
        let catalog = self
            .catalog
            .as_ref()
            .unwrap()
            .query(scope, &CatalogQuery::default(), None, 1000)
            .await
            .unwrap()
            .cards;
        ScopeSnapshot {
            canonical,
            catalog,
            control,
        }
    }
    fn purge_record_paths(&self) -> Vec<String> {
        use sha2::{Digest, Sha256};
        let digest = hex::encode(Sha256::digest(PURGE_OPERATION_IDENTIFIER.as_bytes()));
        vec![
            format!("purge/intents/{digest}.json"),
            format!("purge/receipts/{digest}.json"),
        ]
    }
    fn mutation(&self, id: &str, condition: WriteCondition, bytes: &[u8]) -> Mutation {
        Mutation {
            path: "project/issues/x.json".into(),
            condition,
            bytes: bytes.to_vec(),
            content_type: "application/json".into(),
            event_objects: vec![EventObject {
                path: format!("project/events/{id}.json"),
                bytes: b"{\"history\":true}\n".to_vec(),
                content_type: "application/json".into(),
            }],
            operation_id: id.into(),
            request_hash: id.into(),
            fields: BTreeMap::new(),
        }
    }
}
#[given("an ordinary project file with whitespace and unknown fields")]
async fn raw(w: &mut StorageWorld) {
    w.setup();
    w.original = Some(w.mutation(
        "create",
        WriteCondition::Absent,
        b"{ \"id\":\"x\", \"unknown\": [1, 2] }\n",
    ));
}
#[when("I conditionally create it in a tenant project")]
async fn create(w: &mut StorageWorld) {
    w.current = Some(
        w.store()
            .commit(w.scope(), w.original.as_ref().unwrap())
            .await
            .unwrap()
            .stored,
    );
}
#[then("reading the same relative path returns identical bytes")]
async fn identical(w: &mut StorageWorld) {
    let original = w.original.as_ref().unwrap();
    assert_eq!(
        w.store()
            .get(w.scope(), &original.path)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        original.bytes
    );
}
#[then("another tenant cannot read it")]
async fn isolated(w: &mut StorageWorld) {
    assert!(w
        .objects
        .as_ref()
        .unwrap()
        .get(
            &Scope::new("other", "app").unwrap(),
            "project/issues/x.json"
        )
        .await
        .unwrap()
        .is_none());
}
#[given("an issue and its opaque revision")]
async fn revision(w: &mut StorageWorld) {
    raw(w).await;
    create(w).await;
}
#[when("a writer replaces the issue using that revision")]
async fn update(w: &mut StorageWorld) {
    let update = w.mutation(
        "update",
        WriteCondition::Revision(w.current.as_ref().unwrap().revision.clone()),
        b"{\"id\":\"x\",\"title\":\"new\"}",
    );
    w.store().commit(w.scope(), &update).await.unwrap();
}
#[then("another writer using the old revision is rejected")]
async fn stale(w: &mut StorageWorld) {
    let stale = w.mutation(
        "stale",
        WriteCondition::Revision(w.current.as_ref().unwrap().revision.clone()),
        b"{}",
    );
    assert!(matches!(
        w.store().commit(w.scope(), &stale).await,
        Err(StoreError::Conflict)
    ));
}
#[given("a mutation whose issue was published but event publication failed")]
async fn event_failure(w: &mut StorageWorld) {
    raw(w).await;
    w.objects
        .as_ref()
        .unwrap()
        .fail_events
        .store(1, Ordering::SeqCst);
    let result = w
        .store()
        .commit(w.scope(), w.original.as_ref().unwrap())
        .await
        .unwrap();
    assert!(result.repair_pending);
    w.current = Some(result.stored);
}
#[when("the same operation is retried after a newer mutation")]
async fn repair(w: &mut StorageWorld) {
    update(w).await;
    w.newer = Some(
        w.store()
            .get(w.scope(), "project/issues/x.json")
            .await
            .unwrap()
            .unwrap()
            .bytes,
    );
    assert!(
        !w.store()
            .commit(w.scope(), w.original.as_ref().unwrap())
            .await
            .unwrap()
            .repair_pending
    );
}
#[then("the original event is repaired exactly once")]
async fn event_once(w: &mut StorageWorld) {
    let paths = w
        .store()
        .list(w.scope(), "project/events/", None, 10)
        .await
        .unwrap()
        .paths;
    assert_eq!(
        paths.iter().filter(|p| p.ends_with("create.json")).count(),
        1
    );
}
#[then("the newer issue remains current")]
async fn newer(w: &mut StorageWorld) {
    assert_eq!(
        w.store()
            .get(w.scope(), "project/issues/x.json")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        *w.newer.as_ref().unwrap()
    );
}
#[given("a successfully published operation")]
async fn published(w: &mut StorageWorld) {
    revision(w).await;
}
#[when("its operation identifier is reused for different bytes")]
async fn mismatch(w: &mut StorageWorld) {
    let mut changed = w.original.clone().unwrap();
    changed.bytes = b"{}".to_vec();
    changed.request_hash = "different".into();
    assert!(matches!(
        w.store().commit(w.scope(), &changed).await,
        Err(StoreError::OperationMismatch)
    ));
}
#[then("the request is rejected")]
async fn unchanged(w: &mut StorageWorld) {
    identical(w).await;
}
#[given("two scopes each holding an issue, its history, a catalog row and journal state")]
async fn two_populated_scopes(w: &mut StorageWorld) {
    raw(w).await;
    let original = w.original.clone().unwrap();
    for scope in [w.scope().clone(), w.other_scope.clone().unwrap()] {
        let result = w.store().commit(&scope, &original).await.unwrap();
        assert!(!result.repair_pending);
        let snapshot = w.snapshot(&scope).await;
        assert!(snapshot.canonical.len() == 2 && snapshot.catalog.len() == 1);
        assert!(snapshot.control.len() >= 3);
    }
    w.other_before = Some(w.snapshot(&w.other_scope.clone().unwrap()).await);
}
#[given("a purge of the first scope that failed while deleting control objects")]
async fn purge_failed_in_control(w: &mut StorageWorld) {
    let objects = w.objects.clone().unwrap();
    objects.fail_control_delete_at.store(1, Ordering::SeqCst);
    let failure = w
        .store()
        .purge_scope(w.scope(), PURGE_OPERATION_IDENTIFIER)
        .await;
    assert!(matches!(failure, Err(StoreError::Unavailable(_))));
    assert_eq!(objects.control_deletes.load(Ordering::SeqCst), 2);
    assert!(w.snapshot(w.scope()).await.canonical.is_empty());
    assert!(!w.snapshot(w.scope()).await.control.is_empty());
}
#[given("a purge of the first scope that failed while deleting canonical objects")]
async fn purge_failed_in_canonical(w: &mut StorageWorld) {
    let objects = w.objects.clone().unwrap();
    objects.fail_delete_at.store(1, Ordering::SeqCst);
    let failure = w
        .store()
        .purge_scope(w.scope(), PURGE_OPERATION_IDENTIFIER)
        .await;
    assert!(matches!(failure, Err(StoreError::Unavailable(_))));
    assert_eq!(w.snapshot(w.scope()).await.canonical.len(), 1);
}
#[given("the first scope is purged with an operation identifier")]
#[when("the first scope is purged with an operation identifier")]
async fn purge(w: &mut StorageWorld) {
    w.purge_result = Some(
        w.store()
            .purge_scope(w.scope(), PURGE_OPERATION_IDENTIFIER)
            .await
            .unwrap(),
    );
}
#[when("the first scope is purged again with the same operation identifier")]
async fn purge_again(w: &mut StorageWorld) {
    let before = w.purge_result.clone();
    let after = w
        .store()
        .purge_scope(w.scope(), PURGE_OPERATION_IDENTIFIER)
        .await
        .unwrap();
    if let Some(before) = before {
        assert_eq!(after, before);
    }
    w.purge_result = Some(after);
}
#[when("the original create operation is submitted again to the first scope")]
async fn resubmit_create(w: &mut StorageWorld) {
    w.store()
        .commit(w.scope(), w.original.as_ref().unwrap())
        .await
        .unwrap();
}
#[then("the first scope has no canonical objects, catalog rows or control state except the purge record")]
async fn first_scope_empty(w: &mut StorageWorld) {
    let snapshot = w.snapshot(w.scope()).await;
    assert!(snapshot.canonical.is_empty());
    assert!(snapshot.catalog.is_empty());
    assert_eq!(
        snapshot.control.keys().cloned().collect::<Vec<_>>(),
        w.purge_record_paths()
    );
    assert!(w.purge_result.is_some());
}
#[then("the second scope is byte-identical to its state before the purge")]
async fn second_scope_unchanged(w: &mut StorageWorld) {
    let after = w.snapshot(&w.other_scope.clone().unwrap()).await;
    assert_eq!(&after, w.other_before.as_ref().unwrap());
}
#[then("no object metadata sidecar remains for the first scope")]
async fn no_sidecars(w: &mut StorageWorld) {
    let sidecars = w
        .root
        .as_ref()
        .unwrap()
        .path()
        .join("_virtuus")
        .join(w.scope().namespace())
        .join("object-meta");
    assert!(!sidecars.exists() || std::fs::read_dir(sidecars).unwrap().next().is_none());
    let other = w
        .root
        .as_ref()
        .unwrap()
        .path()
        .join("_virtuus")
        .join(w.other_scope.as_ref().unwrap().namespace())
        .join("object-meta");
    assert!(std::fs::read_dir(other).unwrap().next().is_some());
}
#[then("the recorded purge result is returned unchanged")]
async fn recorded_result(w: &mut StorageWorld) {
    let result = w.purge_result.as_ref().unwrap();
    assert_eq!(result.canonical_objects_deleted, 2);
    assert_eq!(result.catalog_rows_deleted, 1);
    assert!(result.control_objects_deleted >= 3);
}
#[then("the issue is published afresh in the first scope")]
async fn published_afresh(w: &mut StorageWorld) {
    identical(w).await;
    assert_eq!(w.snapshot(w.scope()).await.catalog.len(), 1);
}
#[tokio::main]
async fn main() {
    StorageWorld::cucumber()
        .fail_on_skipped()
        .run_and_exit(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../features/store"
        ))
        .await;
}
