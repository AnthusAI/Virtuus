mod support;
use cucumber::{given, then, when, World};
use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};
use support::FaultStore;
use virtuus_store::{
    EventObject, FileStore, JournaledStore, MemoryCatalog, Mutation, ObjectStore, Scope,
    StoreError, StoredObject, WriteCondition,
};

#[derive(Default, World)]
struct StorageWorld {
    root: Option<tempfile::TempDir>,
    objects: Option<Arc<FaultStore>>,
    store: Option<JournaledStore>,
    scope: Option<Scope>,
    original: Option<Mutation>,
    current: Option<StoredObject>,
    newer: Option<Vec<u8>>,
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
        self.store = Some(JournaledStore::new(
            objects.clone(),
            Arc::new(MemoryCatalog::default()),
        ));
        self.objects = Some(objects);
        self.scope = Some(Scope::new("brand", "app").unwrap());
        self.root = Some(root);
    }
    fn scope(&self) -> &Scope {
        self.scope.as_ref().unwrap()
    }
    fn store(&self) -> &JournaledStore {
        self.store.as_ref().unwrap()
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
