use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use virtuus_store::{
    FileStore, JournaledStore, MemoryCatalog, ObjectPage, ObjectStore, Scope, StoreError,
    StoredObject, WriteCondition,
};

struct PurgeRecordStore {
    inner: Arc<dyn ObjectStore>,
    reject_intent: AtomicBool,
    lose_receipt_race: AtomicBool,
    vanish_receipt: AtomicBool,
    reject_receipt: AtomicBool,
}

impl PurgeRecordStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            reject_intent: AtomicBool::new(false),
            lose_receipt_race: AtomicBool::new(false),
            vanish_receipt: AtomicBool::new(false),
            reject_receipt: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl ObjectStore for PurgeRecordStore {
    async fn get(&self, s: &Scope, p: &str) -> virtuus_store::Result<Option<StoredObject>> {
        self.inner.get(s, p).await
    }
    async fn get_bounded(
        &self,
        s: &Scope,
        p: &str,
        m: usize,
    ) -> virtuus_store::Result<Option<StoredObject>> {
        self.inner.get_bounded(s, p, m).await
    }
    async fn list(
        &self,
        s: &Scope,
        p: &str,
        c: Option<&str>,
        l: usize,
    ) -> virtuus_store::Result<ObjectPage> {
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
    ) -> virtuus_store::Result<StoredObject> {
        self.inner.put(s, p, b, t, c, o).await
    }
    async fn delete(&self, s: &Scope, p: &str, r: &str) -> virtuus_store::Result<()> {
        self.inner.delete(s, p, r).await
    }
    async fn get_control(&self, s: &Scope, p: &str) -> virtuus_store::Result<Option<StoredObject>> {
        self.inner.get_control(s, p).await
    }
    async fn put_control(
        &self,
        s: &Scope,
        p: &str,
        b: &[u8],
        c: WriteCondition,
    ) -> virtuus_store::Result<StoredObject> {
        if p.starts_with("purge/intents/") && self.reject_intent.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable("injected intent rejection".into()));
        }
        if p.starts_with("purge/receipts/") {
            if self.reject_receipt.load(Ordering::SeqCst) {
                return Err(StoreError::Unavailable("injected receipt rejection".into()));
            }
            if self.vanish_receipt.load(Ordering::SeqCst) {
                return Err(StoreError::Conflict);
            }
            if self.lose_receipt_race.swap(false, Ordering::SeqCst) {
                self.inner
                    .put_control(
                        s,
                        p,
                        br#"{"canonical_objects_deleted":7,"catalog_rows_deleted":8,"control_objects_deleted":9}"#,
                        WriteCondition::Absent,
                    )
                    .await?;
            }
        }
        self.inner.put_control(s, p, b, c).await
    }
    async fn list_control(
        &self,
        s: &Scope,
        p: &str,
        c: Option<&str>,
        l: usize,
    ) -> virtuus_store::Result<ObjectPage> {
        self.inner.list_control(s, p, c, l).await
    }
    async fn delete_control(&self, s: &Scope, p: &str, r: &str) -> virtuus_store::Result<()> {
        self.inner.delete_control(s, p, r).await
    }
}

fn fixture() -> (
    tempfile::TempDir,
    Arc<PurgeRecordStore>,
    JournaledStore,
    Scope,
) {
    let root = tempfile::tempdir().unwrap();
    let objects = Arc::new(PurgeRecordStore::new(Arc::new(
        FileStore::new(root.path()).unwrap(),
    )));
    let store = JournaledStore::new(objects.clone(), Arc::new(MemoryCatalog::default()));
    (root, objects, store, Scope::new("brand", "app").unwrap())
}

#[tokio::test]
async fn purge_rejects_invalid_operation_identifiers_and_intent_rejection() {
    let (_root, objects, store, scope) = fixture();
    assert!(matches!(
        store.purge_scope(&scope, "").await,
        Err(StoreError::Invalid(_))
    ));
    objects.reject_intent.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.purge_scope(&scope, "purge-1").await,
        Err(StoreError::Unavailable(_))
    ));
}

#[tokio::test]
async fn purge_of_an_empty_scope_records_zero_counts() {
    let (_root, _objects, store, scope) = fixture();
    let result = store.purge_scope(&scope, "purge-1").await.unwrap();
    assert_eq!(
        (
            result.canonical_objects_deleted,
            result.catalog_rows_deleted,
            result.control_objects_deleted
        ),
        (0, 0, 0)
    );
}

#[tokio::test]
async fn purge_returns_a_concurrently_recorded_receipt_and_reports_receipt_failures() {
    let (_root, objects, store, scope) = fixture();
    objects.lose_receipt_race.store(true, Ordering::SeqCst);
    let recorded = store.purge_scope(&scope, "purge-1").await.unwrap();
    assert_eq!(recorded.canonical_objects_deleted, 7);

    objects.vanish_receipt.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.purge_scope(&scope, "purge-2").await,
        Err(StoreError::Uncertain(_))
    ));
    objects.vanish_receipt.store(false, Ordering::SeqCst);
    objects.reject_receipt.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.purge_scope(&scope, "purge-2").await,
        Err(StoreError::Unavailable(_))
    ));
}

#[tokio::test]
async fn control_listing_pages_in_lexical_order_and_rejects_stale_deletes() {
    let (_root, objects, _store, scope) = fixture();
    for path in ["b/two.json", "a/one.json", "b/three.json"] {
        objects
            .put_control(&scope, path, path.as_bytes(), WriteCondition::Absent)
            .await
            .unwrap();
    }
    let first = objects.list_control(&scope, "", None, 2).await.unwrap();
    assert_eq!(first.paths, ["a/one.json", "b/three.json"]);
    let second = objects
        .list_control(&scope, "", first.next_cursor.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(second.paths, ["b/two.json"]);
    assert!(second.next_cursor.is_none());
    assert_eq!(
        objects
            .list_control(&scope, "b/", None, 10)
            .await
            .unwrap()
            .paths,
        ["b/three.json", "b/two.json"]
    );
    assert!(matches!(
        objects.list_control(&scope, "../x", None, 1).await,
        Err(StoreError::Invalid(_))
    ));
    assert!(matches!(
        objects.list_control(&scope, "", None, 0).await,
        Err(StoreError::Invalid(_))
    ));
    assert!(matches!(
        objects.list_control(&scope, "", Some("../x"), 1).await,
        Err(StoreError::Invalid(_))
    ));

    assert!(matches!(
        objects.delete_control(&scope, "a/one.json", "stale").await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        objects.delete_control(&scope, "missing.json", "any").await,
        Err(StoreError::Conflict)
    ));
    let revision = objects
        .get_control(&scope, "a/one.json")
        .await
        .unwrap()
        .unwrap()
        .revision;
    objects
        .delete_control(&scope, "a/one.json", &revision)
        .await
        .unwrap();
    assert!(objects
        .get_control(&scope, "a/one.json")
        .await
        .unwrap()
        .is_none());
}
