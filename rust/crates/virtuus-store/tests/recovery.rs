mod support;
use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};
use support::{FaultStore, Gate};
use virtuus_store::{
    DeleteMutation, EventObject, FileStore, JournaledStore, MemoryCatalog, Mutation, Scope,
    StoreError, WriteCondition,
};

fn mutation(id: &str, condition: WriteCondition, bytes: &[u8]) -> Mutation {
    Mutation {
        path: "project/issues/x.json".into(),
        condition,
        bytes: bytes.to_vec(),
        content_type: "application/json".into(),
        event_objects: vec![EventObject {
            path: format!("project/events/{id}.json"),
            bytes: format!("{{\"op\":\"{id}\"}}\n").into_bytes(),
            content_type: "application/json".into(),
        }],
        operation_id: id.into(),
        request_hash: id.into(),
        fields: BTreeMap::new(),
    }
}

fn setup() -> (tempfile::TempDir, Arc<FaultStore>, JournaledStore, Scope) {
    let root = tempfile::tempdir().unwrap();
    let objects = Arc::new(FaultStore::new(Arc::new(
        FileStore::new(root.path()).unwrap(),
    )));
    let store = JournaledStore::new(objects.clone(), Arc::new(MemoryCatalog::default()));
    (root, objects, store, Scope::new("brand", "app").unwrap())
}

#[tokio::test]
async fn failed_history_and_receipt_repair_do_not_rewrite_newer_bytes_or_strip_custom_fields() {
    let (_root, objects, store, scope) = setup();
    objects.fail_events.store(1, Ordering::SeqCst);
    let a = mutation("a", WriteCondition::Absent, b"{\"id\":\"x\"}");
    let first = store.commit(&scope, &a).await.unwrap();
    assert!(first.repair_pending);
    let mut b = mutation(
        "b",
        WriteCondition::Revision(first.stored.revision),
        b"{\"id\":\"x\",\"custom\":{\"region\":\"west\"}}",
    );
    b.fields.insert("custom.region".into(), "west".into());
    objects.fail_receipts.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.commit(&scope, &b).await,
        Err(StoreError::Uncertain(_))
    ));
    assert!(!store.commit(&scope, &b).await.unwrap().repair_pending);
    assert!(!store.commit(&scope, &a).await.unwrap().repair_pending);
    assert_eq!(objects.puts.load(Ordering::SeqCst), 2);
    assert_eq!(
        store.get(&scope, &b.path).await.unwrap().unwrap().bytes,
        b.bytes
    );
    assert_eq!(
        store
            .query_candidates(&scope, &[], None, 10)
            .await
            .unwrap()
            .cards[0]
            .fields["custom.region"],
        "west"
    );
    assert_eq!(
        store
            .list(&scope, "project/events/", None, 10)
            .await
            .unwrap()
            .paths
            .len(),
        2
    );
}

#[tokio::test]
async fn duplicate_delete_worker_cannot_delete_an_identical_recreation() {
    let (_root, objects, store, scope) = setup();
    let create = mutation("create", WriteCondition::Absent, b"{\"id\":\"x\"}");
    let created = store.commit(&scope, &create).await.unwrap();
    let deletion = DeleteMutation {
        path: create.path.clone(),
        expected_revision: created.stored.revision,
        event_objects: vec![],
        operation_id: "delete".into(),
        request_hash: "delete".into(),
    };
    let gate = Arc::new(Gate::default());
    *objects.gate_delete.lock().unwrap() = Some(gate.clone());
    let task = tokio::spawn({
        let store = store.clone();
        let scope = scope.clone();
        let deletion = deletion.clone();
        async move { store.delete(&scope, &deletion).await }
    });
    gate.entered.notified().await;
    assert!(matches!(
        store.delete(&scope, &deletion).await,
        Err(StoreError::Uncertain(_))
    ));
    let recreate = mutation("recreate", WriteCondition::Absent, &create.bytes);
    assert!(matches!(
        store.commit(&scope, &recreate).await,
        Err(StoreError::Uncertain(_))
    ));
    gate.release.notify_one();
    assert!(!task.await.unwrap().unwrap().repair_pending);
    store.commit(&scope, &recreate).await.unwrap();
    store.delete(&scope, &deletion).await.unwrap();
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .get(&scope, &create.path)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        create.bytes
    );
}

#[tokio::test]
async fn duplicate_create_worker_cannot_resurrect_after_delete() {
    let (_root, objects, store, scope) = setup();
    let create = mutation("create", WriteCondition::Absent, b"{\"id\":\"x\"}");
    let gate = Arc::new(Gate::default());
    *objects.gate_put.lock().unwrap() = Some(gate.clone());
    let task = tokio::spawn({
        let store = store.clone();
        let scope = scope.clone();
        let create = create.clone();
        async move { store.commit(&scope, &create).await }
    });
    gate.entered.notified().await;
    assert!(matches!(
        store.commit(&scope, &create).await,
        Err(StoreError::Uncertain(_))
    ));
    gate.release.notify_one();
    let created = task.await.unwrap().unwrap();
    store
        .delete(
            &scope,
            &DeleteMutation {
                path: create.path.clone(),
                expected_revision: created.stored.revision,
                event_objects: vec![],
                operation_id: "delete".into(),
                request_hash: "delete".into(),
            },
        )
        .await
        .unwrap();
    store.commit(&scope, &create).await.unwrap();
    assert_eq!(objects.puts.load(Ordering::SeqCst), 1);
    assert!(store.get(&scope, &create.path).await.unwrap().is_none());
}

#[tokio::test]
async fn definite_upload_rejection_is_retryable_but_ambiguous_publication_blocks_successors() {
    let (_root, objects, store, scope) = setup();
    let a = mutation("a", WriteCondition::Absent, b"{\"id\":\"x\"}");
    objects.fail_uploads.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.commit(&scope, &a).await,
        Err(StoreError::Unavailable(_))
    ));
    assert!(store.get(&scope, &a.path).await.unwrap().is_none());
    let first = store.commit(&scope, &a).await.unwrap();
    let b = mutation(
        "b",
        WriteCondition::Revision(first.stored.revision),
        b"{\"id\":\"x\",\"title\":\"b\"}",
    );
    objects.uncertain_uploads.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.commit(&scope, &b).await,
        Err(StoreError::Uncertain(_))
    ));
    assert!(matches!(
        store.commit(&scope, &b).await,
        Err(StoreError::Uncertain(_))
    ));
    let c = mutation("c", WriteCondition::Absent, b"{}");
    assert!(matches!(
        store.commit(&scope, &c).await,
        Err(StoreError::Uncertain(_))
    ));
    assert_eq!(
        store.get(&scope, &a.path).await.unwrap().unwrap().bytes,
        a.bytes
    );
    assert_eq!(objects.puts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn canonical_read_failure_before_publication_releases_the_unapplied_attempt() {
    let (_root, objects, store, scope) = setup();
    let a = mutation("a", WriteCondition::Absent, b"{\"id\":\"x\"}");
    objects.fail_reads.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.commit(&scope, &a).await,
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(objects.puts.load(Ordering::SeqCst), 0);
    assert!(!store.commit(&scope, &a).await.unwrap().repair_pending);
    assert_eq!(objects.puts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lost_delete_receipt_is_recovered_without_a_second_delete() {
    let (_root, objects, store, scope) = setup();
    let a = mutation("a", WriteCondition::Absent, b"{\"id\":\"x\"}");
    let created = store.commit(&scope, &a).await.unwrap();
    let deletion = DeleteMutation {
        path: a.path.clone(),
        expected_revision: created.stored.revision,
        event_objects: vec![],
        operation_id: "delete".into(),
        request_hash: "delete".into(),
    };
    objects.fail_receipts.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.delete(&scope, &deletion).await,
        Err(StoreError::Uncertain(_))
    ));
    assert!(
        !store
            .delete(&scope, &deletion)
            .await
            .unwrap()
            .repair_pending
    );
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 1);
    assert!(store.get(&scope, &a.path).await.unwrap().is_none());
}

#[tokio::test]
async fn failed_conflict_receipt_before_publication_can_be_retried() {
    let (_root, objects, store, scope) = setup();
    let a = mutation("a", WriteCondition::Absent, b"{\"id\":\"x\"}");
    store.commit(&scope, &a).await.unwrap();
    let stale = mutation("stale", WriteCondition::Revision("stale".into()), b"{}");
    objects.fail_receipts.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.commit(&scope, &stale).await,
        Err(StoreError::Uncertain(_))
    ));
    assert!(matches!(
        store.commit(&scope, &stale).await,
        Err(StoreError::Conflict)
    ));
    assert_eq!(objects.puts.load(Ordering::SeqCst), 1);
}
