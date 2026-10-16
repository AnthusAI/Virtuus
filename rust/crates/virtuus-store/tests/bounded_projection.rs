use std::sync::Arc;
use virtuus_store::{
    AuthorizedProjects, BoardQuery, FileStore, JournaledStore, MemoryCatalog, ObjectStore,
    ProjectSelection, QueryOrder, Scope, StoreError, WriteCondition,
};

#[tokio::test]
async fn bounded_native_reads_reject_oversized_objects_without_changing_bytes() {
    let root = tempfile::tempdir().unwrap();
    let objects = FileStore::new(root.path()).unwrap();
    let scope = Scope::new("brand", "app").unwrap();
    objects
        .put(
            &scope,
            "attachment.bin",
            &vec![5; 450_000],
            "application/octet-stream",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        objects.get_bounded(&scope, "attachment.bin", 1024).await,
        Err(StoreError::TooLarge { limit: 1024 })
    ));
    assert_eq!(
        objects
            .get_bounded(&scope, "attachment.bin", 450_000)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .len(),
        450_000
    );
    assert!(objects
        .get_bounded(&scope, "missing.bin", 1024)
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        objects.get_bounded(&scope, "attachment.bin", 0).await,
        Err(StoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn huge_optional_title_import_rebuilds_a_card_and_rejects_unprojected_sort() {
    let root = tempfile::tempdir().unwrap();
    let scope = Scope::new("brand", "app").unwrap();
    let objects = Arc::new(FileStore::for_project(root.path(), scope.clone()).unwrap());
    let raw=serde_json::to_vec(&serde_json::json!({"id":"x","type":"task","status":"open","title":"x".repeat(17_000),"unknown":{"extra":[1,2]}})).unwrap();
    objects
        .put(
            &scope,
            "project/issues/x.json",
            &raw,
            "application/json",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    let store = JournaledStore::new(objects, Arc::new(MemoryCatalog::default()));
    assert_eq!(
        store
            .rebuild_catalog(&scope, "project/issues/")
            .await
            .unwrap(),
        1
    );
    let page = store
        .query_candidates(&scope, &["task".into()], None, 10)
        .await
        .unwrap();
    assert_eq!(page.cards.len(), 1);
    assert_eq!(page.cards[0].omitted_fields, ["title"]);
    assert_eq!(page.cards[0].fields["status"], "open");
    assert_eq!(
        store
            .get_bounded(&scope, "project/issues/x.json", 20_000)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        raw
    );
    let authorized = AuthorizedProjects::new("brand", vec![scope]).unwrap();
    let query = BoardQuery {
        order: vec![QueryOrder {
            field: "title".into(),
            descending: false,
        }],
        ..BoardQuery::default()
    };
    assert!(matches!(
        store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
            .await,
        Err(StoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn replay_repair_obeys_the_consumer_read_budget_after_quiesced_raw_import() {
    use std::collections::BTreeMap;
    use virtuus_store::{content_hash, Mutation};
    let root = tempfile::tempdir().unwrap();
    let scope = Scope::new("brand", "app").unwrap();
    let objects = Arc::new(FileStore::new(root.path()).unwrap());
    let store = JournaledStore::new(objects.clone(), Arc::new(MemoryCatalog::default()))
        .with_canonical_read_limit(1024)
        .unwrap();
    let original = b"{\"id\":\"x\",\"type\":\"task\"}";
    let mutation = Mutation {
        path: "project/issues/x.json".into(),
        condition: WriteCondition::Absent,
        bytes: original.to_vec(),
        content_type: "application/json".into(),
        event_objects: vec![],
        operation_id: "create".into(),
        request_hash: content_hash(original),
        fields: BTreeMap::new(),
    };
    let first = store.commit(&scope, &mutation).await.unwrap();
    let imported =
        serde_json::to_vec(&serde_json::json!({"id":"x","type":"task","unknown":"x".repeat(5000)}))
            .unwrap();
    objects
        .put(
            &scope,
            &mutation.path,
            &imported,
            "application/json",
            WriteCondition::Revision(first.stored.revision.clone()),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        store.get(&scope, &mutation.path).await,
        Err(StoreError::TooLarge { limit: 1024 })
    ));
    assert!(matches!(
        store.get_bounded(&scope, &mutation.path, 10_000).await,
        Err(StoreError::TooLarge { limit: 1024 })
    ));
    let replay = store
        .operation(&scope, "create", &mutation.request_hash)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.stored, first.stored);
    assert!(replay.repair_pending);
    assert_eq!(
        objects
            .get(&scope, &mutation.path)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        imported
    );
}

#[tokio::test]
async fn primary_candidate_fields_survive_a_nearly_full_optional_identity_scalar() {
    let root = tempfile::tempdir().unwrap();
    let scope = Scope::new("brand", "app").unwrap();
    let objects = Arc::new(FileStore::new(root.path()).unwrap());
    let raw = serde_json::to_vec(
        &serde_json::json!({"id":"x".repeat(16_370),"type":"task","status":"open"}),
    )
    .unwrap();
    objects
        .put(
            &scope,
            "project/issues/x.json",
            &raw,
            "application/json",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    let store = JournaledStore::new(objects, Arc::new(MemoryCatalog::default()));
    store
        .rebuild_catalog_with_fields(
            &scope,
            "project/issues/",
            &["id".into(), "type".into(), "status".into()],
        )
        .await
        .unwrap();
    let page = store
        .query_candidates(&scope, &["task".into()], None, 10)
        .await
        .unwrap();
    assert_eq!(page.cards.len(), 1);
    assert_eq!(page.cards[0].fields["type"], "task");
    assert_eq!(page.cards[0].fields["status"], "open");
    assert_eq!(page.cards[0].omitted_fields, ["id"]);
    assert_eq!(
        store
            .get(&scope, "project/issues/x.json")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        raw
    );
}
