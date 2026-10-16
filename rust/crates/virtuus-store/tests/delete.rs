use std::collections::BTreeMap;
use std::sync::Arc;
use virtuus_store::{
    DeleteMutation, FileStore, JournaledStore, MemoryCatalog, Mutation, Scope, StoreError,
    WriteCondition,
};

#[tokio::test]
async fn delete_is_conditional_retryable_and_does_not_remove_a_recreation() {
    let root = tempfile::tempdir().unwrap();
    let store = JournaledStore::new(
        Arc::new(FileStore::new(root.path()).unwrap()),
        Arc::new(MemoryCatalog::default()),
    );
    let scope = Scope::new("brand", "app").unwrap();
    let mut mutation = Mutation {
        path: "project/issues/x.json".into(),
        condition: WriteCondition::Absent,
        bytes: b"{\"id\":\"x\",\"type\":\"task\"}".to_vec(),
        content_type: "application/json".into(),
        event_objects: vec![],
        operation_id: "create".into(),
        request_hash: "create".into(),
        fields: BTreeMap::new(),
    };
    let created = store.commit(&scope, &mutation).await.unwrap();
    let mut deletion = DeleteMutation {
        path: mutation.path.clone(),
        expected_revision: "stale".into(),
        event_objects: vec![],
        operation_id: "stale-delete".into(),
        request_hash: "stale-delete".into(),
    };
    assert!(matches!(
        store.delete(&scope, &deletion).await,
        Err(StoreError::Conflict)
    ));
    deletion.expected_revision = created.stored.revision;
    deletion.operation_id = "delete".into();
    deletion.request_hash = "delete".into();
    assert!(
        !store
            .delete(&scope, &deletion)
            .await
            .unwrap()
            .repair_pending
    );
    assert!(store.get(&scope, &mutation.path).await.unwrap().is_none());
    assert!(store
        .query_candidates(&scope, &[], None, 10)
        .await
        .unwrap()
        .cards
        .is_empty());
    mutation.operation_id = "recreate".into();
    mutation.request_hash = "recreate".into();
    mutation.bytes = b"{\"id\":\"x\",\"type\":\"task\",\"title\":\"new\"}".to_vec();
    store.commit(&scope, &mutation).await.unwrap();
    assert!(
        !store
            .delete(&scope, &deletion)
            .await
            .unwrap()
            .repair_pending
    );
    assert_eq!(
        store
            .get(&scope, &mutation.path)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        mutation.bytes
    );
    assert_eq!(
        store
            .query_candidates(&scope, &[], None, 10)
            .await
            .unwrap()
            .cards
            .len(),
        1
    );
}
