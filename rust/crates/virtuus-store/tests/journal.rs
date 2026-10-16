use std::collections::BTreeMap;
use std::sync::Arc;
use virtuus_store::{
    EventObject, FileStore, JournaledStore, MemoryCatalog, Mutation, Scope, StoreError,
    WriteCondition,
};

#[tokio::test]
async fn retry_returns_original_publication_and_rejects_changed_meaning() {
    let root = tempfile::tempdir().unwrap();
    let store = JournaledStore::new(
        Arc::new(FileStore::new(root.path()).unwrap()),
        Arc::new(MemoryCatalog::default()),
    );
    let scope = Scope::new("brand", "app").unwrap();
    let mutation = Mutation {
        path: "project/issues/x.json".into(),
        condition: WriteCondition::Absent,
        bytes: b"{ \"id\": \"x\", \"type\": \"prospect\" }\n".to_vec(),
        content_type: "application/json".into(),
        event_objects: vec![EventObject {
            path: "project/events/create-x.json".into(),
            bytes: b"{\"created\":true}\n".to_vec(),
            content_type: "application/json".into(),
        }],
        operation_id: "create-x".into(),
        request_hash: "request-one".into(),
        fields: BTreeMap::from([("type".into(), serde_json::json!("prospect"))]),
    };
    let first = store.commit(&scope, &mutation).await.unwrap();
    assert!(!first.repair_pending);
    let second = store.commit(&scope, &mutation).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(
        store
            .operation(&scope, "create-x", "request-one")
            .await
            .unwrap()
            .unwrap(),
        first
    );
    assert!(matches!(
        store.operation(&scope, "create-x", "different").await,
        Err(StoreError::OperationMismatch)
    ));
    assert_eq!(
        store
            .list(&scope, "project/events/", None, 10)
            .await
            .unwrap()
            .paths
            .len(),
        1
    );
    assert_eq!(
        store
            .query_candidates(&scope, &["prospect".into()], None, 10)
            .await
            .unwrap()
            .cards
            .len(),
        1
    );
}
