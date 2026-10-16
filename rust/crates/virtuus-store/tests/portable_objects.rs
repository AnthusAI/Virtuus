use virtuus_store::{FileStore, ObjectStore, Scope, StoreError, WriteCondition};

#[tokio::test]
async fn raw_files_keep_normal_paths_and_tenant_isolation() {
    let root = tempfile::tempdir().unwrap();
    let store = FileStore::new(root.path()).unwrap();
    let scope = Scope::new("brand", "app").unwrap();
    let raw = b"{ \"id\":\"x\", \"unknown\": [ 1, 2 ] }\n";
    let first = store
        .put(
            &scope,
            "project/issues/x.json",
            raw,
            "application/json",
            WriteCondition::Absent,
            Some("create-x"),
        )
        .await
        .unwrap();
    assert_eq!(first.bytes, raw);
    assert_eq!(
        std::fs::read(root.path().join("brand/app/project/issues/x.json")).unwrap(),
        raw
    );
    assert!(store
        .get(
            &Scope::new("other", "app").unwrap(),
            "project/issues/x.json"
        )
        .await
        .unwrap()
        .is_none());
    let updated = store
        .put(
            &scope,
            "project/issues/x.json",
            b"new",
            "application/json",
            WriteCondition::Revision(first.revision.clone()),
            Some("update-x"),
        )
        .await
        .unwrap();
    assert_ne!(first.revision, updated.revision);
    assert!(matches!(
        store
            .put(
                &scope,
                "project/issues/x.json",
                b"stale",
                "application/json",
                WriteCondition::Revision(first.revision),
                None
            )
            .await,
        Err(StoreError::Conflict)
    ));
    assert_eq!(
        store
            .get(&scope, "project/issues/x.json")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"new"
    );
}
