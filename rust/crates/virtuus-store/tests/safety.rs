use virtuus_store::{FileStore, ObjectStore, Scope, StoreError, WriteCondition};

#[test]
fn serialized_scopes_cannot_bypass_namespace_validation() {
    assert!(Scope::new("Brand", "app").is_err());
    assert!(Scope::new("brand", "App").is_err());
    for tenant in ["..", "_virtuus", "a/b", "Brand"] {
        assert!(serde_json::from_value::<Scope>(
            serde_json::json!({"tenant":tenant,"project":"app"})
        )
        .is_err());
    }
}

#[tokio::test]
async fn namespace_symlinks_cannot_escape_the_store() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("brand")).unwrap();
    let store = FileStore::new(root.path()).unwrap();
    let scope = Scope::new("brand", "app").unwrap();
    assert!(matches!(
        store
            .put(
                &scope,
                "issue.json",
                b"{}",
                "application/json",
                WriteCondition::Absent,
                None
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert!(!outside.path().join("app/issue.json").exists());
}
