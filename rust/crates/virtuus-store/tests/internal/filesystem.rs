use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn invalid_lock_descriptor_is_an_io_error_and_never_acquires_ownership() {
    let error = FileStore::lock_descriptor(-1).unwrap_err();
    assert!(matches!(error, StoreError::Io(error) if error.raw_os_error() == Some(libc::EBADF)));
}

#[test]
fn bounded_reader_rejects_growth_after_the_initial_size_check() {
    let bytes = std::io::Cursor::new(b"grew after metadata");
    assert!(matches!(
        FileStore::read_bytes(bytes, 2, Some(4)),
        Err(StoreError::TooLarge { limit: 4 })
    ));
    assert_eq!(
        FileStore::read_bytes(std::io::Cursor::new(b"fits"), 4, Some(4)).unwrap(),
        b"fits"
    );
}

#[tokio::test]
async fn unreadable_canonical_objects_and_corrupt_sidecars_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let store = FileStore::new(root.path()).unwrap();
    let scope = Scope::new("brand", "board").unwrap();
    let path = "project/issues/a.json";
    let object = store
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
    let (canonical, control) = store.roots(&scope).unwrap();
    let full = canonical.join(path);
    let metadata = fs::metadata(&full).unwrap();
    let sidecar = FileStore::metadata_path(&control, path, &metadata, &object.revision);
    fs::write(&sidecar, b"invalid JSON").unwrap();
    assert!(matches!(
        store.get(&scope, path).await,
        Err(StoreError::Json(_))
    ));
    fs::remove_file(&sidecar).unwrap();
    fs::create_dir(&sidecar).unwrap();
    assert!(store.get(&scope, path).await.is_err());
    fs::remove_dir(&sidecar).unwrap();
    fs::set_permissions(&full, fs::Permissions::from_mode(0o000)).unwrap();
    let result = store.get(&scope, path).await;
    fs::set_permissions(&full, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(result.is_err());
    assert_eq!(store.get(&scope, path).await.unwrap().unwrap().bytes, b"{}");
}
