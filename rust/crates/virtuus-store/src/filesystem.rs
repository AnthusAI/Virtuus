use crate::{
    content_hash, validate_limit, validate_path, validate_prefix, ObjectPage, ObjectStore, Result,
    Scope, StoreError, StoredObject, WriteCondition,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[cfg(test)]
#[path = "../tests/internal/filesystem.rs"]
mod failure_tests;

/// A portable filesystem backend; cooperating writers serialize with advisory flock.
#[derive(Clone, Debug)]
pub struct FileStore {
    root: PathBuf,
    binding: Option<Scope>,
    control_root: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    operation_id: Option<String>,
    content_type: String,
}

struct Lock(File);
impl Drop for Lock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

impl FileStore {
    /// Create a store whose canonical folders are root/tenant/project.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root = root.as_ref().canonicalize()?;
        Ok(Self {
            control_root: root.join("_virtuus"),
            root,
            binding: None,
        })
    }

    /// Bind an existing ordinary Kanbus folder to one scope.
    /// Supplemental files live beside, never inside, the canonical project folder.
    pub fn for_project(root: impl AsRef<Path>, scope: Scope) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root = root.as_ref().canonicalize()?;
        let parent = root
            .parent()
            .ok_or_else(|| StoreError::Invalid("project folder must have a parent".into()))?;
        let name = root
            .file_name()
            .ok_or_else(|| StoreError::Invalid("project folder must have a name".into()))?
            .to_string_lossy();
        let control_root = parent.join(format!(".{name}.virtuus"));
        Ok(Self {
            root,
            control_root,
            binding: Some(scope),
        })
    }

    fn roots(&self, scope: &Scope) -> Result<(PathBuf, PathBuf)> {
        Scope::new(&scope.tenant, &scope.project)?;
        if self.binding.as_ref().is_some_and(|bound| bound != scope) {
            return Err(StoreError::Invalid(
                "scope does not match bound project".into(),
            ));
        }
        let canonical = if self.binding.is_some() {
            self.root.clone()
        } else {
            self.root.join(scope.namespace())
        };
        let control = self.control_root.join(scope.namespace());
        Self::reject_symlink_ancestors(&canonical)?;
        Self::reject_symlink_ancestors(&control)?;
        Ok((canonical, control))
    }

    fn reject_symlink_ancestors(path: &Path) -> Result<()> {
        for ancestor in path.ancestors() {
            if fs::symlink_metadata(ancestor).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(StoreError::Invalid("symbolic namespace ancestor".into()));
            }
        }
        Ok(())
    }

    fn safe_join(root: &Path, path: &str) -> Result<PathBuf> {
        validate_path(path)?;
        let mut current = root.to_path_buf();
        for component in path.split('/') {
            current.push(component);
            if fs::symlink_metadata(&current).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(StoreError::Invalid(
                    "symbolic links are not canonical objects".into(),
                ));
            }
        }
        Ok(current)
    }

    fn lock(control: &Path) -> Result<Lock> {
        fs::create_dir_all(control)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(control.join("writer.lock"))?;
        Self::lock_descriptor(file.as_raw_fd())?;
        Ok(Lock(file))
    }

    fn lock_descriptor(descriptor: libc::c_int) -> Result<()> {
        if unsafe { libc::flock(descriptor, libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    fn metadata_path(control: &Path, path: &str, metadata: &fs::Metadata, hash: &str) -> PathBuf {
        control
            .join("object-meta")
            .join(content_hash(path.as_bytes()))
            .join(format!("{}-{}-{hash}.json", metadata.dev(), metadata.ino()))
    }

    fn read_file(
        root: &Path,
        control: &Path,
        path: &str,
        canonical: bool,
        max_bytes: Option<usize>,
    ) -> Result<Option<StoredObject>> {
        let full = Self::safe_join(root, path)?;
        let file = match File::open(&full) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(StoreError::Invalid(
                "object path is not a regular file".into(),
            ));
        }
        let bytes = Self::read_bytes(file, metadata.len(), max_bytes)?;
        let revision = content_hash(&bytes);
        let sidecar = if canonical {
            match fs::read(Self::metadata_path(control, path, &metadata, &revision)) {
                Ok(bytes) => Some(serde_json::from_slice::<Metadata>(&bytes)?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        Ok(Some(StoredObject {
            bytes,
            revision,
            operation_id: sidecar.as_ref().and_then(|m| m.operation_id.clone()),
            content_type: sidecar
                .map(|m| m.content_type)
                .unwrap_or_else(|| "application/octet-stream".into()),
        }))
    }

    fn read_bytes(mut reader: impl Read, size: u64, max_bytes: Option<usize>) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        if let Some(limit) = max_bytes {
            if size > limit as u64 {
                return Err(StoreError::TooLarge { limit });
            }
            reader
                .take(limit.saturating_add(1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > limit {
                return Err(StoreError::TooLarge { limit });
            }
        } else {
            reader.read_to_end(&mut bytes)?;
        }
        Ok(bytes)
    }

    fn publish(
        roots: (&Path, &Path),
        path: &str,
        bytes: &[u8],
        content_type: &str,
        condition: WriteCondition,
        operation_id: Option<&str>,
        canonical: bool,
    ) -> Result<StoredObject> {
        let (root, control) = roots;
        let _lock = Self::lock(control)?;
        let current = Self::read_file(root, control, path, canonical, None)?;
        let matches = match condition {
            WriteCondition::Absent => current.is_none(),
            WriteCondition::Revision(expected) => current
                .as_ref()
                .is_some_and(|current| current.revision == expected),
        };
        if !matches {
            return Err(StoreError::Conflict);
        }
        let full = Self::safe_join(root, path)?;
        let parent = full
            .parent()
            .ok_or_else(|| StoreError::Invalid("object requires parent".into()))?;
        fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        let revision = content_hash(bytes);
        if canonical {
            let meta_path =
                Self::metadata_path(control, path, &temporary.as_file().metadata()?, &revision);
            fs::create_dir_all(meta_path.parent().unwrap())?;
            let metadata = Metadata {
                operation_id: operation_id.map(str::to_owned),
                content_type: content_type.into(),
            };
            let mut meta_file = File::create(&meta_path)?;
            meta_file.write_all(&serde_json::to_vec(&metadata)?)?;
            meta_file.sync_all()?;
            File::open(meta_path.parent().unwrap())?.sync_all()?;
        }
        temporary.persist(&full).map_err(|error| error.error)?;
        File::open(parent)?.sync_all()?;
        Ok(StoredObject {
            bytes: bytes.into(),
            revision,
            operation_id: operation_id.map(str::to_owned),
            content_type: content_type.into(),
        })
    }

    fn remove_object_metadata(control: &Path, path: &str) -> Result<()> {
        match fs::remove_dir_all(
            control
                .join("object-meta")
                .join(content_hash(path.as_bytes())),
        ) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn page_paths(
        root: &Path,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ObjectPage> {
        let mut paths = Vec::new();
        let subtree = if prefix.is_empty() {
            root.to_path_buf()
        } else {
            let prefix_path = Self::safe_join(root, prefix.trim_end_matches('/'))?;
            if prefix.ends_with('/') {
                prefix_path
            } else {
                prefix_path.parent().unwrap_or(root).to_path_buf()
            }
        };
        Self::collect(root, &subtree, &mut paths)?;
        paths.retain(|path| {
            path.starts_with(prefix) && cursor.is_none_or(|cursor| path.as_str() > cursor)
        });
        paths.sort();
        let next_cursor = if paths.len() > limit {
            paths.truncate(limit);
            paths.last().cloned()
        } else {
            None
        };
        Ok(ObjectPage { paths, next_cursor })
    }

    fn collect(root: &Path, current: &Path, paths: &mut Vec<String>) -> Result<()> {
        if !current.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                continue;
            }
            if kind.is_dir() {
                Self::collect(root, &entry.path(), paths)?;
            }
            if kind.is_file() {
                paths.push(
                    entry
                        .path()
                        .strip_prefix(root)
                        .map_err(|e| StoreError::Invalid(e.to_string()))?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for FileStore {
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<StoredObject>> {
        let (root, control) = self.roots(scope)?;
        Self::read_file(&root, &control, path, true, None)
    }

    async fn get_bounded(
        &self,
        scope: &Scope,
        path: &str,
        max_bytes: usize,
    ) -> Result<Option<StoredObject>> {
        if max_bytes == 0 {
            return Err(StoreError::Invalid("read budget must be positive".into()));
        }
        let (root, control) = self.roots(scope)?;
        Self::read_file(&root, &control, path, true, Some(max_bytes))
    }

    async fn list(
        &self,
        scope: &Scope,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ObjectPage> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        if let Some(cursor) = cursor {
            validate_path(cursor)?;
        }
        let (root, _) = self.roots(scope)?;
        Self::page_paths(&root, prefix, cursor, limit)
    }

    async fn put(
        &self,
        scope: &Scope,
        path: &str,
        bytes: &[u8],
        content_type: &str,
        condition: WriteCondition,
        operation_id: Option<&str>,
    ) -> Result<StoredObject> {
        let (root, control) = self.roots(scope)?;
        Self::publish(
            (&root, &control),
            path,
            bytes,
            content_type,
            condition,
            operation_id,
            true,
        )
    }

    async fn get_control(&self, scope: &Scope, path: &str) -> Result<Option<StoredObject>> {
        let (_, control) = self.roots(scope)?;
        Self::read_file(&control.join("journal"), &control, path, false, None)
    }

    async fn delete(&self, scope: &Scope, path: &str, expected_revision: &str) -> Result<()> {
        let (root, control) = self.roots(scope)?;
        let _lock = Self::lock(&control)?;
        let current = Self::read_file(&root, &control, path, true, None)?;
        if !current.is_some_and(|object| object.revision == expected_revision) {
            return Err(StoreError::Conflict);
        }
        let full = Self::safe_join(&root, path)?;
        fs::remove_file(&full)?;
        File::open(full.parent().unwrap())?.sync_all()?;
        Self::remove_object_metadata(&control, path)?;
        Ok(())
    }

    async fn list_control(
        &self,
        scope: &Scope,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ObjectPage> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        if let Some(cursor) = cursor {
            validate_path(cursor)?;
        }
        let (_, control) = self.roots(scope)?;
        Self::page_paths(&control.join("journal"), prefix, cursor, limit)
    }

    async fn delete_control(
        &self,
        scope: &Scope,
        path: &str,
        expected_revision: &str,
    ) -> Result<()> {
        let (_, control) = self.roots(scope)?;
        let journal = control.join("journal");
        let _lock = Self::lock(&control)?;
        let current = Self::read_file(&journal, &control, path, false, None)?;
        if !current.is_some_and(|object| object.revision == expected_revision) {
            return Err(StoreError::Conflict);
        }
        let full = Self::safe_join(&journal, path)?;
        fs::remove_file(&full)?;
        File::open(full.parent().unwrap())?.sync_all()?;
        Ok(())
    }

    async fn put_control(
        &self,
        scope: &Scope,
        path: &str,
        bytes: &[u8],
        condition: WriteCondition,
    ) -> Result<StoredObject> {
        let (_, control) = self.roots(scope)?;
        Self::publish(
            (&control.join("journal"), &control),
            path,
            bytes,
            "application/json",
            condition,
            None,
            false,
        )
    }
}
