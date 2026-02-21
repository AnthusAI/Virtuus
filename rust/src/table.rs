//! Table storage implementation.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::gsi::Gsi;
use crate::sort::SortCondition;

type Hook = Box<dyn Fn(&Value) + Send + Sync>;

/// Primary key representation for simple and composite keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableKey {
    /// Simple primary key.
    Simple(String),
    /// Composite primary key (partition, sort).
    Composite(String, String),
}

/// Association definition between tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Association {
    /// Parent lookup by foreign key.
    BelongsTo {
        target_table: String,
        foreign_key: String,
    },
    /// Child lookup via target table GSI.
    HasMany { target_table: String, index: String },
    /// Many-to-many through a junction table.
    HasManyThrough {
        through_table: String,
        through_index: String,
        target_table: String,
        target_foreign_key: String,
    },
}

/// Table with primary key, optional GSIs, and file persistence.
pub struct Table {
    name: String,
    primary_key: Option<String>,
    partition_key: Option<String>,
    sort_key: Option<String>,
    directory: Option<PathBuf>,
    validation: ValidationMode,
    records: HashMap<TableKey, Value>,
    gsis: HashMap<String, Gsi>,
    association_defs: HashMap<String, Association>,
    warnings: Vec<String>,
    hook_errors: Vec<String>,
    on_put: Vec<Hook>,
    on_delete: Vec<Hook>,
    on_refresh: Vec<Hook>,
    last_write_used_atomic: bool,
    associations: Vec<String>,
    check_interval: Duration,
    auto_refresh: bool,
    manifest: HashMap<String, SystemTime>,
    last_dir_mtime: Option<SystemTime>,
    last_check_time: Option<SystemTime>,
    last_is_stale: bool,
    pub last_change_summary: ChangeSummary,
}

/// Summary of changes detected during refresh or check.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeSummary {
    pub added: usize,
    pub modified: usize,
    pub deleted: usize,
    pub reread: usize,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("primary_key", &self.primary_key)
            .field("partition_key", &self.partition_key)
            .field("sort_key", &self.sort_key)
            .field("directory", &self.directory)
            .field("validation", &self.validation)
            .field("records", &self.records)
            .field("gsis", &self.gsis)
            .field("association_defs", &self.association_defs)
            .field("warnings", &self.warnings)
            .field("hook_errors", &self.hook_errors)
            .field("last_write_used_atomic", &self.last_write_used_atomic)
            .field("associations", &self.associations)
            .finish()
    }
}

/// Validation mode for record inserts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationMode {
    /// Ignore validation errors.
    Silent,
    /// Record validation warnings.
    Warn,
    /// Raise validation errors.
    Error,
}

impl Table {
    /// Create a new table.
    pub fn new(
        name: &str,
        primary_key: Option<&str>,
        partition_key: Option<&str>,
        sort_key: Option<&str>,
        directory: Option<PathBuf>,
        validation: ValidationMode,
    ) -> Self {
        if primary_key.is_none() && partition_key.is_none() {
            panic!("primary_key or partition_key is required");
        }
        if primary_key.is_some() && partition_key.is_some() {
            panic!("use either primary_key or partition_key");
        }
        if partition_key.is_some() && sort_key.is_none() {
            panic!("sort_key is required for composite primary keys");
        }
        Self {
            name: name.to_string(),
            primary_key: primary_key.map(|s| s.to_string()),
            partition_key: partition_key.map(|s| s.to_string()),
            sort_key: sort_key.map(|s| s.to_string()),
            directory,
            validation,
            records: HashMap::new(),
            gsis: HashMap::new(),
            association_defs: HashMap::new(),
            warnings: Vec::new(),
            hook_errors: Vec::new(),
            on_put: Vec::new(),
            on_delete: Vec::new(),
            on_refresh: Vec::new(),
            last_write_used_atomic: false,
            associations: Vec::new(),
            check_interval: Duration::from_secs(0),
            auto_refresh: true,
            manifest: HashMap::new(),
            last_dir_mtime: None,
            last_check_time: None,
            last_is_stale: false,
            last_change_summary: ChangeSummary::default(),
        }
    }

    /// Register a GSI.
    pub fn add_gsi(&mut self, name: &str, partition_key: &str, sort_key: Option<&str>) {
        self.gsis
            .insert(name.to_string(), Gsi::new(name, partition_key, sort_key));
    }

    /// Insert or update a record.
    pub fn put(&mut self, record: Value) {
        let key = match self.extract_key(&record) {
            Some(key) => key,
            None => return,
        };
        self.validate_gsi_fields(&record);
        if let Some(existing) = self.records.get(&key).cloned() {
            self.remove_from_gsis(&key, &existing);
        }
        self.records.insert(key.clone(), record.clone());
        self.index_in_gsis(&key, &record);
        if let Some(dir) = self.directory.clone() {
            self.write_record(&dir, &key, &record);
        }
        let hooks = self.on_put.as_slice();
        let hook_errors = &mut self.hook_errors;
        Self::fire_hooks(hooks, hook_errors, &record);
    }

    /// Get a record by primary key.
    pub fn get(&self, pk: &str, sort: Option<&str>) -> Option<Value> {
        let key = self.compose_key(pk, sort);
        self.records.get(&key).cloned()
    }

    /// Delete a record by primary key.
    pub fn delete(&mut self, pk: &str, sort: Option<&str>) {
        let key = self.compose_key(pk, sort);
        let record = match self.records.remove(&key) {
            Some(record) => record,
            None => return,
        };
        self.remove_from_gsis(&key, &record);
        if let Some(dir) = self.directory.clone() {
            self.delete_record(&dir, &key);
        }
        let hooks = self.on_delete.as_slice();
        let hook_errors = &mut self.hook_errors;
        Self::fire_hooks(hooks, hook_errors, &record);
    }

    /// Return all records.
    pub fn scan(&mut self) -> Vec<Value> {
        self.maybe_refresh_before_query();
        self.records.values().cloned().collect()
    }

    /// Bulk load multiple records.
    pub fn bulk_load(&mut self, records: Vec<Value>) {
        for record in records {
            self.put(record);
        }
    }

    /// Count records in table or GSI partition.
    pub fn count(&self, index: Option<&str>, value: Option<&Value>) -> usize {
        match index {
            None => self.records.len(),
            Some(name) => match self.gsis.get(name) {
                Some(gsi) => gsi.query(value.unwrap_or(&Value::Null), None, false).len(),
                None => 0,
            },
        }
    }

    /// Describe table metadata.
    pub fn describe(&self) -> Value {
        let mut data = serde_json::Map::new();
        data.insert("name".to_string(), Value::String(self.name.clone()));
        data.insert(
            "record_count".to_string(),
            Value::Number(self.records.len().into()),
        );
        data.insert(
            "gsis".to_string(),
            Value::Array(self.gsis.keys().map(|k| Value::String(k.clone())).collect()),
        );
        data.insert(
            "associations".to_string(),
            Value::Array(
                self.associations
                    .iter()
                    .map(|k| Value::String(k.clone()))
                    .collect(),
            ),
        );
        if let Some(pk) = &self.primary_key {
            data.insert("primary_key".to_string(), Value::String(pk.clone()));
        } else {
            data.insert(
                "partition_key".to_string(),
                Value::String(self.partition_key.clone().unwrap_or_default()),
            );
            data.insert(
                "sort_key".to_string(),
                Value::String(self.sort_key.clone().unwrap_or_default()),
            );
        }
        Value::Object(data)
    }

    /// Return a reference to warnings.
    pub fn warnings(&self) -> &Vec<String> {
        &self.warnings
    }

    /// Return a reference to hook errors.
    pub fn hook_errors(&self) -> &Vec<String> {
        &self.hook_errors
    }

    /// Return GSIs.
    pub fn gsis(&self) -> &HashMap<String, Gsi> {
        &self.gsis
    }

    /// Return whether the last write used atomic rename.
    pub fn last_write_used_atomic(&self) -> bool {
        self.last_write_used_atomic
    }

    /// Register an association name for describe output only.
    pub fn add_association(&mut self, name: &str) {
        if !self.associations.contains(&name.to_string()) {
            self.associations.push(name.to_string());
        }
    }

    /// Register a belongs_to association definition.
    pub fn add_belongs_to(&mut self, name: &str, target_table: &str, foreign_key: &str) {
        self.register_association(
            name,
            Association::BelongsTo {
                target_table: target_table.to_string(),
                foreign_key: foreign_key.to_string(),
            },
        );
    }

    /// Register a has_many association definition.
    pub fn add_has_many(&mut self, name: &str, target_table: &str, index: &str) {
        self.register_association(
            name,
            Association::HasMany {
                target_table: target_table.to_string(),
                index: index.to_string(),
            },
        );
    }

    /// Register a has_many_through association definition.
    pub fn add_has_many_through(
        &mut self,
        name: &str,
        through_table: &str,
        through_index: &str,
        target_table: &str,
        target_foreign_key: &str,
    ) {
        self.register_association(
            name,
            Association::HasManyThrough {
                through_table: through_table.to_string(),
                through_index: through_index.to_string(),
                target_table: target_table.to_string(),
                target_foreign_key: target_foreign_key.to_string(),
            },
        );
    }

    /// Retrieve an association definition.
    pub fn association(&self, name: &str) -> Option<&Association> {
        self.association_defs.get(name)
    }

    /// Register an on_put hook.
    pub fn register_on_put(&mut self, hook: Hook) {
        self.on_put.push(hook);
    }

    /// Register an on_delete hook.
    pub fn register_on_delete(&mut self, hook: Hook) {
        self.on_delete.push(hook);
    }

    /// Register an on_refresh hook.
    pub fn register_on_refresh(&mut self, hook: Hook) {
        self.on_refresh.push(hook);
    }

    /// Configure minimum seconds between staleness checks.
    pub fn set_check_interval(&mut self, seconds: u64) {
        self.check_interval = Duration::from_secs(seconds);
    }

    /// Enable or disable auto refresh on queries.
    pub fn set_auto_refresh(&mut self, enabled: bool) {
        self.auto_refresh = enabled;
    }

    /// Mark last staleness check time manually (useful for tests).
    pub fn mark_checked_now(&mut self, stale: bool) {
        self.last_check_time = Some(SystemTime::now());
        self.last_is_stale = stale;
    }

    /// Return the primary or partition key field name.
    pub fn key_field(&self) -> Option<&str> {
        self.primary_key
            .as_deref()
            .or(self.partition_key.as_deref())
    }

    /// Return associations list.
    pub fn associations(&self) -> &Vec<String> {
        &self.associations
    }

    /// Return association definitions map.
    pub fn association_defs(&self) -> &HashMap<String, Association> {
        &self.association_defs
    }

    /// Directory path if file-backed.
    pub fn directory(&self) -> Option<&PathBuf> {
        self.directory.as_ref()
    }

    /// Query GSI and return full records.
    pub fn query_gsi(
        &mut self,
        name: &str,
        partition_value: &Value,
        sort_condition: Option<&SortCondition>,
        descending: bool,
    ) -> Vec<Value> {
        self.maybe_refresh_before_query();
        let gsi = self.gsis.get(name).expect("GSI does not exist");
        let mut result = Vec::new();
        for pk in gsi.query(partition_value, sort_condition, descending) {
            let key = self.key_from_string(&pk);
            if let Some(record) = self.records.get(&key) {
                result.push(record.clone());
            }
        }
        result
    }

    /// Determine whether on-disk files are stale relative to the manifest.
    pub fn is_stale(&mut self, force_scan: bool) -> bool {
        if self.directory.is_none() {
            return false;
        }
        let now = SystemTime::now();
        if !force_scan {
            if let Some(last_check) = self.last_check_time {
                if self.check_interval > Duration::from_secs(0)
                    && now.duration_since(last_check).unwrap_or_default() < self.check_interval
                {
                    return self.last_is_stale;
                }
            }
        }
        let (summary, _, _, _) = self.compute_changes();
        self.last_check_time = Some(now);
        self.last_is_stale = summary.added + summary.modified + summary.deleted > 0;
        self.last_dir_mtime = self.dir_mtime();
        self.last_is_stale
    }

    /// Dry-run change detection without mutating the table.
    pub fn check(&self) -> ChangeSummary {
        let (summary, _, _, _) = self.compute_changes();
        summary
    }

    /// Incrementally refresh from disk.
    pub fn refresh(&mut self) -> ChangeSummary {
        if self.directory.is_none() {
            return ChangeSummary::default();
        }
        let (mut summary, added, modified, deleted) = self.compute_changes();
        let mut reread = 0;
        for path in added.iter().chain(modified.iter()) {
            if let Some(record) = self.read_record(path) {
                self.put(record);
                reread += 1;
            }
        }
        for path in deleted {
            if let Some(key) = self.key_from_filename(path) {
                match key {
                    TableKey::Simple(pk) => {
                        self.delete(&pk, None);
                    }
                    TableKey::Composite(partition, sort) => {
                        self.delete(&partition, Some(&sort));
                    }
                }
            }
        }
        self.manifest = self
            .iter_json_files()
            .into_iter()
            .filter_map(|path| {
                let name = path.file_name()?.to_string_lossy().to_string();
                let mtime = fs::metadata(&path).ok()?.modified().ok()?;
                Some((name, mtime))
            })
            .collect();
        self.last_dir_mtime = self.dir_mtime();
        self.last_check_time = Some(SystemTime::now());
        self.last_is_stale = false;
        summary.reread = reread;
        self.last_change_summary = summary.clone();
        let hooks = self.on_refresh.as_slice();
        let hook_errors = &mut self.hook_errors;
        let summary_value = serde_json::json!({
            "added": summary.added,
            "modified": summary.modified,
            "deleted": summary.deleted,
            "reread": summary.reread
        });
        Self::fire_hooks(hooks, hook_errors, &summary_value);
        summary
    }

    /// Proactively refresh regardless of staleness.
    pub fn warm(&mut self) {
        if self.directory.is_none() {
            return;
        }
        self.refresh();
    }

    /// Load records from directory.
    pub fn load_from_dir(&mut self, directory: Option<PathBuf>) {
        let dir = directory.or_else(|| self.directory.clone());
        let dir = match dir {
            Some(d) => d,
            None => panic!("directory is required"),
        };
        if !dir.exists() {
            return;
        }
        for entry in fs::read_dir(&dir).expect("read_dir failed") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let data = fs::read_to_string(&path).expect("read file");
            let record: Value = serde_json::from_str(&data).expect("parse json");
            self.put(record);
            if let Ok(meta) = fs::metadata(&path) {
                if let Ok(mtime) = meta.modified() {
                    if let Some(name) = path.file_name() {
                        self.manifest
                            .insert(name.to_string_lossy().to_string(), mtime);
                    }
                }
            }
        }
        self.last_dir_mtime = self.dir_mtime();
        self.last_check_time = Some(SystemTime::now());
        self.last_is_stale = false;
    }

    /// Export records to directory.
    pub fn export(&mut self, directory: PathBuf) {
        fs::create_dir_all(&directory).expect("create export dir");
        let records: Vec<(TableKey, Value)> = self
            .records
            .iter()
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect();
        for (key, record) in records {
            self.validate_pk_for_path(&key);
            let filename = self.filename_for_key(&key);
            let path = directory.join(filename);
            self.write_json_atomic(&path, &record);
        }
    }

    fn extract_key(&mut self, record: &Value) -> Option<TableKey> {
        if let Some(pk) = &self.primary_key {
            match record.get(pk) {
                Some(value) => return Some(TableKey::Simple(value_to_string(value))),
                None => return self.handle_validation(&format!("missing primary key {pk}")),
            }
        }
        let partition_key = self.partition_key.as_ref().expect("partition key");
        let sort_key = self.sort_key.as_ref().expect("sort key");
        let partition = match record.get(partition_key) {
            Some(value) => value,
            None => return self.handle_validation("missing composite primary key"),
        };
        let sort = match record.get(sort_key) {
            Some(value) => value,
            None => return self.handle_validation("missing composite primary key"),
        };
        Some(TableKey::Composite(
            value_to_string(partition),
            value_to_string(sort),
        ))
    }

    fn compose_key(&self, pk: &str, sort: Option<&str>) -> TableKey {
        if self.primary_key.is_some() {
            return TableKey::Simple(pk.to_string());
        }
        let sort = sort.expect("sort key required");
        TableKey::Composite(pk.to_string(), sort.to_string())
    }

    fn index_in_gsis(&mut self, key: &TableKey, record: &Value) {
        let pk = key_to_string(key);
        for gsi in self.gsis.values_mut() {
            gsi.put(&pk, record);
        }
    }

    fn remove_from_gsis(&mut self, key: &TableKey, record: &Value) {
        let pk = key_to_string(key);
        for gsi in self.gsis.values_mut() {
            gsi.remove(&pk, record);
        }
    }

    fn maybe_refresh_before_query(&mut self) {
        if self.directory.is_none() || !self.auto_refresh {
            return;
        }
        if self.is_stale(false) {
            self.refresh();
        }
    }

    fn key_from_string(&self, value: &str) -> TableKey {
        if self.primary_key.is_some() {
            return TableKey::Simple(value.to_string());
        }
        let mut parts = value.splitn(2, "__");
        let partition = parts.next().unwrap_or_default();
        let sort = parts.next().unwrap_or_default();
        TableKey::Composite(partition.to_string(), sort.to_string())
    }

    fn key_from_filename(&self, path: PathBuf) -> Option<TableKey> {
        let name = path.file_name()?.to_string_lossy().replace(".json", "");
        if name.contains("__") {
            let mut parts = name.splitn(2, "__");
            let partition = parts.next().unwrap_or_default().to_string();
            let sort = parts.next().unwrap_or_default().to_string();
            Some(TableKey::Composite(partition, sort))
        } else {
            Some(TableKey::Simple(name))
        }
    }

    fn validate_gsi_fields(&mut self, record: &Value) {
        let fields: Vec<(String, Option<String>)> = self
            .gsis
            .values()
            .map(|gsi| {
                (
                    gsi.partition_key().to_string(),
                    gsi.sort_key().map(|key| key.to_string()),
                )
            })
            .collect();
        for (partition_key, sort_key) in fields {
            if record.get(&partition_key).is_none() {
                let _ = self.handle_validation(&format!("missing GSI field {partition_key}"));
            }
            if let Some(sort_key) = sort_key {
                if record.get(&sort_key).is_none() {
                    let _ = self.handle_validation(&format!("missing GSI field {sort_key}"));
                }
            }
        }
    }

    fn handle_validation(&mut self, message: &str) -> Option<TableKey> {
        match self.validation {
            ValidationMode::Silent => None,
            ValidationMode::Warn => {
                self.warnings.push(message.to_string());
                None
            }
            ValidationMode::Error => panic!("{message}"),
        }
    }

    fn filename_for_key(&self, key: &TableKey) -> String {
        match key {
            TableKey::Simple(pk) => format!("{pk}.json"),
            TableKey::Composite(partition, sort) => format!("{partition}__{sort}.json"),
        }
    }

    fn write_record(&mut self, directory: &Path, key: &TableKey, record: &Value) {
        self.validate_pk_for_path(key);
        fs::create_dir_all(directory).expect("create directory");
        let filename = self.filename_for_key(key);
        let path = directory.join(filename);
        self.write_json_atomic(&path, record);
        if let Ok(meta) = fs::metadata(&path) {
            if let Ok(mtime) = meta.modified() {
                self.manifest.insert(
                    path.file_name().unwrap().to_string_lossy().to_string(),
                    mtime,
                );
                self.last_dir_mtime = self.dir_mtime();
            }
        }
    }

    fn delete_record(&mut self, directory: &Path, key: &TableKey) {
        self.validate_pk_for_path(key);
        let filename = self.filename_for_key(key);
        let path = directory.join(&filename);
        if path.exists() {
            fs::remove_file(path).expect("remove file");
            self.manifest.remove(&filename);
            self.last_dir_mtime = self.dir_mtime();
        }
    }

    fn validate_pk_for_path(&self, key: &TableKey) {
        let parts = match key {
            TableKey::Simple(pk) => vec![pk.as_str()],
            TableKey::Composite(partition, sort) => vec![partition.as_str(), sort.as_str()],
        };
        for part in parts {
            if part.contains('/') || part.contains('\\') {
                panic!("invalid PK characters");
            }
        }
    }

    fn write_json_atomic(&mut self, path: &Path, record: &Value) {
        let directory = path.parent().expect("parent dir");
        fs::create_dir_all(directory).expect("create dir");
        let temp_name = format!(
            ".tmp_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let temp_path = directory.join(temp_name);
        fs::write(&temp_path, serde_json::to_vec(record).unwrap()).expect("write temp");
        fs::rename(&temp_path, path).expect("rename");
        self.last_write_used_atomic = true;
    }

    fn register_association(&mut self, name: &str, association: Association) {
        self.add_association(name);
        self.association_defs.insert(name.to_string(), association);
    }

    fn fire_hooks(hooks: &[Hook], hook_errors: &mut Vec<String>, record: &Value) {
        for hook in hooks {
            if let Err(err) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook(record)))
            {
                hook_errors.push(format!("{:?}", err));
            }
        }
    }

    fn iter_json_files(&self) -> Vec<PathBuf> {
        if let Some(dir) = &self.directory {
            if let Ok(entries) = fs::read_dir(dir) {
                return entries
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("json"))
                    .collect();
            }
        }
        Vec::new()
    }

    fn dir_mtime(&self) -> Option<SystemTime> {
        self.directory
            .as_ref()
            .and_then(|dir| fs::metadata(dir).ok())
            .and_then(|meta| meta.modified().ok())
    }

    fn compute_changes(&self) -> (ChangeSummary, Vec<PathBuf>, Vec<PathBuf>, Vec<PathBuf>) {
        if self.directory.is_none() {
            return (ChangeSummary::default(), vec![], vec![], vec![]);
        }
        let current_files: HashMap<String, SystemTime> = self
            .iter_json_files()
            .into_iter()
            .filter_map(|path| {
                let name = path.file_name()?.to_string_lossy().to_string();
                let mtime = fs::metadata(&path).ok()?.modified().ok()?;
                Some((name, mtime))
            })
            .collect();
        let mut added = Vec::new();
        let mut deleted = Vec::new();
        let mut modified = Vec::new();
        for (name, mtime) in current_files.iter() {
            match self.manifest.get(name) {
                None => added.push(self.directory.as_ref().unwrap().join(name)),
                Some(prev_mtime) => {
                    if prev_mtime != mtime {
                        modified.push(self.directory.as_ref().unwrap().join(name));
                    }
                }
            }
        }
        for name in self.manifest.keys() {
            if !current_files.contains_key(name) {
                deleted.push(self.directory.as_ref().unwrap().join(name));
            }
        }
        let summary = ChangeSummary {
            added: added.len(),
            modified: modified.len(),
            deleted: deleted.len(),
            reread: 0,
        };
        (summary, added, modified, deleted)
    }

    fn read_record(&self, path: &PathBuf) -> Option<Value> {
        fs::read_to_string(path)
            .ok()
            .and_then(|data| serde_json::from_str(&data).ok())
    }
}

fn key_to_string(key: &TableKey) -> String {
    match key {
        TableKey::Simple(pk) => pk.clone(),
        TableKey::Composite(partition, sort) => format!("{partition}__{sort}"),
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn temp_dir(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "virtuus_{name}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        dir
    }

    #[test]
    fn create_simple_table() {
        let table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        assert_eq!(table.primary_key, Some("id".to_string()));
    }

    #[test]
    fn create_composite_table() {
        let table = Table::new(
            "scores",
            None,
            Some("user_id"),
            Some("game_id"),
            None,
            ValidationMode::Silent,
        );
        assert_eq!(table.partition_key, Some("user_id".to_string()));
        assert_eq!(table.sort_key, Some("game_id".to_string()));
    }

    #[test]
    fn debug_format_includes_name() {
        let table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        let output = format!("{table:?}");
        assert!(output.contains("users"));
    }

    #[test]
    #[should_panic]
    fn new_requires_primary_or_partition_key() {
        let _table = Table::new("users", None, None, None, None, ValidationMode::Silent);
    }

    #[test]
    #[should_panic]
    fn new_disallows_both_primary_and_partition_key() {
        let _table = Table::new(
            "users",
            Some("id"),
            Some("pk"),
            None,
            None,
            ValidationMode::Silent,
        );
    }

    #[test]
    #[should_panic]
    fn new_requires_sort_key_for_composite() {
        let _table = Table::new(
            "scores",
            None,
            Some("user_id"),
            None,
            None,
            ValidationMode::Silent,
        );
    }

    #[test]
    fn put_get_delete_simple() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user-1", "name": "Alice"}));
        assert_eq!(table.get("user-1", None).unwrap()["name"], "Alice");
        table.delete("user-1", None);
        assert!(table.get("user-1", None).is_none());
    }

    #[test]
    fn delete_missing_record_is_noop() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.delete("missing", None);
        assert_eq!(table.count(None, None), 0);
    }

    #[test]
    fn put_get_delete_composite() {
        let mut table = Table::new(
            "scores",
            None,
            Some("user_id"),
            Some("game_id"),
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"user_id": "user-1", "game_id": "game-A", "score": 100}));
        assert!(table.get("user-1", Some("game-A")).is_some());
        table.delete("user-1", Some("game-A"));
        assert!(table.get("user-1", Some("game-A")).is_none());
    }

    #[test]
    fn describe_includes_composite_keys() {
        let table = Table::new(
            "scores",
            None,
            Some("user_id"),
            Some("game_id"),
            None,
            ValidationMode::Silent,
        );
        let desc = table.describe();
        assert_eq!(desc["partition_key"], "user_id");
        assert_eq!(desc["sort_key"], "game_id");
    }

    #[test]
    fn upsert_overwrites() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user-1", "name": "Alice"}));
        table.put(json!({"id": "user-1", "name": "Alice Updated"}));
        assert_eq!(table.get("user-1", None).unwrap()["name"], "Alice Updated");
    }

    #[test]
    fn accessors_return_state() {
        let mut table = Table::new("users", Some("id"), None, None, None, ValidationMode::Warn);
        table.add_gsi("by_status", "status", None);
        table.put(json!({"name": "Missing"}));
        table.register_on_put(Box::new(|_| panic!("hook failure")));
        table.put(json!({"id": "user-1"}));
        assert!(!table.warnings().is_empty());
        assert!(!table.hook_errors().is_empty());
        assert_eq!(table.gsis().len(), 1);
    }

    #[test]
    fn scan_returns_all() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user-1"}));
        table.put(json!({"id": "user-2"}));
        assert_eq!(table.scan().len(), 2);
    }

    #[test]
    fn validation_silent_ignores_missing_key() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"name": "Alice"}));
        assert!(table.warnings.is_empty());
        assert_eq!(table.count(None, None), 0);
    }

    #[test]
    fn bulk_load_inserts() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        let records = vec![json!({"id": "user-1"}), json!({"id": "user-2"})];
        table.bulk_load(records);
        assert_eq!(table.count(None, None), 2);
    }

    #[test]
    fn composite_missing_keys_warn() {
        let mut table = Table::new(
            "scores",
            None,
            Some("user_id"),
            Some("game_id"),
            None,
            ValidationMode::Warn,
        );
        table.put(json!({"game_id": "game-A"}));
        table.put(json!({"user_id": "user-1"}));
        assert!(table.warnings.len() >= 2);
    }

    #[test]
    fn gsi_missing_fields_warn() {
        let mut table = Table::new("users", Some("id"), None, None, None, ValidationMode::Warn);
        table.add_gsi("by_email", "email", Some("created_at"));
        table.put(json!({"id": "user-1"}));
        assert!(table.warnings.len() >= 2);
    }

    #[test]
    fn count_gsi_partition() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.add_gsi("by_status", "status", None);
        table.put(json!({"id": "user-1", "status": "active"}));
        table.put(json!({"id": "user-2", "status": "active"}));
        let count = table.count(Some("by_status"), Some(&json!("active")));
        assert_eq!(count, 2);
    }

    #[test]
    fn describe_contains_fields() {
        let table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        let desc = table.describe();
        assert_eq!(desc["name"], "users");
        assert_eq!(desc["primary_key"], "id");
    }

    #[test]
    #[should_panic]
    fn load_requires_directory() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.load_from_dir(None);
    }

    #[test]
    fn load_missing_directory_is_noop() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "virtuus_missing_{}",
            std::time::SystemTime::now().elapsed().unwrap().as_nanos()
        ));
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.load_from_dir(Some(path));
        assert_eq!(table.count(None, None), 0);
    }

    #[test]
    fn load_ignores_non_json_files() {
        let dir = temp_dir("load_non_json");
        fs::create_dir_all(&dir).unwrap();
        let json_path = dir.join("user-1.json");
        let txt_path = dir.join("note.txt");
        fs::write(&json_path, json!({"id": "user-1"}).to_string()).unwrap();
        fs::write(&txt_path, "ignore").unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.load_from_dir(Some(dir.clone()));
        assert_eq!(table.count(None, None), 1);
        assert!(txt_path.exists());
    }

    #[test]
    fn validation_warns() {
        let mut table = Table::new("users", Some("id"), None, None, None, ValidationMode::Warn);
        table.put(json!({"name": "Alice"}));
        assert!(!table.warnings.is_empty());
    }

    #[test]
    #[should_panic]
    fn validation_errors() {
        let mut table = Table::new("users", Some("id"), None, None, None, ValidationMode::Error);
        table.put(json!({"name": "Alice"}));
    }

    #[test]
    fn gsi_maintained_on_put_delete() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.add_gsi("by_status", "status", None);
        table.put(json!({"id": "user-1", "status": "active"}));
        assert_eq!(
            table.gsis["by_status"]
                .query(&json!("active"), None, false)
                .len(),
            1
        );
        table.delete("user-1", None);
        assert_eq!(
            table.gsis["by_status"]
                .query(&json!("active"), None, false)
                .len(),
            0
        );
    }

    #[test]
    fn file_persistence_round_trip() {
        let dir = temp_dir("persist");
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user-1", "name": "Alice"}));
        let mut loaded = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        loaded.load_from_dir(None);
        assert_eq!(loaded.count(None, None), 1);
    }

    #[test]
    fn delete_removes_persisted_file() {
        let dir = temp_dir("delete_file");
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user-1"}));
        table.delete("user-1", None);
        let path = dir.join("user-1.json");
        assert!(!path.exists());
    }

    #[test]
    fn export_writes_files() {
        let export_dir = temp_dir("export");
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user-1", "name": "Alice"}));
        table.export(export_dir.clone());
        let files = fs::read_dir(export_dir).unwrap().count();
        assert_eq!(files, 1);
    }

    #[test]
    fn export_composite_writes_filename() {
        let export_dir = temp_dir("export_composite");
        let mut table = Table::new(
            "scores",
            None,
            Some("user_id"),
            Some("game_id"),
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"user_id": "user-1", "game_id": "game-A"}));
        table.export(export_dir.clone());
        let path = export_dir.join("user-1__game-A.json");
        assert!(path.exists());
    }

    #[test]
    fn refresh_without_directory_returns_default() {
        let mut table = Table::new(
            "items",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        let summary = table.refresh();
        assert_eq!(summary.added + summary.deleted + summary.modified, 0);
    }

    #[test]
    fn refresh_detects_deleted_files() {
        let dir = temp_dir("table_refresh_deleted");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("u1.json"), r#"{"id":"u1"}"#).unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.load_from_dir(None);
        assert!(table.get("u1", None).is_some());
        fs::remove_file(dir.join("u1.json")).unwrap();
        let summary = table.refresh();
        assert_eq!(summary.deleted, 1);
        assert!(table.get("u1", None).is_none());
    }

    #[test]
    fn refresh_deletes_composite_records() {
        let dir = temp_dir("table_refresh_composite_deleted");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("p__s.json"), r#"{"partition":"p","sort":"s"}"#).unwrap();
        let mut table = Table::new(
            "items",
            None,
            Some("partition"),
            Some("sort"),
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.load_from_dir(None);
        assert_eq!(table.scan().len(), 1);
        fs::remove_file(dir.join("p__s.json")).unwrap();
        let summary = table.refresh();
        assert_eq!(summary.deleted, 1);
        assert!(table.scan().is_empty());
    }

    #[test]
    fn warm_without_directory_is_noop() {
        let mut table = Table::new(
            "items",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.warm();
        assert_eq!(table.scan().len(), 0);
    }

    #[test]
    fn maybe_refresh_before_query_refreshes_when_stale() {
        let dir = temp_dir("table_maybe_refresh");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("u1.json"), r#"{"id":"u1"}"#).unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        let records = table.scan();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn key_from_filename_parses_composite_and_simple() {
        let dir = temp_dir("table_key_from_filename");
        let table = Table::new(
            "items",
            None,
            Some("pk"),
            Some("sk"),
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        let composite = table
            .key_from_filename(PathBuf::from("pk__sk.json"))
            .unwrap();
        match composite {
            TableKey::Composite(pk, sk) => {
                assert_eq!(pk, "pk");
                assert_eq!(sk, "sk");
            }
            _ => panic!("expected composite key"),
        }
        let simple_table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        let simple = simple_table
            .key_from_filename(PathBuf::from("u1.json"))
            .unwrap();
        match simple {
            TableKey::Simple(pk) => assert_eq!(pk, "u1"),
            _ => panic!("expected simple key"),
        }
    }

    #[test]
    fn export_sets_atomic_flag() {
        let export_dir = temp_dir("export_atomic");
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user-1", "name": "Alice"}));
        table.export(export_dir);
        assert!(table.last_write_used_atomic());
    }

    #[test]
    fn numeric_primary_key_is_stringified() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.put(json!({"id": 123, "name": "Alice"}));
        let record = table.get("123", None).unwrap();
        assert_eq!(record["name"], "Alice");
    }

    #[test]
    fn query_gsi_returns_records() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.add_gsi("by_status", "status", None);
        table.put(json!({"id": "user-1", "status": "active"}));
        let results = table.query_gsi("by_status", &json!("active"), None, false);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["id"], "user-1");
    }

    #[test]
    fn query_gsi_composite_keys() {
        let mut table = Table::new(
            "scores",
            None,
            Some("user_id"),
            Some("game_id"),
            None,
            ValidationMode::Silent,
        );
        table.add_gsi("by_status", "status", None);
        table.put(json!({"user_id": "user-1", "game_id": "game-A", "status": "active"}));
        let results = table.query_gsi("by_status", &json!("active"), None, false);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["game_id"], "game-A");
    }

    #[test]
    fn hook_errors_capture_panics() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.register_on_put(Box::new(|_| panic!("boom")));
        table.put(json!({"id": "user-1"}));
        assert!(!table.hook_errors.is_empty());
    }

    #[test]
    fn count_missing_gsi_returns_zero() {
        let table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        let count = table.count(Some("missing"), Some(&json!("active")));
        assert_eq!(count, 0);
    }

    #[test]
    fn describe_lists_associations() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.add_association("posts");
        let desc = table.describe();
        let associations = desc["associations"].as_array().cloned().unwrap_or_default();
        assert!(associations
            .iter()
            .any(|value| value.as_str() == Some("posts")));
    }

    #[test]
    fn resolves_belongs_to_association() {
        let mut db = Database::new();
        let mut posts = Table::new(
            "posts",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        posts.add_belongs_to("author", "users", "user_id");
        posts.put(json!({"id": "post-1", "user_id": "user-1"}));
        let mut users = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        users.put(json!({"id": "user-1", "name": "Alice"}));
        db.add_table("posts", posts);
        db.add_table("users", users);
        let result = db.resolve_association("posts", "author", "post-1");
        assert_eq!(result["id"], "user-1");
    }

    #[test]
    fn resolves_has_many_association() {
        let mut db = Database::new();
        let mut users = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        users.add_has_many("posts", "posts", "by_user");
        let mut posts = Table::new(
            "posts",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        posts.add_gsi("by_user", "user_id", None);
        posts.put(json!({"id": "post-1", "user_id": "user-1"}));
        posts.put(json!({"id": "post-2", "user_id": "user-1"}));
        posts.put(json!({"id": "post-3", "user_id": "user-2"}));
        users.put(json!({"id": "user-1"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let result = db.resolve_association("users", "posts", "user-1");
        let array = result.as_array().unwrap();
        assert_eq!(array.len(), 2);
        let ids: Vec<String> = array
            .iter()
            .filter_map(|r| r.get("id"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        assert!(ids.contains(&"post-1".to_string()));
        assert!(ids.contains(&"post-2".to_string()));
    }

    #[test]
    fn resolves_has_many_through_association() {
        let mut db = Database::new();
        let mut jobs = Table::new("jobs", Some("id"), None, None, None, ValidationMode::Silent);
        jobs.add_has_many_through(
            "workers",
            "job_assignments",
            "by_job",
            "workers",
            "worker_id",
        );
        jobs.put(json!({"id": "job-1"}));

        let mut assignments = Table::new(
            "job_assignments",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        assignments.add_gsi("by_job", "job_id", None);
        assignments.put(json!({"id": "ja-1", "job_id": "job-1", "worker_id": "worker-1"}));
        assignments.put(json!({"id": "ja-2", "job_id": "job-1", "worker_id": "worker-2"}));

        let mut workers = Table::new(
            "workers",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        workers.put(json!({"id": "worker-1"}));
        workers.put(json!({"id": "worker-2"}));
        workers.put(json!({"id": "worker-3"}));

        db.add_table("jobs", jobs);
        db.add_table("job_assignments", assignments);
        db.add_table("workers", workers);

        let result = db.resolve_association("jobs", "workers", "job-1");
        let array = result.as_array().unwrap();
        let ids: Vec<String> = array
            .iter()
            .filter_map(|r| r.get("id"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        assert!(ids.contains(&"worker-1".to_string()));
        assert!(ids.contains(&"worker-2".to_string()));
        assert!(!ids.contains(&"worker-3".to_string()));
    }

    #[test]
    fn register_on_put_and_delete_hooks() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        let put_called = Arc::new(Mutex::new(false));
        let delete_called = Arc::new(Mutex::new(false));
        let put_clone = put_called.clone();
        let delete_clone = delete_called.clone();
        table.register_on_put(Box::new(move |_| {
            *put_clone.lock().unwrap() = true;
        }));
        table.register_on_delete(Box::new(move |_| {
            *delete_clone.lock().unwrap() = true;
        }));
        table.put(json!({"id": "user-1"}));
        table.delete("user-1", None);
        assert!(*put_called.lock().unwrap());
        assert!(*delete_called.lock().unwrap());
    }

    #[test]
    #[should_panic]
    fn invalid_pk_characters_panic() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(temp_dir("invalid_pk")),
            ValidationMode::Silent,
        );
        table.put(json!({"id": "user/1"}));
    }

    #[test]
    fn event_hooks_fire() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        let called = std::sync::Arc::new(std::sync::Mutex::new(false));
        let called_clone = called.clone();
        table.on_put.push(Box::new(move |_| {
            let mut guard = called_clone.lock().unwrap();
            *guard = true;
        }));
        table.put(json!({"id": "user-1"}));
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn cache_is_stale_detects_changes() {
        let dir = temp_dir("cache_stale");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("user-0.json");
        fs::write(
            &path,
            json!({"id": "user-0", "status": "active"}).to_string(),
        )
        .unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.load_from_dir(None);
        assert!(!table.is_stale(false));
        fs::write(
            &path,
            json!({"id": "user-0", "status": "inactive"}).to_string(),
        )
        .unwrap();
        assert!(table.is_stale(false));
    }

    #[test]
    fn cache_check_reports_without_refreshing() {
        let dir = temp_dir("cache_check");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            &dir.join("user-0.json"),
            json!({"id": "user-0"}).to_string(),
        )
        .unwrap();
        fs::write(
            &dir.join("user-1.json"),
            json!({"id": "user-1"}).to_string(),
        )
        .unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.load_from_dir(None);
        let before = table.count(None, None);
        fs::write(
            &dir.join("user-2.json"),
            json!({"id": "user-2"}).to_string(),
        )
        .unwrap();
        let summary = table.check();
        assert_eq!(summary.added, 1);
        assert_eq!(table.count(None, None), before);
    }

    #[test]
    fn cache_refresh_updates_gsi_and_records() {
        let dir = temp_dir("cache_refresh");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("user-0.json");
        fs::write(
            &path,
            json!({"id": "user-0", "status": "active"}).to_string(),
        )
        .unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.add_gsi("by_status", "status", None);
        table.load_from_dir(None);
        fs::write(
            &path,
            json!({"id": "user-0", "status": "inactive"}).to_string(),
        )
        .unwrap();
        let summary = table.refresh();
        assert_eq!(summary.modified, 1);
        assert_eq!(summary.reread, 1);
        assert_eq!(
            table
                .gsis()
                .get("by_status")
                .unwrap()
                .query(&json!("inactive"), None, false),
            vec!["user-0".to_string()]
        );
        assert_eq!(
            table.get("user-0", None).unwrap().get("status").unwrap(),
            "inactive"
        );
    }

    #[test]
    fn cache_check_interval_skips_recent_checks() {
        let dir = temp_dir("cache_interval");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("user-0.json");
        fs::write(&path, json!({"id": "user-0"}).to_string()).unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.set_check_interval(60);
        table.load_from_dir(None);
        table.mark_checked_now(false);
        fs::write(
            &path,
            json!({"id": "user-0", "name": "Updated"}).to_string(),
        )
        .unwrap();
        assert!(!table.is_stale(false));
    }

    #[test]
    fn cache_auto_refresh_can_be_disabled() {
        let dir = temp_dir("cache_auto");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            &dir.join("user-0.json"),
            json!({"id": "user-0"}).to_string(),
        )
        .unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        table.set_auto_refresh(false);
        table.load_from_dir(None);
        fs::write(
            &dir.join("user-1.json"),
            json!({"id": "user-1"}).to_string(),
        )
        .unwrap();
        let initial_ids: Vec<String> = table
            .scan()
            .iter()
            .filter_map(|r| r.get("id"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        assert!(!initial_ids.contains(&"user-1".to_string()));
        table.warm();
        let refreshed_ids: Vec<String> = table
            .scan()
            .iter()
            .filter_map(|r| r.get("id"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        assert!(refreshed_ids.contains(&"user-1".to_string()));
    }

    #[test]
    fn cache_on_refresh_hook_receives_summary() {
        let dir = temp_dir("cache_hook");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            &dir.join("user-0.json"),
            json!({"id": "user-0"}).to_string(),
        )
        .unwrap();
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        let calls = Arc::new(Mutex::new(Vec::<Value>::new()));
        let calls_clone = calls.clone();
        table.register_on_refresh(Box::new(move |summary| {
            calls_clone.lock().unwrap().push(summary.clone());
        }));
        table.load_from_dir(None);
        fs::write(
            &dir.join("user-1.json"),
            json!({"id": "user-1"}).to_string(),
        )
        .unwrap();
        table.refresh();
        assert!(!calls.lock().unwrap().is_empty());
        let last = calls.lock().unwrap().last().cloned().unwrap();
        for key in ["added", "modified", "deleted"] {
            assert!(last.get(key).is_some());
        }
    }

    #[test]
    fn accessors_return_association_metadata() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        table.add_gsi("by_status", "status", None);
        table.add_has_many("posts", "posts", "by_status");
        table.directory = Some(PathBuf::from("/tmp"));
        assert!(table.associations().contains(&"posts".to_string()));
        assert!(table.association_defs().contains_key("posts"));
        assert!(table.directory().is_some());
    }

    #[test]
    fn helper_methods_handle_missing_directory() {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        assert!(table.iter_json_files().is_empty());
        assert_eq!(table.dir_mtime(), None);
        table.maybe_refresh_before_query();
    }
}
