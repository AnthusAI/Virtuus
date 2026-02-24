//! Minimal database container for tables.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use serde_yaml::Value as YamlValue;

use crate::sort::SortCondition;
use crate::table::{Association, Table};

/// Collection of tables with cache helpers.
#[derive(Debug, Default)]
pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    /// Create an empty database.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a table by name.
    pub fn add_table(&mut self, name: &str, table: Table) {
        self.tables.insert(name.to_string(), table);
    }

    /// Proactively refresh all tables.
    pub fn warm(&mut self) {
        for table in self.tables.values_mut() {
            table.warm();
        }
    }

    /// Run dry-run checks on all tables.
    pub fn check(&self) -> HashMap<String, Value> {
        self.tables
            .iter()
            .map(|(name, table)| {
                let summary = table.check();
                (
                    name.clone(),
                    serde_json::json!({
                        "added": summary.added,
                        "modified": summary.modified,
                        "deleted": summary.deleted,
                        "reread": summary.reread
                    }),
                )
            })
            .collect()
    }

    /// Describe all tables in the database.
    pub fn describe(&mut self) -> HashMap<String, Value> {
        self.tables
            .iter_mut()
            .map(|(name, table)| {
                let mut desc_map = table.describe().as_object().cloned().unwrap_or_default();
                desc_map.insert("stale".to_string(), Value::Bool(table.is_stale(false)));
                (name.clone(), Value::Object(desc_map))
            })
            .collect()
    }

    /// Validate referential integrity for belongs_to associations.
    pub fn validate(&mut self) -> Vec<Value> {
        let mut violations = Vec::new();
        let table_names: Vec<String> = self.tables.keys().cloned().collect();
        for table_name in table_names {
            let (association_defs, key_field, records) = {
                let table = self.tables.get_mut(&table_name).expect("table missing");
                (
                    table.association_defs().clone(),
                    table.key_field().unwrap_or("id").to_string(),
                    table.scan(),
                )
            };
            for (assoc_name, def) in association_defs {
                if let Association::BelongsTo {
                    target_table,
                    foreign_key,
                } = def
                {
                    for record in &records {
                        let Some(fk_value) = record.get(&foreign_key) else {
                            continue;
                        };
                        let fk_owned = fk_value.to_string();
                        let fk_str = fk_value.as_str().unwrap_or(&fk_owned);
                        let target_missing = self
                            .tables
                            .get(&target_table)
                            .and_then(|t| t.get(fk_str, None))
                            .is_none();
                        if target_missing {
                            violations.push(json!({
                                "table": table_name,
                                "record_pk": record.get(&key_field).cloned().unwrap_or(Value::Null),
                                "association": assoc_name,
                                "foreign_key": foreign_key,
                                "missing_target": fk_value.clone(),
                            }));
                        }
                    }
                }
            }
        }
        violations
    }

    /// Load a database from a YAML schema file.
    pub fn from_schema(path: &Path, data_root: Option<&Path>) -> Self {
        let schema_text = fs::read_to_string(path).expect("failed to read schema");
        let yaml: YamlValue = serde_yaml::from_str(&schema_text).expect("invalid yaml");
        let tables = yaml
            .get("tables")
            .and_then(|t| t.as_mapping())
            .cloned()
            .unwrap_or_default();
        let mut db = Database::new();
        for (name_value, conf_value) in tables {
            let name = name_value.as_str().expect("table name must be string");
            let conf = conf_value.as_mapping().expect("table conf must be mapping");
            let primary_key = conf
                .get(YamlValue::from("primary_key"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let partition_key = conf
                .get(YamlValue::from("partition_key"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let sort_key = conf
                .get(YamlValue::from("sort_key"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let directory = conf
                .get(YamlValue::from("directory"))
                .and_then(|v| v.as_str())
                .map(|d| {
                    let base = data_root
                        .map(PathBuf::from)
                        .or_else(|| path.parent().map(|p| p.to_path_buf()))
                        .unwrap_or_default();
                    base.join(d).to_string_lossy().to_string()
                });
            let mut table = Table::new(
                name,
                primary_key.as_deref(),
                partition_key.as_deref(),
                sort_key.as_deref(),
                directory.clone().map(PathBuf::from),
                crate::table::ValidationMode::Warn,
            );
            if let Some(storage) = conf
                .get(YamlValue::from("storage"))
                .and_then(|v| v.as_str())
            {
                match storage {
                    "memory" => table.set_storage_mode(crate::table::StorageMode::Memory),
                    "index_only" => table.set_storage_mode(crate::table::StorageMode::IndexOnly),
                    _ => {}
                }
            }
            if let Some(search_conf) = conf
                .get(YamlValue::from("search"))
                .and_then(|v| v.as_mapping())
            {
                if let Some(fields_value) = search_conf
                    .get(YamlValue::from("fields"))
                    .and_then(|v| v.as_sequence())
                {
                    let fields: Vec<String> = fields_value
                        .iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.to_string())
                        .collect();
                    if !fields.is_empty() {
                        table.set_search_fields(fields);
                    }
                }
            }
            if let Some(gsis) = conf
                .get(YamlValue::from("gsis"))
                .and_then(|v| v.as_mapping())
            {
                for (gsi_name_value, gsi_conf_value) in gsis {
                    let gsi_name = gsi_name_value.as_str().expect("gsi name");
                    let gsi_conf = gsi_conf_value.as_mapping().expect("gsi conf mapping");
                    let partition = gsi_conf
                        .get(YamlValue::from("partition_key"))
                        .and_then(|v| v.as_str())
                        .expect("gsi partition key");
                    let sort = gsi_conf
                        .get(YamlValue::from("sort_key"))
                        .and_then(|v| v.as_str());
                    table.add_gsi(gsi_name, partition, sort);
                }
            }
            if let Some(assocs) = conf
                .get(YamlValue::from("associations"))
                .and_then(|v| v.as_mapping())
            {
                for (assoc_name_value, assoc_conf_value) in assocs {
                    let assoc_name = assoc_name_value.as_str().expect("assoc name");
                    let assoc_conf = assoc_conf_value.as_mapping().expect("assoc conf mapping");
                    let kind = assoc_conf
                        .get(YamlValue::from("type"))
                        .and_then(|v| v.as_str())
                        .expect("association type");
                    if kind == "belongs_to" {
                        let target = assoc_conf
                            .get(YamlValue::from("table"))
                            .and_then(|v| v.as_str())
                            .expect("target table");
                        let fk = assoc_conf
                            .get(YamlValue::from("foreign_key"))
                            .and_then(|v| v.as_str())
                            .expect("foreign_key");
                        table.add_belongs_to(assoc_name, target, fk);
                    } else if kind == "has_many" {
                        let target = assoc_conf
                            .get(YamlValue::from("table"))
                            .and_then(|v| v.as_str())
                            .expect("target table");
                        let index = assoc_conf
                            .get(YamlValue::from("index"))
                            .and_then(|v| v.as_str())
                            .expect("index");
                        table.add_has_many(assoc_name, target, index);
                    } else if kind == "has_many_through" {
                        let through = assoc_conf
                            .get(YamlValue::from("through"))
                            .and_then(|v| v.as_str())
                            .expect("through");
                        let index = assoc_conf
                            .get(YamlValue::from("index"))
                            .and_then(|v| v.as_str())
                            .expect("index");
                        let target = assoc_conf
                            .get(YamlValue::from("table"))
                            .and_then(|v| v.as_str())
                            .expect("target");
                        let fk = assoc_conf
                            .get(YamlValue::from("foreign_key"))
                            .and_then(|v| v.as_str())
                            .expect("foreign_key");
                        table.add_has_many_through(assoc_name, through, index, target, fk);
                    }
                }
            }
            db.add_table(name, table);
        }
        for table in db.tables.values_mut() {
            if table.directory().is_some() {
                table.load_from_dir(None);
            }
        }
        db
    }

    /// Execute a query dictionary against the database.
    pub fn execute(&mut self, query: &Value) -> Value {
        let map = query.as_object().expect("query must be object");
        if map.len() != 1 {
            panic!("query must target exactly one table");
        }
        let (table_name, directive) = map.iter().next().unwrap();
        let directive = directive.as_object().cloned().unwrap_or_default();
        let table = self
            .tables
            .get_mut(table_name.as_str())
            .unwrap_or_else(|| panic!("table \"{}\" does not exist", table_name));

        if let Some(pk_value) = directive.get("pk") {
            let pk_str = match pk_value.as_str() {
                Some(s) => s.to_string(),
                None => pk_value.to_string(),
            };
            let sort = directive.get("sort").and_then(|v| v.as_str());
            let mut record = table.get(&pk_str, sort).unwrap_or(Value::Null);
            if let Some(fields) = directive.get("fields").and_then(|v| v.as_array()) {
                record = project(&record, fields);
            }
            let include_map = directive.get("include").and_then(|v| v.as_object());
            return self.apply_includes(table_name, record, include_map);
        }

        if let Some(search_value) = directive.get("search") {
            let query_text = search_value.as_str().unwrap_or("");
            let where_map = directive
                .get("where")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let mut records: Vec<Value> = table
                .search(query_text)
                .into_iter()
                .filter(|record| record_matches(record, &where_map))
                .collect();
            if let Some(fields) = directive.get("fields").and_then(|v| v.as_array()) {
                records = records.into_iter().map(|r| project(&r, fields)).collect();
            }
            let start: usize = directive
                .get("next_token")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let mut result = json!({ "items": records });
            if let Some(limit) = directive.get("limit").and_then(|v| v.as_u64()) {
                let end = start + limit as usize;
                let items = result["items"].as_array().cloned().unwrap_or_default();
                let page: Vec<Value> = items.into_iter().skip(start).take(limit as usize).collect();
                result["items"] = Value::Array(page.clone());
                if end < records.len() {
                    result["next_token"] = Value::String(end.to_string());
                }
            }
            if let Some(includes) = directive.get("include").and_then(|v| v.as_object()) {
                let items = result["items"].as_array().cloned().unwrap_or_default();
                let mut enriched = Vec::new();
                for item in items {
                    enriched.push(self.apply_includes(table_name, item, Some(includes)));
                }
                result["items"] = Value::Array(enriched);
            }
            return result;
        }

        let mut records: Vec<Value> =
            if let Some(index_name) = directive.get("index").and_then(|v| v.as_str()) {
                let where_map = directive
                    .get("where")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                let gsi = table
                    .gsis()
                    .get(index_name)
                    .unwrap_or_else(|| panic!("GSI \"{}\" does not exist", index_name));
                let partition_field = gsi.partition_key();
                let partition_value = where_map
                    .get(partition_field)
                    .unwrap_or_else(|| panic!("missing partition key in where"));
                let sort_condition = directive.get("sort").and_then(build_sort_condition);
                let descending = directive
                    .get("sort_direction")
                    .and_then(|v| v.as_str())
                    .map(|s| s == "desc")
                    .unwrap_or(false);
                table.query_gsi(
                    index_name,
                    partition_value,
                    sort_condition.as_ref(),
                    descending,
                )
            } else {
                let where_map = directive
                    .get("where")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                table
                    .scan()
                    .into_iter()
                    .filter(|record| record_matches(record, &where_map))
                    .collect()
            };

        if let Some(fields) = directive.get("fields").and_then(|v| v.as_array()) {
            records = records.into_iter().map(|r| project(&r, fields)).collect();
        }

        let start: usize = directive
            .get("next_token")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let mut result = json!({"items": records});
        if let Some(limit) = directive.get("limit").and_then(|v| v.as_u64()) {
            let end = start + limit as usize;
            let items = result["items"].as_array().cloned().unwrap_or_default();
            let page: Vec<Value> = items.into_iter().skip(start).take(limit as usize).collect();
            result["items"] = Value::Array(page.clone());
            if end < records.len() {
                result["next_token"] = Value::String(end.to_string());
            }
        }
        if let Some(includes) = directive.get("include").and_then(|v| v.as_object()) {
            let items = result["items"].as_array().cloned().unwrap_or_default();
            let mut enriched = Vec::new();
            for item in items {
                enriched.push(self.apply_includes(table_name, item, Some(includes)));
            }
            result["items"] = Value::Array(enriched);
        }
        result
    }

    /// Access a table mutably.
    pub fn table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    /// Access all tables.
    pub fn tables(&self) -> &HashMap<String, Table> {
        &self.tables
    }

    /// Resolve an association for a record within the database.
    pub fn resolve_association(&mut self, table: &str, association: &str, pk: &str) -> Value {
        let assoc = match self
            .tables
            .get(table)
            .and_then(|t| t.association(association))
            .cloned()
        {
            Some(a) => a,
            None => return Value::Null,
        };
        let record = match self.tables.get(table).and_then(|t| t.get(pk, None)) {
            Some(record) => record,
            None => return Value::Null,
        };
        match assoc {
            Association::BelongsTo {
                target_table,
                foreign_key,
            } => {
                let fk_value = match record.get(&foreign_key) {
                    Some(value) => value,
                    None => return Value::Null,
                };
                let fk_str = match fk_value.as_str() {
                    Some(s) => s.to_string(),
                    None => fk_value.to_string(),
                };
                let target = self
                    .tables
                    .get_mut(&target_table)
                    .expect("target table not found");
                target.get(&fk_str, None).unwrap_or(Value::Null)
            }
            Association::HasMany {
                target_table,
                index,
            } => {
                let table_ref = self.tables.get(table).unwrap();
                let field = table_ref
                    .key_field()
                    .expect("key field missing")
                    .to_string();
                let key_value = record
                    .get(&field)
                    .cloned()
                    .expect("record missing key field");
                let target = self
                    .tables
                    .get_mut(&target_table)
                    .expect("target table not found");
                Value::Array(target.query_gsi(&index, &key_value, None, false))
            }
            Association::HasManyThrough {
                through_table,
                through_index,
                target_table,
                target_foreign_key,
            } => {
                let table_ref = self.tables.get(table).unwrap();
                let field = table_ref
                    .key_field()
                    .expect("key field missing")
                    .to_string();
                let key_value = record
                    .get(&field)
                    .cloned()
                    .expect("record missing key field");
                let assignments = {
                    let through = self
                        .tables
                        .get_mut(&through_table)
                        .expect("through table not found");
                    through.query_gsi(&through_index, &key_value, None, false)
                };
                let mut related = Vec::new();
                for assignment in assignments {
                    let fk_value = match assignment.get(&target_foreign_key) {
                        Some(value) => value,
                        None => continue,
                    };
                    let fk_str = match fk_value.as_str() {
                        Some(s) => s,
                        None => continue,
                    };
                    if let Some(record) = self
                        .tables
                        .get(&target_table)
                        .and_then(|t| t.get(fk_str, None))
                    {
                        related.push(record);
                    }
                }
                Value::Array(related)
            }
        }
    }

    fn apply_includes(
        &mut self,
        table_name: &str,
        record: Value,
        includes: Option<&serde_json::Map<String, Value>>,
    ) -> Value {
        let Some(include_map) = includes else {
            return record;
        };
        if record.is_null() {
            return record;
        }
        let mut enriched = record;
        let (association_defs, key_field) = {
            let table = self.tables.get(table_name).unwrap();
            (
                table.association_defs().clone(),
                table.key_field().unwrap_or("id").to_string(),
            )
        };
        let pk = enriched
            .get(&key_field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        for (assoc_name, assoc_directive) in include_map {
            let related = self.resolve_association(table_name, assoc_name, &pk);
            let target_table = association_defs
                .get(assoc_name)
                .map(|d| match d {
                    Association::BelongsTo { target_table, .. } => target_table.clone(),
                    Association::HasMany { target_table, .. } => target_table.clone(),
                    Association::HasManyThrough { target_table, .. } => target_table.clone(),
                })
                .unwrap_or_else(|| table_name.to_string());
            if related.is_null() {
                enriched[assoc_name] = Value::Null;
                continue;
            }
            if let Some(array) = related.as_array() {
                let mut items = Vec::new();
                for mut item in array.clone() {
                    if let Some(fields) = assoc_directive.get("fields").and_then(|v| v.as_array()) {
                        item = project(&item, fields);
                    }
                    if assoc_directive.get("include").is_some() {
                        let nested = assoc_directive
                            .get("include")
                            .and_then(|v| v.as_object())
                            .cloned();
                        item = self.apply_includes(&target_table, item, nested.as_ref());
                    }
                    items.push(item);
                }
                enriched[assoc_name] = Value::Array(items);
            } else {
                let mut item = related;
                if let Some(fields) = assoc_directive.get("fields").and_then(|v| v.as_array()) {
                    item = project(&item, fields);
                }
                if assoc_directive.get("include").is_some() {
                    let nested = assoc_directive
                        .get("include")
                        .and_then(|v| v.as_object())
                        .cloned();
                    item = self.apply_includes(&target_table, item, nested.as_ref());
                }
                enriched[assoc_name] = item;
            }
        }
        enriched
    }
}

fn project(record: &Value, fields: &[Value]) -> Value {
    if !record.is_object() {
        return record.clone();
    }
    if fields.is_empty() {
        return record.clone();
    }
    let mut obj = serde_json::Map::new();
    for field in fields {
        if let Some(name) = field.as_str() {
            if let Some(value) = record.get(name) {
                obj.insert(name.to_string(), value.clone());
            }
        }
    }
    Value::Object(obj)
}

fn record_matches(record: &Value, where_map: &serde_json::Map<String, Value>) -> bool {
    for (key, expected) in where_map {
        if record.get(key) != Some(expected) {
            return false;
        }
    }
    true
}

fn build_sort_condition(value: &Value) -> Option<SortCondition> {
    let map = value.as_object()?;
    let (op, operand) = map.iter().next()?;
    match op.as_str() {
        "eq" => Some(SortCondition::Eq(operand.clone())),
        "ne" => Some(SortCondition::Ne(operand.clone())),
        "lt" => Some(SortCondition::Lt(operand.clone())),
        "lte" => Some(SortCondition::Lte(operand.clone())),
        "gt" => Some(SortCondition::Gt(operand.clone())),
        "gte" => Some(SortCondition::Gte(operand.clone())),
        "between" => operand.as_array().and_then(|a| {
            if a.len() == 2 {
                Some(SortCondition::Between(a[0].clone(), a[1].clone()))
            } else {
                None
            }
        }),
        "begins_with" => operand
            .as_str()
            .map(|s| SortCondition::BeginsWith(s.to_string())),
        "contains" => operand
            .as_str()
            .map(|s| SortCondition::Contains(s.to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::table::{StorageMode, ValidationMode};
    use serde_json::json;

    fn table_with_pk(name: &str) -> Table {
        Table::new(name, Some("id"), None, None, None, ValidationMode::Silent)
    }

    #[test]
    fn execute_pk_returns_record() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        users.put(json!({"id":"user-1","name":"Alice"}));
        db.add_table("users", users);
        let result = db.execute(&json!({"users": {"pk": "user-1"}}));
        assert_eq!(result.get("id"), Some(&json!("user-1")));
    }

    #[test]
    fn execute_pk_uses_sort_key() {
        let mut db = Database::new();
        let mut scores = Table::new(
            "scores",
            None,
            Some("user_id"),
            Some("id"),
            None,
            ValidationMode::Silent,
        );
        scores.put(json!({"user_id":"u1","id":"a","value":1}));
        scores.put(json!({"user_id":"u1","id":"b","value":2}));
        db.add_table("scores", scores);
        let result = db.execute(&json!({"scores": {"pk": "u1", "sort": "b"}}));
        assert_eq!(result.get("value"), Some(&json!(2)));
    }

    #[test]
    fn execute_index_query_filters() {
        let mut db = Database::new();
        let mut posts = table_with_pk("posts");
        posts.add_gsi("by_user", "user_id", None);
        posts.put(json!({"id":"p1","user_id":"u1"}));
        posts.put(json!({"id":"p2","user_id":"u1"}));
        posts.put(json!({"id":"p3","user_id":"u2"}));
        db.add_table("posts", posts);
        let result = db.execute(
            &json!({"posts": {"index": "by_user", "where": {"user_id": "u1"}, "limit": 1}}),
        );
        let items = result.get("items").and_then(|v| v.as_array()).unwrap();
        assert_eq!(items.len(), 1);
        assert!(result.get("next_token").is_some());
    }

    #[test]
    #[should_panic]
    fn execute_requires_existing_gsi() {
        let mut db = Database::new();
        let posts = table_with_pk("posts");
        db.add_table("posts", posts);
        db.execute(&json!({"posts": {"index": "by_user", "where": {"user_id": "u1"}}}));
    }

    #[test]
    #[should_panic]
    fn execute_requires_partition_in_where() {
        let mut db = Database::new();
        let mut posts = table_with_pk("posts");
        posts.add_gsi("by_user", "user_id", None);
        db.add_table("posts", posts);
        db.execute(&json!({"posts": {"index": "by_user", "where": {}}}));
    }

    #[test]
    fn validate_reports_missing_target() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        users.put(json!({"id":"u1"}));
        let mut posts = table_with_pk("posts");
        posts.add_belongs_to("author", "users", "user_id");
        posts.put(json!({"id":"p1","user_id":"u-missing"}));
        posts.put(json!({"id":"p2"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let violations = db.validate();
        assert_eq!(violations.len(), 1);
        assert_eq!(
            violations[0].get("missing_target"),
            Some(&json!("u-missing"))
        );
    }

    #[test]
    fn apply_includes_embeds_related() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        users.put(json!({"id":"u1","name":"Alice"}));
        let mut posts = table_with_pk("posts");
        posts.add_gsi("by_user", "user_id", None);
        posts.add_belongs_to("author", "users", "user_id");
        posts.put(json!({"id":"p1","user_id":"u1"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let result = db.execute(&json!({"posts": {"index": "by_user", "where": {"user_id": "u1"}, "include": {"author": {}}}}));
        let items = result.get("items").and_then(|v| v.as_array()).unwrap();
        assert_eq!(
            items[0].get("author").and_then(|a| a.get("name")),
            Some(&json!("Alice"))
        );
    }

    #[test]
    fn check_and_describe_work_without_directory() {
        let mut db = Database::new();
        db.add_table("users", table_with_pk("users"));
        let summary = db.check();
        assert_eq!(summary["users"]["added"], 0);
        let desc = db.describe();
        assert_eq!(desc["users"].get("stale"), Some(&json!(false)));
    }

    #[test]
    fn from_schema_loads_tables_and_data() {
        let tmp = std::env::temp_dir().join("virtuus_db_schema");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(tmp.join("data")).unwrap();
        fs::write(
            tmp.join("data").join("u1.json"),
            r#"{"id":"u1","name":"Alice"}"#,
        )
        .unwrap();
        let schema = r#"
tables:
  users:
    primary_key: id
    directory: data
"#;
        let schema_path = tmp.join("schema.yml");
        fs::write(&schema_path, schema).unwrap();
        let mut db = Database::from_schema(schema_path.as_path(), Some(tmp.as_path()));
        assert!(db.table_mut("users").unwrap().get("u1", None).is_some());
    }

    #[test]
    fn from_schema_builds_gsis_and_associations() {
        let tmp = std::env::temp_dir().join("virtuus_db_schema_full");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        let schema = r#"
tables:
  users:
    primary_key: id
    gsis:
      by_email:
        partition_key: email
    associations:
      posts:
        type: has_many
        table: posts
        index: by_user
      assignments:
        type: has_many_through
        through: job_assignments
        index: by_user
        table: jobs
        foreign_key: job_id
  posts:
    partition_key: user_id
    sort_key: id
    associations:
      author:
        type: belongs_to
        table: users
        foreign_key: user_id
    gsis:
      by_user:
        partition_key: user_id
        sort_key: created_at
  job_assignments:
    partition_key: user_id
    sort_key: job_id
    gsis:
      by_user:
        partition_key: user_id
    associations:
      worker:
        type: belongs_to
        table: users
        foreign_key: user_id
"#;
        let schema_path = tmp.join("schema_full.yml");
        fs::write(&schema_path, schema).unwrap();
        let mut db = Database::from_schema(schema_path.as_path(), Some(tmp.as_path()));
        let users = db.table_mut("users").unwrap();
        assert!(users.gsis().contains_key("by_email"));
        assert!(users.associations().contains(&"posts".to_string()));
        let posts = db.table_mut("posts").unwrap();
        assert!(posts.gsis().contains_key("by_user"));
        assert!(posts.associations().contains(&"author".to_string()));
        let assignments = db.table_mut("job_assignments").unwrap();
        assert!(assignments.gsis().contains_key("by_user"));
    }

    #[test]
    fn from_schema_without_data_root_uses_relative_directory() {
        let tmp = std::env::temp_dir().join("virtuus_db_schema_relative");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(tmp.join("data")).unwrap();
        fs::write(tmp.join("data").join("u1.json"), r#"{"id":"u1"}"#).unwrap();
        let schema = r#"
tables:
  users:
    primary_key: id
    directory: data
"#;
        let schema_path = tmp.join("schema.yml");
        fs::write(&schema_path, schema).unwrap();
        let mut db = Database::from_schema(schema_path.as_path(), None);
        assert!(db.table_mut("users").unwrap().get("u1", None).is_some());
    }

    #[test]
    fn from_schema_respects_storage_and_search_fields() {
        let tmp = std::env::temp_dir().join("virtuus_db_schema_storage");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(tmp.join("data")).unwrap();
        fs::write(
            tmp.join("data").join("u1.json"),
            r#"{"id":"u1","title":"Alpha"}"#,
        )
        .unwrap();
        let schema = r#"
tables:
  memory_table:
    primary_key: id
    directory: data
    storage: memory
    search:
      fields: [title]
  index_table:
    primary_key: id
    directory: data
    storage: index_only
  invalid_table:
    primary_key: id
    directory: data
    storage: nope
"#;
        let schema_path = tmp.join("schema.yml");
        fs::write(&schema_path, schema).unwrap();
        let mut db = Database::from_schema(schema_path.as_path(), Some(tmp.as_path()));
        let memory = db.table_mut("memory_table").unwrap();
        assert_eq!(memory.storage_mode(), StorageMode::Memory);
        assert_eq!(memory.search_fields(), &vec!["title".to_string()]);
        let index = db.table_mut("index_table").unwrap();
        assert_eq!(index.storage_mode(), StorageMode::IndexOnly);
        let invalid = db.table_mut("invalid_table").unwrap();
        assert_eq!(invalid.storage_mode(), StorageMode::IndexOnly);
    }

    #[test]
    fn execute_search_filters_paginates_and_includes() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        users.put(json!({"id":"u1","name":"Alice"}));
        db.add_table("users", users);

        let mut posts = table_with_pk("posts");
        posts.set_search_fields(vec!["title".to_string()]);
        posts.add_belongs_to("author", "users", "user_id");
        posts.put(json!({"id":"p1","user_id":"u1","title":"Alpha Beta","status":"active"}));
        posts.put(json!({"id":"p2","user_id":"u1","title":"Alpha Beta Two","status":"active"}));
        posts.put(json!({"id":"p3","user_id":"u1","title":"Alpha Beta","status":"inactive"}));
        db.add_table("posts", posts);

        let result = db.execute(&json!({"posts": {
            "search": "alpha beta",
            "where": {"status": "active"},
            "fields": ["id", "title", "user_id"],
            "include": {"author": {}},
            "limit": 1,
            "next_token": "0"
        }}));
        let items = result.get("items").and_then(|v| v.as_array()).unwrap();
        assert_eq!(items.len(), 1);
        assert!(result.get("next_token").is_some());
        assert_eq!(items[0]["author"]["name"].as_str(), Some("Alice"));
        assert!(items[0].get("status").is_none());
    }

    #[test]
    fn warm_refreshes_tables() {
        let dir = std::env::temp_dir().join("virtuus_db_warm");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("item.json"), r#"{"id":"1","name":"One"}"#).unwrap();
        let table = Table::new(
            "items",
            Some("id"),
            None,
            None,
            Some(dir.clone()),
            ValidationMode::Silent,
        );
        let mut db = Database::new();
        db.add_table("items", table);
        db.warm();
        assert_eq!(
            db.table_mut("items").unwrap().get("1", None),
            Some(json!({"id":"1","name":"One"}))
        );
    }

    #[test]
    fn execute_pk_accepts_non_string_pk_and_projects_fields() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        users.put(json!({"id":1,"name":"Bob","role":"admin"}));
        db.add_table("users", users);
        let result = db.execute(&json!({"users": {"pk": 1, "fields": ["name"]}}));
        assert_eq!(result, json!({"name":"Bob"}));
    }

    #[test]
    #[should_panic]
    fn execute_requires_single_table() {
        let mut db = Database::new();
        db.add_table("users", table_with_pk("users"));
        db.execute(&json!({"users": {}, "posts": {}}));
    }

    #[test]
    fn execute_scan_supports_pagination_and_filters() {
        let mut db = Database::new();
        let mut items = table_with_pk("items");
        items.put(json!({"id":"a","kind":"keep"}));
        items.put(json!({"id":"b","kind":"keep"}));
        items.put(json!({"id":"c","kind":"drop"}));
        db.add_table("items", items);
        let first_page = db.execute(&json!({"items": {"where": {"kind":"keep"}, "limit": 1}}));
        assert_eq!(
            first_page
                .get("items")
                .and_then(|v| v.as_array())
                .unwrap()
                .len(),
            1
        );
        let token = first_page
            .get("next_token")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let second_page = db.execute(
            &json!({"items": {"where": {"kind":"keep"}, "limit": 1, "next_token": token}}),
        );
        assert_eq!(
            second_page
                .get("items")
                .and_then(|v| v.as_array())
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn execute_index_honors_sort_direction_and_projection() {
        let mut db = Database::new();
        let mut posts = table_with_pk("posts");
        posts.add_gsi("by_user", "user_id", Some("created_at"));
        posts.put(json!({"id":"p1","user_id":"u1","created_at":1,"title":"old"}));
        posts.put(json!({"id":"p2","user_id":"u1","created_at":2,"title":"new"}));
        db.add_table("posts", posts);
        let result = db.execute(&json!({"posts": {"index": "by_user", "where": {"user_id": "u1"}, "sort_direction": "desc", "fields": ["title"]}}));
        let items = result.get("items").and_then(|v| v.as_array()).unwrap();
        assert_eq!(items[0], json!({"title":"new"}));
    }

    #[test]
    fn execute_include_handles_arrays_and_nested_fields() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        posts.add_gsi("by_user", "user_id", None);
        posts.add_belongs_to("author", "users", "user_id");
        users.add_has_many("posts", "posts", "by_user");
        users.put(json!({"id":"u1","name":"Alice"}));
        posts.put(json!({"id":"p1","user_id":"u1","title":"Hello","body":"body"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let result = db.execute(&json!({"users": {"include": {"posts": {"fields": ["title"]}}}}));
        let items = result.get("items").and_then(|v| v.as_array()).unwrap();
        let user = &items[0];
        assert_eq!(
            user.get("posts").and_then(|p| p.as_array()).unwrap()[0],
            json!({"title":"Hello"})
        );
    }

    #[test]
    fn tables_accessor_exposes_map() {
        let mut db = Database::new();
        db.add_table("users", table_with_pk("users"));
        assert!(db.tables().contains_key("users"));
    }

    #[test]
    fn resolve_association_handles_all_kinds() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        let mut assignments = table_with_pk("assignments");
        posts.add_gsi("by_user", "user_id", None);
        assignments.add_gsi("by_job", "job_id", None);
        posts.add_belongs_to("author", "users", "user_id");
        users.add_has_many("posts", "posts", "by_user");
        users.add_has_many_through("jobs", "assignments", "by_job", "jobs", "job_id");
        assignments.add_belongs_to("job", "jobs", "job_id");
        let mut jobs = table_with_pk("jobs");
        jobs.put(json!({"id":"j1","name":"Job"}));
        users.put(json!({"id":"u1","name":"Alice"}));
        posts.put(json!({"id":"p1","user_id":"u1","title":"Hello"}));
        assignments.put(json!({"id":"a1","job_id":"u1","worker_id":"u1"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        db.add_table("assignments", assignments);
        db.add_table("jobs", jobs);

        assert_eq!(
            db.resolve_association("posts", "author", "p1").get("id"),
            Some(&json!("u1"))
        );
        assert!(
            db.resolve_association("users", "posts", "u1")
                .as_array()
                .unwrap()
                .len()
                >= 1
        );
        assert!(db
            .resolve_association("users", "jobs", "u1")
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn resolve_association_handles_missing_values() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        posts.add_belongs_to("author", "users", "user_id");
        users.put(json!({"id":"u1"}));
        posts.put(json!({"id":"p1"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        assert_eq!(
            db.resolve_association("posts", "author", "missing"),
            Value::Null
        );
        assert_eq!(db.resolve_association("posts", "author", "p1"), Value::Null);
    }

    #[test]
    fn resolve_association_belongs_to_allows_numeric_keys() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        posts.add_belongs_to("author", "users", "user_id");
        users.put(json!({"id":1,"name":"Alice"}));
        posts.put(json!({"id":"p1","user_id":1}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let author = db.resolve_association("posts", "author", "p1");
        assert_eq!(author.get("name"), Some(&json!("Alice")));
    }

    #[test]
    fn resolve_association_returns_related_records() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        posts.add_gsi("by_user", "user_id", None);
        users.add_has_many("posts", "posts", "by_user");
        users.put(json!({"id":1,"name":"Alice"}));
        posts.put(json!({"id":"p1","user_id":1}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let related = db.resolve_association("users", "posts", "1");
        assert_eq!(related.as_array().unwrap().len(), 1);

        let mut jobs = table_with_pk("jobs");
        let mut assignments = table_with_pk("assignments");
        let mut workers = table_with_pk("workers");
        assignments.add_gsi("by_job", "job_id", None);
        jobs.add_has_many_through("workers", "assignments", "by_job", "workers", "worker_id");
        jobs.put(json!({"id":"j1"}));
        assignments.put(json!({"id":"a1","job_id":"j1","worker_id":"w1"}));
        assignments.put(json!({"id":"a2","job_id":"j1"}));
        assignments.put(json!({"id":"a3","job_id":"j1","worker_id":1}));
        workers.put(json!({"id":"w1"}));
        db.add_table("jobs", jobs);
        db.add_table("assignments", assignments);
        db.add_table("workers", workers);
        let through = db.resolve_association("jobs", "workers", "j1");
        assert_eq!(through.as_array().unwrap().len(), 1);
    }

    #[test]
    fn apply_includes_respects_field_selection_for_single_objects() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        posts.add_belongs_to("author", "users", "user_id");
        users.add_has_many("posts", "posts", "by_user");
        posts.add_gsi("by_user", "user_id", None);
        users.put(json!({"id":"u1","name":"Alice","role":"admin"}));
        posts.put(json!({"id":"p1","user_id":"u1","title":"Hello"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let result = db
            .execute(&json!({"posts": {"pk": "p1", "include": {"author": {"fields": ["name"]}}}}));
        assert_eq!(
            result.get("author").and_then(|a| a.get("name")),
            Some(&json!("Alice"))
        );
        assert!(result.get("author").and_then(|a| a.get("role")).is_none());
    }

    #[test]
    fn apply_includes_supports_nested_for_objects() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        posts.add_belongs_to("author", "users", "user_id");
        posts.add_gsi("by_user", "user_id", None);
        users.add_has_many("posts", "posts", "by_user");
        users.put(json!({"id":"u1","name":"Alice"}));
        posts.put(json!({"id":"p1","user_id":"u1","title":"Hello"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let result = db.execute(
            &json!({"posts": {"pk": "p1", "include": {"author": {"include": {"posts": {}}}}}}),
        );
        let author = result.get("author").unwrap();
        assert!(author.get("posts").is_some());
    }

    #[test]
    fn apply_includes_handles_null_records_and_nested_arrays() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        let mut posts = table_with_pk("posts");
        posts.add_gsi("by_user", "user_id", None);
        users.add_has_many("posts", "posts", "by_user");
        users.put(json!({"id":"u1","name":"Alice"}));
        posts.put(json!({"id":"p1","user_id":"u1","title":"Hello","body":"b"}));
        db.add_table("users", users);
        db.add_table("posts", posts);
        let missing = db.execute(&json!({"posts": {"pk": "missing", "include": {"author": {}}}}));
        assert!(missing.is_null());
        let nested = db.execute(&json!({"users": {"include": {"posts": {"include": {}}}}}));
        let items = nested.get("items").and_then(|v| v.as_array()).unwrap();
        assert_eq!(
            items[0]
                .get("posts")
                .and_then(|p| p.as_array())
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn apply_includes_handles_unknown_association() {
        let mut db = Database::new();
        let mut users = table_with_pk("users");
        users.put(json!({"id":"u1","name":"Alice"}));
        db.add_table("users", users);
        let result = db.execute(&json!({"users": {"include": {"unknown": {}}}}));
        let items = result.get("items").and_then(|v| v.as_array()).unwrap();
        assert!(items[0].get("unknown").is_some());
    }

    #[test]
    fn apply_includes_supports_has_many_through() {
        let mut db = Database::new();
        let mut jobs = table_with_pk("jobs");
        let mut assignments = table_with_pk("assignments");
        let mut workers = table_with_pk("workers");
        assignments.add_gsi("by_job", "job_id", None);
        jobs.add_has_many_through("workers", "assignments", "by_job", "workers", "worker_id");
        jobs.put(json!({"id":"j1"}));
        assignments.put(json!({"id":"a1","job_id":"j1","worker_id":"w1"}));
        workers.put(json!({"id":"w1","name":"Bob"}));
        db.add_table("jobs", jobs);
        db.add_table("assignments", assignments);
        db.add_table("workers", workers);
        let result = db.execute(&json!({"jobs": {"pk": "j1", "include": {"workers": {}}}}));
        assert_eq!(
            result
                .get("workers")
                .and_then(|w| w.as_array())
                .map(|a| a.len()),
            Some(1)
        );
    }

    #[test]
    fn project_and_record_matches_helpers_work() {
        let value = json!({"a":1,"b":2});
        assert_eq!(project(&value, &vec![json!("a")]), json!({"a":1}));
        assert_eq!(
            project(&Value::String("x".into()), &vec![json!("a")]),
            Value::String("x".into())
        );
        assert_eq!(project(&value, &Vec::new()), value);
        let mut where_map = serde_json::Map::new();
        where_map.insert("a".into(), json!(1));
        assert!(record_matches(&value, &where_map));
        where_map.insert("a".into(), json!(2));
        assert!(!record_matches(&value, &where_map));
    }

    #[test]
    fn build_sort_condition_parses_all_ops() {
        assert!(matches!(
            build_sort_condition(&json!({"eq": 1})),
            Some(SortCondition::Eq(_))
        ));
        assert!(matches!(
            build_sort_condition(&json!({"ne": 1})),
            Some(SortCondition::Ne(_))
        ));
        assert!(matches!(
            build_sort_condition(&json!({"lt": 1})),
            Some(SortCondition::Lt(_))
        ));
        assert!(matches!(
            build_sort_condition(&json!({"lte": 1})),
            Some(SortCondition::Lte(_))
        ));
        assert!(matches!(
            build_sort_condition(&json!({"gt": 1})),
            Some(SortCondition::Gt(_))
        ));
        assert!(matches!(
            build_sort_condition(&json!({"gte": 1})),
            Some(SortCondition::Gte(_))
        ));
        assert!(matches!(
            build_sort_condition(&json!({"between": [1,2]})),
            Some(SortCondition::Between(_, _))
        ));
        assert!(build_sort_condition(&json!({"between": [1,2,3]})).is_none());
        assert!(matches!(
            build_sort_condition(&json!({"begins_with": "a"})),
            Some(SortCondition::BeginsWith(_))
        ));
        assert!(matches!(
            build_sort_condition(&json!({"contains": "a"})),
            Some(SortCondition::Contains(_))
        ));
        assert!(build_sort_condition(&json!({"unknown": "x"})).is_none());
    }
}
