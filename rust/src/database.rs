//! Minimal database container for tables.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};
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
                let mut desc_map = match table.describe() {
                    Value::Object(map) => map,
                    _ => Map::new(),
                };
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
            let (association_defs, association_names, key_field, records) = {
                let table = self.tables.get_mut(&table_name).expect("table missing");
                (
                    table.association_defs().clone(),
                    table.associations().clone(),
                    table.key_field().unwrap_or("id").to_string(),
                    table.scan(),
                )
            };
            for assoc_name in association_names {
                let Some(def) = association_defs.get(&assoc_name) else {
                    continue;
                };
                if let Association::BelongsTo {
                    target_table,
                    foreign_key,
                } = def
                {
                    for record in &records {
                        let Some(fk_value) = record.get(foreign_key) else {
                            continue;
                        };
                        let fk_owned = fk_value.to_string();
                        let fk_str = fk_value.as_str().unwrap_or(&fk_owned);
                        let target_missing = self
                            .tables
                            .get(target_table)
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
                    if let Some(root) = data_root {
                        root.join(d).to_string_lossy().to_string()
                    } else {
                        d.to_string()
                    }
                });
            let mut table = Table::new(
                name,
                primary_key.as_deref(),
                partition_key.as_deref(),
                sort_key.as_deref(),
                directory.clone().map(PathBuf::from),
                crate::table::ValidationMode::Warn,
            );
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
                    match kind {
                        "belongs_to" => {
                            let target = assoc_conf
                                .get(YamlValue::from("table"))
                                .and_then(|v| v.as_str())
                                .expect("target table");
                            let fk = assoc_conf
                                .get(YamlValue::from("foreign_key"))
                                .and_then(|v| v.as_str())
                                .expect("foreign_key");
                            table.add_belongs_to(assoc_name, target, fk);
                        }
                        "has_many" => {
                            let target = assoc_conf
                                .get(YamlValue::from("table"))
                                .and_then(|v| v.as_str())
                                .expect("target table");
                            let index = assoc_conf
                                .get(YamlValue::from("index"))
                                .and_then(|v| v.as_str())
                                .expect("index");
                            table.add_has_many(assoc_name, target, index);
                        }
                        "has_many_through" => {
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
                        _ => {}
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
            let pk_str = pk_value
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| pk_value.to_string());
            let sort = directive.get("sort").and_then(|v| v.as_str());
            let mut record = table.get(&pk_str, sort).unwrap_or(Value::Null);
            if let Some(fields) = directive.get("fields").and_then(|v| v.as_array()) {
                record = project(&record, fields);
            }
            let include_map = directive.get("include").and_then(|v| v.as_object());
            return self.apply_includes(table_name, record, include_map);
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
        let assoc = {
            let table_ref = self.tables.get(table).expect("table not found");
            table_ref
                .association(association)
                .cloned()
                .expect("association not found")
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
                let key_field = {
                    let table_ref = self.tables.get(table).unwrap();
                    table_ref.key_field().map(|s| s.to_string())
                };
                let field = match key_field {
                    Some(f) => f,
                    None => return Value::Array(vec![]),
                };
                let key_value = match record.get(&field) {
                    Some(value) => value.clone(),
                    None => return Value::Array(vec![]),
                };
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
                let key_field = {
                    let table_ref = self.tables.get(table).unwrap();
                    table_ref.key_field().map(|s| s.to_string())
                };
                let field = match key_field {
                    Some(f) => f,
                    None => return Value::Array(vec![]),
                };
                let key_value = match record.get(&field) {
                    Some(value) => value.clone(),
                    None => return Value::Array(vec![]),
                };
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
