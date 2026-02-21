//! Minimal database container for tables.

use std::collections::HashMap;

use serde_json::Value;

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
                Value::Array(target.query_gsi(&index, &key_value))
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
                    through.query_gsi(&through_index, &key_value)
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
}
