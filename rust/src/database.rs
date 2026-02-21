//! Minimal database container for tables.

use std::collections::HashMap;

use serde_json::Value;

use crate::table::Table;

/// Collection of tables with cache helpers.
#[derive(Default)]
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
}
