//! A local JSON-lines service for keeping file-backed Virtuus tables resident.
//!
//! The protocol deliberately uses JSON values so clients in other languages can
//! use the same daemon.  Each opened table is keyed by its canonical directory
//! and declarative table specification.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::table::{StorageMode, ValidationMode};
use crate::Table;

/// Current compatible version of the local service protocol.
pub const PROTOCOL_VERSION: &str = "1.0";

/// A declarative secondary index used when a client opens a table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexSpec {
    pub name: String,
    pub partition_key: String,
    #[serde(default)]
    pub sort_key: Option<String>,
}

/// Declarative table configuration accepted by the service.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableSpec {
    pub name: String,
    pub directory: PathBuf,
    pub primary_key: String,
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    #[serde(default)]
    pub pretty_json: bool,
    #[serde(default)]
    pub validation: Option<String>,
    #[serde(default = "default_reconcile_seconds")]
    pub reconcile_seconds: u64,
}

fn default_reconcile_seconds() -> u64 {
    2
}

struct ResidentTable {
    table: Table,
    reconcile_every: Duration,
    last_reconcile: Instant,
}

/// Retained table registry behind a local service.
pub struct Service {
    tables: HashMap<String, ResidentTable>,
}

impl Service {
    /// Create an empty service.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    fn handle_for(spec: &TableSpec) -> String {
        let directory =
            fs::canonicalize(&spec.directory).unwrap_or_else(|_| spec.directory.clone());
        format!("{}:{}", directory.display(), spec.name)
    }

    /// Open a memory-resident table or return its existing stable handle.
    pub fn open_table(&mut self, spec: TableSpec) -> Result<String, String> {
        let handle = Self::handle_for(&spec);
        if self.tables.contains_key(&handle) {
            return Ok(handle);
        }
        let validation = match spec.validation.as_deref() {
            Some("silent") => ValidationMode::Silent,
            Some("warn") => ValidationMode::Warn,
            Some("error") | None => ValidationMode::Error,
            Some(value) => return Err(format!("invalid validation mode: {value}")),
        };
        let mut table = Table::new(
            &spec.name,
            Some(&spec.primary_key),
            None,
            None,
            Some(spec.directory.clone()),
            validation,
        )
        .map_err(|error| error.to_string())?;
        table.set_storage_mode(StorageMode::Memory);
        table.set_pretty_json(spec.pretty_json);
        table.set_auto_refresh(false);
        // Avoid directory walks on ordinary requests. The service performs a
        // forced reconciliation at the configured interval.
        table.set_check_interval(spec.reconcile_seconds);
        for index in spec.indexes {
            table.add_gsi(&index.name, &index.partition_key, index.sort_key.as_deref());
        }
        table
            .try_load_from_dir(None)
            .map_err(|error| error.to_string())?;
        self.tables.insert(
            handle.clone(),
            ResidentTable {
                table,
                reconcile_every: Duration::from_secs(spec.reconcile_seconds),
                last_reconcile: Instant::now(),
            },
        );
        Ok(handle)
    }

    fn table(&mut self, handle: &str) -> Result<&mut ResidentTable, String> {
        let resident = self
            .tables
            .get_mut(handle)
            .ok_or_else(|| format!("unknown table handle: {handle}"))?;
        if resident.last_reconcile.elapsed() >= resident.reconcile_every {
            resident.table.refresh();
            resident.last_reconcile = Instant::now();
        }
        Ok(resident)
    }

    /// Process one protocol request. A successful response always has `ok: true`.
    pub fn dispatch(&mut self, request: Value) -> Value {
        let action = request.get("action").and_then(Value::as_str).unwrap_or("");
        let result = match action {
            "ping" => Ok(json!({"protocol_version": PROTOCOL_VERSION})),
            "open_table" => serde_json::from_value::<TableSpec>(request.get("spec").cloned().unwrap_or(Value::Null))
                .map_err(|error| error.to_string()).and_then(|spec| self.open_table(spec).map(|handle| json!({"handle": handle}))),
            "status" => Ok(json!({"protocol_version": PROTOCOL_VERSION, "tables": self.tables.len()})),
            "shutdown" => Ok(json!({"shutdown": true})),
            "get" => self.with_table(&request, |table| {
                let pk = request.get("pk").and_then(Value::as_str).ok_or("missing pk")?;
                Ok(table.get(pk, None).unwrap_or(Value::Null))
            }),
            "scan" => self.with_table(&request, |table| Ok(Value::Array(table.scan()))),
            "query" => self.with_table(&request, |table| {
                let index = request.get("index").and_then(Value::as_str).ok_or("missing index")?;
                let value = request.get("value").ok_or("missing value")?;
                Ok(Value::Array(table.query_gsi(index, value, None, false)))
            }),
            "put" => self.with_table(&request, |table| {
                let record = request.get("record").cloned().ok_or("missing record")?;
                table.try_put(record).map_err(|error| error.to_string())?;
                Ok(Value::Null)
            }),
            "put_many" => self.with_table(&request, |table| {
                let records = request.get("records").and_then(Value::as_array).ok_or("missing records")?;
                for record in records {
                    table.try_put(record.clone()).map_err(|error| error.to_string())?;
                }
                Ok(json!({"count": records.len()}))
            }),
            "delete" => self.with_table(&request, |table| {
                let pk = request.get("pk").and_then(Value::as_str).ok_or("missing pk")?;
                table.try_delete(pk, None).map_err(|error| error.to_string())?;
                Ok(Value::Null)
            }),
            "refresh" => self.with_table(&request, |table| {
                let summary = table.refresh();
                Ok(json!({"added": summary.added, "modified": summary.modified, "deleted": summary.deleted, "reread": summary.reread}))
            }),
            _ => Err(format!("unknown action: {action}")),
        };
        match result {
            Ok(result) => json!({"ok": true, "result": result}),
            Err(error) => json!({"ok": false, "error": error}),
        }
    }

    fn with_table<F>(&mut self, request: &Value, operation: F) -> Result<Value, String>
    where
        F: FnOnce(&mut Table) -> Result<Value, String>,
    {
        let handle = request
            .get("handle")
            .and_then(Value::as_str)
            .ok_or("missing handle")?
            .to_string();
        let resident = self.table(&handle)?;
        operation(&mut resident.table)
    }
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

/// Serve the protocol over a Unix-domain socket until a shutdown request.
#[cfg(unix)]
pub fn serve(socket_path: &std::path::Path) -> Result<(), String> {
    if socket_path.exists() {
        fs::remove_file(socket_path).map_err(|error| error.to_string())?;
    }
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let listener = UnixListener::bind(socket_path).map_err(|error| error.to_string())?;
    let mut service = Service::new();
    for stream in listener.incoming() {
        let stream = stream.map_err(|error| error.to_string())?;
        if handle_stream(&mut service, stream)? {
            break;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn handle_stream(service: &mut Service, stream: UnixStream) -> Result<bool, String> {
    let mut reader = BufReader::new(stream.try_clone().map_err(|error| error.to_string())?);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    let request: Value = serde_json::from_str(&line).map_err(|error| error.to_string())?;
    let shutdown = request.get("action").and_then(Value::as_str) == Some("shutdown");
    let response = service.dispatch(request);
    let mut stream = stream;
    serde_json::to_writer(&mut stream, &response).map_err(|error| error.to_string())?;
    stream.write_all(b"\n").map_err(|error| error.to_string())?;
    Ok(shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_table_reads_and_writes_records() {
        let root =
            std::env::temp_dir().join(format!("virtuus-service-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut service = Service::new();
        let open = service.dispatch(json!({"action":"open_table","spec":{"name":"issues","directory":root,"primary_key":"id","pretty_json":true}}));
        let handle = open["result"]["handle"].as_str().unwrap().to_string();
        assert!(service.dispatch(
            json!({"action":"put","handle":handle,"record":{"id":"one","status":"open"}})
        )["ok"]
            .as_bool()
            .unwrap());
        assert_eq!(
            service.dispatch(json!({"action":"scan","handle":handle}))["result"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(root.join("one.json").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
