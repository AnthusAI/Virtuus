use clap::{Parser, Subcommand};
use serde_json::{json, Map as JsonMap, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use virtuus::table::ValidationMode;
use virtuus::{Database, Table};

#[derive(Parser)]
#[command(name = "virtuus", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a one-off query and print JSON results.
    Query {
        /// Data directory containing table folders.
        #[arg(long)]
        dir: PathBuf,
        /// Optional schema file for database definition.
        #[arg(long)]
        schema: Option<PathBuf>,
        /// Table name to query.
        #[arg(long)]
        table: String,
        /// Optional index name for GSI query.
        #[arg(long)]
        index: Option<String>,
        /// Optional primary key lookup.
        #[arg(long)]
        pk: Option<String>,
        /// Optional where clause in key=value form.
        #[arg(long, value_name = "key=value")]
        r#where: Option<String>,
    },
    /// Start an HTTP server for persistent queries.
    Serve {
        /// Data directory containing table folders.
        #[arg(long)]
        dir: PathBuf,
        /// Schema file for database definition.
        #[arg(long)]
        schema: PathBuf,
        /// Port to listen on.
        #[arg(long, default_value = "8080")]
        port: u16,
    },
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Commands::Query {
            dir,
            schema,
            table,
            index,
            pk,
            r#where,
        } => run_query(dir, schema, table, index, pk, r#where),
        Commands::Serve { dir, schema, port } => run_serve(dir, schema, port),
    }
}

fn run_query(
    dir: PathBuf,
    schema: Option<PathBuf>,
    table: String,
    index: Option<String>,
    pk: Option<String>,
    r#where: Option<String>,
) -> Result<(), String> {
    let where_pair = if let Some(where_clause) = r#where.as_deref() {
        Some(parse_where(where_clause)?)
    } else {
        None
    };

    let mut db = if let Some(schema_path) = schema {
        Database::from_schema(&schema_path, Some(dir.as_path()))
    } else {
        let mut db = Database::new();
        let table_dir = dir.join(&table);
        if !table_dir.exists() {
            return Err(format!("table \"{table}\" not found"));
        }
        let mut tbl = Table::new(
            &table,
            Some("id"),
            None,
            None,
            Some(table_dir),
            ValidationMode::Silent,
        );
        if let (Some(index_name), Some((where_key, _))) = (index.as_deref(), where_pair.as_ref()) {
            tbl.add_gsi(index_name, where_key, None);
        }
        tbl.load_from_dir(None);
        db.add_table(&table, tbl);
        db
    };

    let mut directive = JsonMap::new();
    if let Some(pk_value) = pk {
        directive.insert("pk".to_string(), Value::String(pk_value));
    }
    if let Some(index_name) = index {
        directive.insert("index".to_string(), Value::String(index_name));
    }
    if let Some((key, value)) = where_pair {
        let mut where_map = JsonMap::new();
        where_map.insert(key, Value::String(value));
        directive.insert("where".to_string(), Value::Object(where_map));
    } else if directive.get("index").is_some() && directive.get("pk").is_none() {
        return Err("missing --where for index query".to_string());
    }

    let query = Value::Object(JsonMap::from_iter([(
        table.clone(),
        Value::Object(directive),
    )]));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.execute(&query)))
        .map_err(|err| format!("query failed: {}", panic_message(err)))?;

    let output = if let Some(items) = result.get("items") {
        items.clone()
    } else {
        result
    };
    let json_text = serde_json::to_string(&output).map_err(|err| err.to_string())?;
    println!("{json_text}");
    Ok(())
}

struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

fn run_serve(dir: PathBuf, schema: PathBuf, port: u16) -> Result<(), String> {
    let db = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Database::from_schema(&schema, Some(dir.as_path()))
    }))
    .map_err(|err| format!("failed to load schema: {}", panic_message(err)))?;
    let state = Arc::new(Mutex::new(db));
    let load_count = Arc::new(AtomicUsize::new(1));
    let refresh_count = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind(("127.0.0.1", port)).map_err(|err| err.to_string())?;
    for stream in listener.incoming() {
        let stream = stream.map_err(|err| err.to_string())?;
        let state = Arc::clone(&state);
        let load_count = Arc::clone(&load_count);
        let refresh_count = Arc::clone(&refresh_count);
        thread::spawn(move || {
            if let Err(err) = handle_connection(stream, state, load_count, refresh_count) {
                eprintln!("server error: {err}");
            }
        });
    }
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<Database>>,
    load_count: Arc<AtomicUsize>,
    refresh_count: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let request = match read_request(&mut stream) {
        Ok(req) => req,
        Err(_) => return Ok(()),
    };
    let path = request
        .path
        .split('?')
        .next()
        .unwrap_or(request.path.as_str());
    let mut status = 200u16;
    let response = match (request.method.as_str(), path) {
        ("GET", "/health") => {
            json!({
                "status": "ok",
                "load_count": load_count.load(Ordering::SeqCst),
                "refresh_count": refresh_count.load(Ordering::SeqCst)
            })
        }
        ("POST", "/query") => {
            let text = String::from_utf8_lossy(&request.body);
            match serde_json::from_str::<Value>(&text) {
                Ok(query) => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let mut db = state.lock().expect("db lock");
                        db.execute(&query)
                    }));
                    match result {
                        Ok(value) => value,
                        Err(err) => {
                            status = 400;
                            json!({ "error": panic_message(err) })
                        }
                    }
                }
                Err(err) => {
                    status = 400;
                    json!({ "error": format!("invalid json: {err}") })
                }
            }
        }
        ("POST", "/describe") => {
            let mut db = state.lock().expect("db lock");
            let describe = db.describe();
            serde_json::to_value(describe).unwrap_or_else(|_| json!({}))
        }
        ("POST", "/validate") => {
            let mut db = state.lock().expect("db lock");
            let violations = db.validate();
            json!({
                "valid": violations.is_empty(),
                "violations": violations
            })
        }
        ("POST", "/warm") => {
            let mut db = state.lock().expect("db lock");
            db.warm();
            refresh_count.fetch_add(1, Ordering::SeqCst);
            let tables: Vec<String> = db.tables().keys().cloned().collect();
            json!({
                "status": "ok",
                "tables": tables
            })
        }
        _ => {
            status = 404;
            json!({ "error": "not found" })
        }
    };
    let body = serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_string());
    write_response(&mut stream, status, &body)
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<HttpRequest> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    if request_line.trim().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "empty request",
        ));
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let mut headers = HashMap::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_lowercase(), value.trim().to_string());
        }
    }
    let length = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    if length > 0 {
        reader.read_exact(&mut body)?;
    }
    Ok(HttpRequest { method, path, body })
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn parse_where(input: &str) -> Result<(String, String), String> {
    let mut parts = input.splitn(2, '=');
    let key = parts
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "invalid --where; expected key=value".to_string())?;
    let value = parts
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "invalid --where; expected key=value".to_string())?;
    Ok((key.to_string(), value.to_string()))
}

fn panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = err.downcast_ref::<&str>() {
        msg.to_string()
    } else if let Some(msg) = err.downcast_ref::<String>() {
        msg.clone()
    } else {
        "unknown error".to_string()
    }
}
