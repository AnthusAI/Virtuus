use clap::{Parser, Subcommand};
use serde_json::{Map as JsonMap, Value};
use std::path::PathBuf;
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
