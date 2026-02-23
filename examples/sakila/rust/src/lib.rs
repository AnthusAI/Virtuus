use std::path::PathBuf;

use virtuus::Database;

pub fn base_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("missing parent directory")
        .to_path_buf()
}

pub fn load_db() -> Database {
    let base = base_dir();
    let schema_path = base.join("schema.yml");
    Database::from_schema(&schema_path, Some(&base))
}
