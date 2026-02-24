/// The library version, read from Cargo.toml at compile time.
/// Cargo.toml is kept in sync with the repo-root VERSION file.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod database;
pub mod gsi;
pub mod search;
pub mod sort;
pub mod table;
pub use database::Database;
pub use gsi::Gsi;
pub use search::SearchIndex;
pub use sort::SortCondition;
pub use table::Table;

#[cfg(feature = "python")]
mod python;

/// Returns the library version string.
pub fn version() -> &'static str {
    VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_non_empty() {
        assert!(!version().is_empty());
    }

    #[test]
    fn version_constant_matches_cargo_pkg() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }
}
