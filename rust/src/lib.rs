/// The library version, read from Cargo.toml at compile time.
/// Cargo.toml is kept in sync with the repo-root VERSION file.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod sort;

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
