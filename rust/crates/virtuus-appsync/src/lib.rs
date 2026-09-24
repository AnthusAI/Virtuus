//! AppSync-compatible GraphQL server built on the Virtuus storage engine.
//!
//! This crate provides a dynamic GraphQL schema builder that takes an AppSync SDL string
//! and builds an `async_graphql::dynamic::Schema` that matches the input schema exactly.
//! AWS scalars are supported with validation, and AWS directives are declared and ignored.

pub mod router;
pub mod scalars;
pub mod schema;

pub use router::{router, ApiKeyAuth};
pub use scalars::{AwsScalarValidator, ScalarValidationError};
pub use schema::{build_schema, normalize_sdl, SchemaBuildError, SdlNormalization};

/// Builds a dynamic GraphQL schema from an AppSync SDL string.
///
/// # Arguments
/// * `sdl` - The AppSync SDL string to parse and build into a schema
///
/// # Returns
/// A dynamic GraphQL schema that can handle queries, mutations, and subscriptions
///
/// # Errors
/// Returns `SchemaBuildError` if the SDL is invalid or schema building fails
pub fn parse_appsync_sdl(sdl: &str) -> Result<async_graphql::dynamic::Schema, SchemaBuildError> {
    build_schema(sdl)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_schema() {
        let sdl = r#"
            type Query {
                hello: String
            }
        "#;

        let result = parse_appsync_sdl(sdl);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_aws_scalars() {
        let sdl = r#"
            type Query {
                created: AWSDateTime
            }

            scalar AWSDateTime
        "#;

        let result = parse_appsync_sdl(sdl);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_aws_directives() {
        let sdl = r#"
            type Query {
                protected: String @aws_auth(rules: [{allow: "private"}])
            }
        "#;

        let result = parse_appsync_sdl(sdl);
        assert!(result.is_ok());
    }
}
