//! AppSync-compatible GraphQL server built on the Virtuus storage engine.
//!
//! This crate provides a dynamic GraphQL schema builder that takes an AppSync SDL string
//! and builds an `async_graphql::dynamic::Schema` that matches the input schema exactly.
//! AWS scalars are supported with validation, and AWS directives are declared and ignored.

pub mod router;
pub mod scalars;
pub mod schema;

pub use router::{router, RouterOptions};
pub use scalars::{AwsScalarValidator, ScalarValidationError, ScalarValidatorFn};
pub use schema::{build_schema, normalize_sdl, SchemaBuildError};
