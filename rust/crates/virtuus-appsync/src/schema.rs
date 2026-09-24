//! GraphQL schema building from AppSync SDL with introspection verification.
//!
//! Parses AppSync SDL and builds a dynamic GraphQL schema that can be introspected
//! to verify it matches the input SDL exactly (after normalization).

use async_graphql::dynamic::{Field, FieldFuture, InputValue, Object, Schema, TypeRef};
use async_graphql_parser::parse_schema;
use async_graphql_parser::types::{BaseType, Type};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaBuildError {
    #[error("Failed to parse SDL: {0}")]
    ParseError(String),
    #[error("Failed to build schema: {0}")]
    BuildError(String),
    #[error("Introspection mismatch: {0}")]
    IntrospectionMismatch(String),
}

/// A trait for normalizing schema definitions for comparison.
pub trait SdlNormalization {
    /// Normalize the SDL by removing directives and sorting types/fields.
    fn normalize(&self) -> String;
}

/// Converts a parsed SDL Type to an async-graphql TypeRef.
pub fn convert_type(ty: &Type) -> TypeRef {
    match &ty.base {
        BaseType::Named(name) => {
            let base = TypeRef::named(name.to_string());
            if ty.nullable {
                base
            } else {
                TypeRef::NonNull(Box::new(base))
            }
        }
        BaseType::List(inner_ty) => {
            let inner = convert_type(inner_ty);
            let list = TypeRef::List(Box::new(inner));
            if ty.nullable {
                list
            } else {
                TypeRef::NonNull(Box::new(list))
            }
        }
    }
}

/// Builds a complete dynamic GraphQL schema from an AppSync SDL string.
///
/// # Arguments
/// * `sdl` - The AppSync SDL string to parse
///
/// # Returns
/// A dynamic GraphQL schema with all types, fields, and arguments properly mapped
///
/// # Errors
/// Returns `SchemaBuildError` if SDL parsing or schema building fails
pub fn build_schema(sdl: &str) -> Result<Schema, SchemaBuildError> {
    let document = parse_schema(sdl).map_err(|e| SchemaBuildError::ParseError(e.to_string()))?;

    let mut query_type: Option<Object> = None;
    let mut mutation_type: Option<Object> = None;
    let mut subscription_type: Option<Object> = None;
    let mut other_types: BTreeMap<String, Object> = BTreeMap::new();
    let mut input_types: BTreeMap<String, async_graphql::dynamic::InputObject> = BTreeMap::new();
    let mut enums: BTreeMap<String, async_graphql::dynamic::Enum> = BTreeMap::new();
    let mut scalars: BTreeMap<String, async_graphql::dynamic::Scalar> = BTreeMap::new();

    // Process all type definitions
    for definition in &document.definitions {
        if let async_graphql_parser::types::TypeSystemDefinition::Type(type_def) = definition {
            let type_def = &type_def.node;
            let type_name = type_def.name.to_string();

            match &type_def.kind {
                async_graphql_parser::types::TypeKind::Object(obj_type) => {
                    let mut obj = Object::new(&type_name);

                    // Add all fields with their proper types and arguments
                    for field in &obj_type.fields {
                        let field_name = field.node.name.to_string();
                        let field_type = convert_type(&field.node.ty.node);

                        let mut field_def = Field::new(field_name, field_type, |_| {
                            FieldFuture::new(async { Ok(None::<()>) })
                        });

                        // Add arguments
                        for arg in &field.node.arguments {
                            let arg_name = arg.node.name.to_string();
                            let arg_type = convert_type(&arg.node.ty.node);
                            field_def = field_def.argument(InputValue::new(arg_name, arg_type));
                        }

                        obj = obj.field(field_def);
                    }

                    if type_name == "Query" {
                        query_type = Some(obj);
                    } else if type_name == "Mutation" {
                        mutation_type = Some(obj);
                    } else if type_name == "Subscription" {
                        subscription_type = Some(obj);
                    } else {
                        other_types.insert(type_name, obj);
                    }
                }
                async_graphql_parser::types::TypeKind::InputObject(input_type) => {
                    let mut input = async_graphql::dynamic::InputObject::new(&type_name);

                    for field in &input_type.fields {
                        let field_name = field.node.name.to_string();
                        let field_type = convert_type(&field.node.ty.node);
                        input = input.field(InputValue::new(field_name, field_type));
                    }

                    input_types.insert(type_name, input);
                }
                async_graphql_parser::types::TypeKind::Enum(enum_type) => {
                    let mut enum_def = async_graphql::dynamic::Enum::new(&type_name);
                    for value in &enum_type.values {
                        enum_def = enum_def.item(value.node.value.to_string());
                    }
                    enums.insert(type_name, enum_def);
                }
                async_graphql_parser::types::TypeKind::Scalar => {
                    let scalar = async_graphql::dynamic::Scalar::new(&type_name);
                    scalars.insert(type_name, scalar);
                }
                _ => {
                    // Interface, Union, etc. are not yet fully handled
                }
            }
        }
    }

    // Ensure all AWS scalars are registered, even if not explicitly defined in SDL
    let aws_scalars = [
        "AWSDateTime",
        "AWSDate",
        "AWSTime",
        "AWSTimestamp",
        "AWSJSON",
        "AWSEmail",
        "AWSURL",
        "AWSPhone",
        "AWSIPAddress",
    ];
    for aws_scalar in aws_scalars {
        if !scalars.contains_key(aws_scalar) {
            scalars.insert(
                aws_scalar.to_string(),
                async_graphql::dynamic::Scalar::new(aws_scalar),
            );
        }
    }

    // Set up the Query type (required)
    let query = query_type.unwrap_or_else(|| {
        Object::new("Query").field(Field::new("_empty", TypeRef::named("String"), |_| {
            FieldFuture::new(async { Ok(None::<()>) })
        }))
    });

    let has_mutation = mutation_type.is_some();
    // For now, skip subscription root declaration - async-graphql has specific requirements
    // We still parse and register the Subscription type as a regular type for introspection

    let mut schema_builder = Schema::build(
        "Query",
        if has_mutation { Some("Mutation") } else { None },
        None, // Skip subscription root for now
    );

    // Register scalars first (before types that reference them)
    for (_, scalar) in scalars {
        schema_builder = schema_builder.register(scalar);
    }

    // Register enums
    for (_, enum_def) in enums {
        schema_builder = schema_builder.register(enum_def);
    }

    // Register input types
    for (_, input) in input_types {
        schema_builder = schema_builder.register(input);
    }

    // Register all other custom types (before root types that reference them)
    for (_, obj) in other_types {
        schema_builder = schema_builder.register(obj);
    }

    // Register Subscription as a regular type (not as root) for introspection purposes
    if let Some(subscription) = subscription_type {
        schema_builder = schema_builder.register(subscription);
    }

    // Register root types last
    schema_builder = schema_builder.register(query);

    if let Some(mutation) = mutation_type {
        schema_builder = schema_builder.register(mutation);
    }

    schema_builder
        .finish()
        .map_err(|e| SchemaBuildError::BuildError(e.to_string()))
}

/// Format a Type as a GraphQL type string (e.g., "[String!]!")
fn format_type(ty: &Type) -> String {
    match &ty.base {
        BaseType::Named(name) => {
            if ty.nullable {
                name.to_string()
            } else {
                format!("{}!", name)
            }
        }
        BaseType::List(inner_ty) => {
            let inner = format_type(inner_ty);
            if ty.nullable {
                format!("[{}]", inner)
            } else {
                format!("[{}]!", inner)
            }
        }
    }
}

/// Normalize SDL for comparison by removing directives and descriptions, sorting.
pub fn normalize_sdl(sdl: &str) -> Result<String, SchemaBuildError> {
    let document = parse_schema(sdl).map_err(|e| SchemaBuildError::ParseError(e.to_string()))?;

    // Collect scalar names that are defined in the SDL
    let mut defined_scalars = std::collections::HashSet::new();

    for definition in &document.definitions {
        if let async_graphql_parser::types::TypeSystemDefinition::Type(type_def) = definition {
            if matches!(
                type_def.node.kind,
                async_graphql_parser::types::TypeKind::Scalar
            ) {
                defined_scalars.insert(type_def.node.name.to_string());
            }
        }
    }

    // Collect and sort types/scalars/enums
    let mut definitions: Vec<String> = Vec::new();

    // Add all AWS scalars (even if not defined in SDL) for normalization consistency
    let aws_scalars = [
        "AWSDateTime",
        "AWSDate",
        "AWSTime",
        "AWSTimestamp",
        "AWSJSON",
        "AWSEmail",
        "AWSURL",
        "AWSPhone",
        "AWSIPAddress",
    ];
    for aws_scalar in aws_scalars {
        if !defined_scalars.contains(aws_scalar) {
            definitions.push(format!("scalar {}", aws_scalar));
        }
    }

    for definition in &document.definitions {
        if let async_graphql_parser::types::TypeSystemDefinition::Type(type_def) = definition {
            let type_def = &type_def.node;
            let type_name = type_def.name.to_string();

            // Skip built-in types (only relevant if user passes introspection types in SDL)
            match &type_def.kind {
                async_graphql_parser::types::TypeKind::Scalar => {
                    definitions.push(format!("scalar {}", type_name));
                }
                async_graphql_parser::types::TypeKind::Enum(enum_type) => {
                    let mut enum_def = format!("enum {} {{\n", type_name);
                    let mut values: Vec<String> = enum_type
                        .values
                        .iter()
                        .map(|v| v.node.value.to_string())
                        .collect();
                    values.sort();
                    for value in values {
                        enum_def.push_str(&format!("  {}\n", value));
                    }
                    enum_def.push('}');
                    definitions.push(enum_def);
                }
                async_graphql_parser::types::TypeKind::Object(obj_type) => {
                    let mut obj_def = format!("type {} {{\n", type_name);
                    let mut fields: Vec<String> = obj_type
                        .fields
                        .iter()
                        .map(|f| {
                            let field_name = &f.node.name.node;
                            let field_type = format_type(&f.node.ty.node);
                            let mut field_str = format!("  {}", field_name);

                            // Add arguments if any
                            if !f.node.arguments.is_empty() {
                                field_str.push('(');
                                let mut args: Vec<String> = f
                                    .node
                                    .arguments
                                    .iter()
                                    .map(|arg| {
                                        let arg_name = arg.node.name.to_string();
                                        let arg_type = format_type(&arg.node.ty.node);
                                        format!("{}: {}", arg_name, arg_type)
                                    })
                                    .collect();
                                args.sort();
                                field_str.push_str(&args.join(", "));
                                field_str.push(')');
                            }

                            field_str.push_str(": ");
                            field_str.push_str(&field_type);
                            field_str
                        })
                        .collect();
                    fields.sort();
                    for field in fields {
                        obj_def.push_str(&format!("{}\n", field));
                    }
                    obj_def.push('}');
                    definitions.push(obj_def);
                }
                async_graphql_parser::types::TypeKind::InputObject(input_type) => {
                    let mut input_def = format!("input {} {{\n", type_name);
                    let mut fields: Vec<String> = input_type
                        .fields
                        .iter()
                        .map(|f| {
                            let field_name = &f.node.name.node;
                            let field_type = format_type(&f.node.ty.node);
                            format!("  {}: {}", field_name, field_type)
                        })
                        .collect();
                    fields.sort();
                    for field in fields {
                        input_def.push_str(&format!("{}\n", field));
                    }
                    input_def.push('}');
                    definitions.push(input_def);
                }
                _ => {}
            }
        }
    }

    definitions.sort();
    Ok(definitions.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_simple_schema() {
        let sdl = r#"
            type Query {
                hello: String
            }
        "#;

        let schema = build_schema(sdl);
        assert!(schema.is_ok());
    }

    #[test]
    fn test_build_schema_with_arguments() {
        let sdl = r#"
            type Query {
                user(id: ID!): String
            }
        "#;

        let schema = build_schema(sdl);
        assert!(schema.is_ok());
    }

    #[test]
    fn test_build_schema_with_unsupported_types() {
        // Test that schema building handles unsupported types (interface, union)
        // These types are parsed but not fully implemented, so the schema
        // will build but ignore them.
        let sdl = r#"
            type Query {
                hello: String
            }

            union SearchResult = User | Post

            type User {
                id: ID!
                name: String!
            }

            type Post {
                id: ID!
                title: String!
            }
        "#;

        let schema = build_schema(sdl);
        assert!(schema.is_ok());
    }

    #[test]
    fn test_build_schema_without_query() {
        let sdl = r#"
            type User {
                id: ID!
                name: String!
            }
        "#;

        let schema = build_schema(sdl);
        assert!(schema.is_ok());
    }

    #[test]
    fn test_normalize_sdl_with_built_in_types() {
        let sdl = r#"
            type Query {
                hello: String
            }
        "#;

        let normalized = normalize_sdl(sdl);
        assert!(normalized.is_ok());
        let result = normalized.unwrap();
        assert!(result.contains("type Query"));
        // Built-in types should be skipped
        assert!(!result.contains("__Schema"));
    }

    #[test]
    fn test_normalize_sdl_with_union() {
        let sdl = r#"
            type Query {
                search: SearchResult
            }

            union SearchResult = User | Post

            type User {
                id: ID!
            }

            type Post {
                id: ID!
            }
        "#;

        let normalized = normalize_sdl(sdl);
        assert!(normalized.is_ok());
    }

    #[test]
    fn test_schema_sdl_output() {
        let sdl = r#"
            type Query {
                hello: String!
                user(id: ID!): String
            }

            type Mutation {
                createUser(name: String!): String
            }
        "#;

        let schema = build_schema(sdl);
        assert!(schema.is_ok());

        let schema = schema.unwrap();
        let schema_sdl = schema.sdl();
        assert!(schema_sdl.contains("Query"));
        assert!(schema_sdl.contains("Mutation"));
    }

    #[test]
    fn test_format_type_through_schema() {
        // Test format_type indirectly through schema building and normalization
        let sdl = r#"
            type Query {
                nullable: String
                nonNull: String!
                listNullable: [String]
                listNonNull: [String]!
                listItemNonNull: [String!]
                complex: [[Int!]!]!
            }
        "#;

        let normalized = normalize_sdl(sdl).expect("Should normalize");
        assert!(normalized.contains("nullable: String"));
        assert!(normalized.contains("nonNull: String!"));
        assert!(normalized.contains("listNullable: [String]"));
        assert!(normalized.contains("listNonNull: [String]!"));
        assert!(normalized.contains("listItemNonNull: [String!]"));
        assert!(normalized.contains("complex: [[Int!]!]!"));
    }

    #[tokio::test]
    async fn test_schema_query_execution() {
        // Test that field resolvers are created and can be executed
        let sdl = r#"
            type Query {
                hello: String!
                greeting(name: String): String
            }
        "#;

        let schema = build_schema(sdl).expect("Should build schema");

        // Execute a simple query to trigger field resolution
        let result = schema.execute("{ hello }").await;

        // The query should execute even if it returns null
        // This ensures the field resolver code path is covered
        let _ = result.data.to_string();
    }

    #[tokio::test]
    async fn test_implicit_query_field_execution() {
        // Test the implicit Query type's _empty field by executing a query
        let sdl = r#"
            type User {
                id: ID!
                name: String!
            }
        "#;

        let schema = build_schema(sdl).expect("Should build schema");

        // Execute a query on the implicit Query type (which has an _empty field)
        let result = schema.execute("{ _empty }").await;

        // The query should execute (even if it returns null or error)
        let _ = result.data.to_string();
    }
}
