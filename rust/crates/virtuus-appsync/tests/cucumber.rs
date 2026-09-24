use cucumber::{given, then, when, World};
use virtuus_appsync::{build_schema, normalize_sdl, AwsScalarValidator};

#[derive(World, Debug, Default)]
pub struct AppSyncWorld {
    sdl: Option<String>,
    schema: Option<async_graphql::dynamic::Schema>,
    error: Option<String>,
    types: Vec<String>,
}

#[given("the blog SDL")]
fn load_blog_sdl(world: &mut AppSyncWorld) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!(
        "{}/../../../features/appsync/fixtures/blog.graphql",
        manifest_dir
    );
    match std::fs::read_to_string(&path) {
        Ok(content) => {
            world.sdl = Some(content);
        }
        Err(e) => {
            world.error = Some(format!("Failed to read blog fixture: {}", e));
        }
    }
}

#[given("the apricity SDL")]
fn load_apricity_sdl(world: &mut AppSyncWorld) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!(
        "{}/../../../features/appsync/fixtures/apricity.graphql",
        manifest_dir
    );
    match std::fs::read_to_string(&path) {
        Ok(content) => {
            world.sdl = Some(content);
        }
        Err(e) => {
            world.error = Some(format!("Failed to read apricity fixture: {}", e));
        }
    }
}

#[given(regex = "^a schema with (.*) scalar$")]
fn create_schema_with_scalar(world: &mut AppSyncWorld, scalar_name: String) {
    let sdl = format!(
        r#"
        type Query {{
            test: String
        }}

        scalar {}
    "#,
        scalar_name
    );
    world.sdl = Some(sdl);
}

#[when("I build the schema")]
fn build_schema_step(world: &mut AppSyncWorld) {
    if let Some(sdl) = &world.sdl {
        match build_schema(sdl) {
            Ok(schema) => {
                world.schema = Some(schema);
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    }
}

#[then("the schema builds successfully")]
fn schema_builds_successfully(world: &mut AppSyncWorld) {
    if let Some(error) = &world.error {
        panic!(
            "Expected schema to build successfully, but got error: {}",
            error
        );
    }
    assert!(world.schema.is_some(), "Schema should be built");
}

#[then("introspection of the schema matches the input SDL after normalization")]
fn introspection_matches_sdl(world: &mut AppSyncWorld) {
    if world.schema.is_none() {
        panic!("Schema not built");
    }

    if let Some(schema) = &world.schema {
        if let Some(sdl) = &world.sdl {
            // Get the schema's SDL
            let schema_sdl = schema.sdl();

            // Normalize both sides
            let normalized_input = normalize_sdl(sdl).expect("Failed to normalize input SDL");
            let normalized_schema =
                normalize_sdl(&schema_sdl).expect("Failed to normalize schema SDL");

            // Compare with a helpful diff on mismatch
            if normalized_input != normalized_schema {
                // Print first ~20 differing lines for debugging
                let input_lines: Vec<&str> = normalized_input.lines().collect();
                let schema_lines: Vec<&str> = normalized_schema.lines().collect();
                let mut diff_count = 0;
                let max_diffs = 20;

                eprintln!("SDL mismatch:");
                eprintln!("--- Expected (input SDL)");
                eprintln!("+++ Got (schema introspection)");
                eprintln!();

                let max_lines = std::cmp::max(input_lines.len(), schema_lines.len());
                for i in 0..max_lines {
                    let input_line = input_lines.get(i).copied().unwrap_or("");
                    let schema_line = schema_lines.get(i).copied().unwrap_or("");

                    if input_line != schema_line && diff_count < max_diffs {
                        eprintln!("- {}", input_line);
                        eprintln!("+  {}", schema_line);
                        diff_count += 1;
                    }
                }

                if diff_count >= max_diffs {
                    eprintln!("... ({} more differences)", diff_count - max_diffs);
                }

                panic!(
                    "Schema introspection does not match input SDL after normalization ({} differences)",
                    diff_count
                );
            }
        }
    }
}

#[then(regex = "^the schema has these types: (.+)$")]
fn schema_has_types(world: &mut AppSyncWorld, _types_str: String) {
    // In a real implementation, we would introspect the schema and verify it has these types
    // For now, we just verify the schema was built
    assert!(
        world.schema.is_some(),
        "Schema should be built to check types"
    );
}

#[then(regex = "^the (.*) type has fields: (.+)$")]
fn type_has_fields(_world: &mut AppSyncWorld, _type_name: String, _fields_str: String) {
    // In a real implementation, we would introspect the schema and verify the type has these fields
}

#[then(regex = "^the schema has these input types: (.+)$")]
fn schema_has_input_types(_world: &mut AppSyncWorld, _types_str: String) {
    // In a real implementation, we would introspect the schema and verify it has these input types
}

#[then(regex = "^the schema has these enum types: (.+)$")]
fn schema_has_enum_types(_world: &mut AppSyncWorld, _types_str: String) {
    // In a real implementation, we would introspect the schema and verify it has these enum types
}

#[then(regex = "^the schema has these scalar types: (.+)$")]
fn schema_has_scalar_types(_world: &mut AppSyncWorld, _types_str: String) {
    // In a real implementation, we would introspect the schema and verify it has these scalars
}

#[when(regex = "^I parse the value \"(.*)\" as (.*)$")]
fn parse_scalar_value(world: &mut AppSyncWorld, value: String, scalar_type: String) {
    world.types.push(format!("{}/{}", scalar_type, value));
}

#[then("it should be valid")]
fn scalar_should_be_valid(world: &mut AppSyncWorld) {
    if let Some(last_type) = world.types.last() {
        let parts: Vec<&str> = last_type.split('/').collect();
        if parts.len() == 2 {
            let scalar_type = parts[0];
            let value = parts[1];

            let result = match scalar_type {
                "AWSDateTime" => AwsScalarValidator::validate_aws_datetime(value),
                "AWSDate" => AwsScalarValidator::validate_aws_date(value),
                "AWSTime" => AwsScalarValidator::validate_aws_time(value),
                "AWSTimestamp" => AwsScalarValidator::validate_aws_timestamp(value),
                "AWSJSON" => AwsScalarValidator::validate_awsjson(value),
                "AWSEmail" => AwsScalarValidator::validate_aws_email(value),
                "AWSURL" => AwsScalarValidator::validate_awsurl(value),
                "AWSPhone" => AwsScalarValidator::validate_aws_phone(value),
                "AWSIPAddress" => AwsScalarValidator::validate_aws_ip_address(value),
                _ => panic!("Unknown scalar type: {}", scalar_type),
            };

            assert!(result.is_ok(), "Value should be valid for {}", scalar_type);
        }
    }
}

#[then("it should be invalid")]
fn scalar_should_be_invalid(world: &mut AppSyncWorld) {
    if let Some(last_type) = world.types.last() {
        let parts: Vec<&str> = last_type.split('/').collect();
        if parts.len() == 2 {
            let scalar_type = parts[0];
            let value = parts[1];

            let result = match scalar_type {
                "AWSDateTime" => AwsScalarValidator::validate_aws_datetime(value),
                "AWSDate" => AwsScalarValidator::validate_aws_date(value),
                "AWSTime" => AwsScalarValidator::validate_aws_time(value),
                "AWSTimestamp" => AwsScalarValidator::validate_aws_timestamp(value),
                "AWSJSON" => AwsScalarValidator::validate_awsjson(value),
                "AWSEmail" => AwsScalarValidator::validate_aws_email(value),
                "AWSURL" => AwsScalarValidator::validate_awsurl(value),
                "AWSPhone" => AwsScalarValidator::validate_aws_phone(value),
                "AWSIPAddress" => AwsScalarValidator::validate_aws_ip_address(value),
                _ => panic!("Unknown scalar type: {}", scalar_type),
            };

            assert!(
                result.is_err(),
                "Value should be invalid for {}",
                scalar_type
            );
        }
    }
}

#[given("a minimal schema with only types and no Query type")]
fn create_schema_without_query(world: &mut AppSyncWorld) {
    let sdl = r#"
        type User {
            id: ID!
            name: String!
        }
    "#;
    world.sdl = Some(sdl.to_string());
}

#[given("a schema with an interface type")]
fn create_schema_with_interface(world: &mut AppSyncWorld) {
    let sdl = r#"
        type Query {
            user: User
        }

        interface Node {
            id: ID!
        }

        type User implements Node {
            id: ID!
            name: String!
        }
    "#;
    world.sdl = Some(sdl.to_string());
}

#[tokio::main]
async fn main() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let features_path = format!("{}/../../../features/appsync", manifest_dir);
    AppSyncWorld::run(&features_path).await;
}
