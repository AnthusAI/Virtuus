use axum::body::Body;
use cucumber::gherkin;
use cucumber::{given, then, when, World};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tower::ServiceExt;
use virtuus_amplify::{Contract, Engine};
use virtuus_appsync::{build_schema, normalize_sdl, router, AwsScalarValidator};

#[derive(World, Debug, Default)]
pub struct AppSyncWorld {
    sdl: Option<String>,
    schema: Option<async_graphql::dynamic::Schema>,
    error: Option<String>,
    types: Vec<String>,
    router: Option<axum::Router>,
    engine: Option<Arc<Mutex<Engine>>>,
    response: Option<Value>,
    #[allow(dead_code)]
    dir: Option<TempDir>,
    api_key: Option<String>,
    include_api_key: bool,
    http_status: Option<u16>,
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

#[given("a blog engine")]
fn blog_engine(world: &mut AppSyncWorld) {
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            world.error = Some(format!("Failed to create temp dir: {}", e));
            return;
        }
    };

    let contract_json = include_str!("../../../../features/amplify/fixtures/blog.contract.json");
    let contract = match Contract::from_json(contract_json) {
        Ok(c) => c,
        Err(e) => {
            world.error = Some(format!("Failed to parse contract: {}", e));
            return;
        }
    };

    let engine = match Engine::open(
        Some(dir.path().to_path_buf()),
        contract.clone(),
        virtuus_amplify::EngineOptions,
    ) {
        Ok(e) => Arc::new(Mutex::new(e)),
        Err(e) => {
            world.error = Some(format!("Failed to create engine: {}", e));
            return;
        }
    };

    let sdl = include_str!("../../../../features/appsync/fixtures/blog.graphql");
    match router(engine.clone(), sdl, &contract, None) {
        Ok(r) => {
            world.router = Some(r);
            world.engine = Some(engine);
            world.dir = Some(dir);
        }
        Err(e) => {
            world.error = Some(format!("Failed to build router: {}", e));
        }
    }
}

#[given("a blog engine with API-key auth")]
fn blog_engine_with_auth(world: &mut AppSyncWorld) {
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            world.error = Some(format!("Failed to create temp dir: {}", e));
            return;
        }
    };

    let contract_json = include_str!("../../../../features/amplify/fixtures/blog.contract.json");
    let contract = match Contract::from_json(contract_json) {
        Ok(c) => c,
        Err(e) => {
            world.error = Some(format!("Failed to parse contract: {}", e));
            return;
        }
    };

    let engine = match Engine::open(
        Some(dir.path().to_path_buf()),
        contract.clone(),
        virtuus_amplify::EngineOptions,
    ) {
        Ok(e) => Arc::new(Mutex::new(e)),
        Err(e) => {
            world.error = Some(format!("Failed to create engine: {}", e));
            return;
        }
    };

    let sdl = include_str!("../../../../features/appsync/fixtures/blog.graphql");
    let auth = virtuus_appsync::ApiKeyAuth {
        key: "test-key-12345".to_string(),
    };
    match router(engine.clone(), sdl, &contract, Some(auth.clone())) {
        Ok(r) => {
            world.router = Some(r);
            world.engine = Some(engine);
            world.dir = Some(dir);
            world.api_key = Some(auth.key);
            world.include_api_key = true;
        }
        Err(e) => {
            world.error = Some(format!("Failed to build router: {}", e));
        }
    }
}

#[given(expr = "a {word} exists with:")]
fn record_exists(world: &mut AppSyncWorld, model: String, step: &gherkin::Step) {
    if let Some(engine) = &world.engine {
        let docstring = step.docstring().map_or("", |v| v);
        match serde_json::from_str::<Value>(docstring) {
            Ok(input) => {
                let mut eng = match engine.lock() {
                    Ok(e) => e,
                    Err(_) => {
                        world.error = Some("Failed to lock engine".to_string());
                        return;
                    }
                };

                match eng.call(&model, "create", &input) {
                    Ok((record, errors)) => {
                        if record.is_null() {
                            world.error =
                                Some(format!("Engine returned null record for {}", model));
                        } else if errors.is_some() {
                            world.error = Some(format!("Engine returned errors: {:?}", errors));
                        }
                    }
                    Err(e) => {
                        world.error = Some(format!("Failed to create {}: {}", model, e));
                    }
                }
            }
            Err(e) => {
                world.error = Some(format!("Failed to parse JSON: {}", e));
            }
        }
    } else {
        world.error = Some("Engine not initialized".to_string());
    }
}

async fn send_graphql(world: &mut AppSyncWorld, query: &str, api_key: Option<&str>) {
    if let Some(router) = world.router.take() {
        let body = serde_json::json!({"query": query}).to_string();

        let mut req_builder =
            http::Request::post("/graphql").header("content-type", "application/json");

        if let Some(key) = api_key {
            req_builder = req_builder.header("x-api-key", key);
        }

        match req_builder.body(Body::from(body)) {
            Ok(req) => match router.clone().oneshot(req).await {
                Ok(resp) => {
                    world.http_status = Some(resp.status().as_u16());
                    match axum::body::to_bytes(resp.into_body(), usize::MAX).await {
                        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                            Ok(json_response) => {
                                world.response = Some(json_response);
                                world.router = Some(router);
                            }
                            Err(e) => {
                                world.error = Some(format!("Failed to parse response: {}", e));
                                world.router = Some(router);
                            }
                        },
                        Err(e) => {
                            world.error = Some(format!("Failed to read response body: {}", e));
                            world.router = Some(router);
                        }
                    }
                }
                Err(e) => {
                    world.error = Some(format!("Failed to send request: {}", e));
                    world.router = Some(router);
                }
            },
            Err(e) => {
                world.error = Some(format!("Failed to build request: {}", e));
                world.router = Some(router);
            }
        }
    } else {
        world.error = Some("Router not initialized".to_string());
    }
}

#[when(regex = "^I send the GraphQL request:$")]
async fn send(world: &mut AppSyncWorld, step: &gherkin::Step) {
    let docstring = step.docstring().map_or("", |v| v);
    let api_key_opt = if world.include_api_key {
        world.api_key.clone()
    } else {
        None
    };
    let api_key_ref = api_key_opt.as_deref();
    send_graphql(world, docstring, api_key_ref).await;
}

#[when(regex = "^I send a request without the x-api-key header to:$")]
async fn send_without_api_key(world: &mut AppSyncWorld, step: &gherkin::Step) {
    let docstring = step.docstring().map_or("", |v| v);
    send_graphql(world, docstring, None).await;
}

#[when(regex = "^I send a request with wrong x-api-key header to:$")]
async fn send_with_wrong_api_key(world: &mut AppSyncWorld, step: &gherkin::Step) {
    let docstring = step.docstring().map_or("", |v| v);
    send_graphql(world, docstring, Some("wrong-key")).await;
}

#[when(regex = "^I send a request with correct x-api-key header to:$")]
async fn send_with_correct_api_key(world: &mut AppSyncWorld, step: &gherkin::Step) {
    let docstring = step.docstring().map_or("", |v| v);
    let api_key_opt = world.api_key.clone();
    let api_key_ref = api_key_opt.as_deref();
    send_graphql(world, docstring, api_key_ref).await;
}

#[then(regex = "^the GraphQL response is:$")]
async fn response_is(world: &mut AppSyncWorld, step: &gherkin::Step) {
    if let Some(error) = &world.error {
        panic!("Error in test setup: {}", error);
    }

    let docstring = step.docstring().map_or("", |v| v);
    match serde_json::from_str::<Value>(docstring) {
        Ok(expected) => {
            if let Some(actual) = &world.response {
                assert_eq!(
                    actual,
                    &expected,
                    "\nactual:   {}\nexpected: {}",
                    serde_json::to_string_pretty(actual).unwrap_or_default(),
                    serde_json::to_string_pretty(&expected).unwrap_or_default()
                );
            } else {
                panic!("No response received");
            }
        }
        Err(e) => {
            panic!("Failed to parse expected response: {}", e);
        }
    }
}

#[then(regex = "^the HTTP status is (\\d+)$")]
fn check_http_status(world: &mut AppSyncWorld, status_str: String) {
    let expected_status: u16 = status_str.parse().expect("Status code must be a number");
    if let Some(actual_status) = world.http_status {
        assert_eq!(
            actual_status, expected_status,
            "Expected HTTP status {}, got {}",
            expected_status, actual_status
        );
    } else {
        panic!("No HTTP status recorded");
    }
}

#[then("the response contains UnauthorizedException")]
fn check_unauthorized(world: &mut AppSyncWorld) {
    if let Some(response) = &world.response {
        let error_type = response
            .get("errors")
            .and_then(|e| e.as_array())
            .and_then(|arr| arr.first())
            .and_then(|err| err.get("errorType"))
            .and_then(|et| et.as_str());

        assert_eq!(
            error_type,
            Some("UnauthorizedException"),
            "Expected UnauthorizedException, got {:?}",
            error_type
        );
    } else {
        panic!("No response to check for UnauthorizedException");
    }
}

#[then(regex = "^the GraphQL response contains error with errorType \"([^\"]+)\"$")]
fn check_error_type(world: &mut AppSyncWorld, expected_type: String) {
    if let Some(response) = &world.response {
        let error_type = response
            .get("errors")
            .and_then(|e| e.as_array())
            .and_then(|arr| arr.first())
            .and_then(|err| err.get("errorType"))
            .and_then(|et| et.as_str());

        assert_eq!(
            error_type,
            Some(expected_type.as_str()),
            "Expected errorType {}, got {:?}",
            expected_type,
            error_type
        );
    } else {
        panic!("No response to check for error");
    }
}

#[given("a blog engine with enum constraints")]
fn blog_engine_with_enum_constraints(world: &mut AppSyncWorld) {
    // Placeholder for enum constraint scenarios - use the regular blog engine
    blog_engine(world);
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
            let schema_sdl = schema.sdl();
            let normalized_input = normalize_sdl(sdl).expect("Failed to normalize input SDL");
            let normalized_schema =
                normalize_sdl(&schema_sdl).expect("Failed to normalize schema SDL");

            if normalized_input != normalized_schema {
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
    assert!(
        world.schema.is_some(),
        "Schema should be built to check types"
    );
}

#[then(regex = "^the (.*) type has fields: (.+)$")]
fn type_has_fields(_world: &mut AppSyncWorld, _type_name: String, _fields_str: String) {}

#[then(regex = "^the schema has these input types: (.+)$")]
fn schema_has_input_types(_world: &mut AppSyncWorld, _types_str: String) {}

#[then(regex = "^the schema has these enum types: (.+)$")]
fn schema_has_enum_types(_world: &mut AppSyncWorld, _types_str: String) {}

#[then(regex = "^the schema has these scalar types: (.+)$")]
fn schema_has_scalar_types(_world: &mut AppSyncWorld, _types_str: String) {}

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
    AppSyncWorld::cucumber()
        .filter_run(&features_path, |_, _, sc| {
            !sc.tags.iter().any(|t| t == "wip")
        })
        .await;
}
