use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_graphql_parser::types::{TypeKind, TypeSystemDefinition};
use axum::body::Body;
use cucumber::gherkin::Step;
use cucumber::{given, then, when, World};
use http::HeaderValue;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;
use virtuus_amplify::{Contract, Engine, EngineOptions, Identity};
use virtuus_appsync::{build_schema, normalize_sdl, router, AwsScalarValidator, RouterOptions};

const BLOG_CONTRACT: &str =
    include_str!("../../../../features/amplify/fixtures/blog.contract.json");
const BLOG_SDL: &str = include_str!("../../../../features/appsync/fixtures/blog.graphql");
const APRICITY_SDL: &str = include_str!("../../../../features/appsync/fixtures/apricity.graphql");
const TEST_API_KEY: &str = "test-key-12345";

#[derive(World, Debug, Default)]
pub struct AppSyncWorld {
    // Schema scenarios
    sdl: Option<String>,
    schema: Option<async_graphql::dynamic::Schema>,
    schema_error: Option<String>,
    scalar_check: Option<(String, String)>,
    // Router scenarios
    contract: Option<Value>,
    router_sdl: Option<String>,
    router_error: Option<String>,
    router: Option<axum::Router>,
    engine: Option<Arc<Mutex<Engine>>>,
    dir: Option<TempDir>,
    api_key: Option<String>,
    http_status: Option<u16>,
    response: Option<Value>,
    next_token: Option<String>,
}

// ── Engine and router setup ────────────────────────────────────────────────

fn contract_json(world: &mut AppSyncWorld) -> &mut Value {
    world
        .contract
        .get_or_insert_with(|| serde_json::from_str(BLOG_CONTRACT).expect("blog contract JSON"))
}

/// Open an engine on the (possibly modified) blog contract and build the router.
fn build_blog_router(world: &mut AppSyncWorld, options: RouterOptions, enforce_auth: bool) {
    let contract_text = contract_json(world).to_string();
    let contract = Contract::from_json(&contract_text).expect("blog contract");
    let dir = tempfile::tempdir().expect("temp dir");
    let engine = Engine::open(
        Some(dir.path().to_path_buf()),
        contract.clone(),
        EngineOptions { enforce_auth },
    )
    .expect("engine");
    let engine = Arc::new(Mutex::new(engine));
    let sdl = world
        .router_sdl
        .clone()
        .unwrap_or_else(|| BLOG_SDL.to_string());
    world.api_key = options.api_key.clone();
    match router(engine.clone(), &sdl, &contract, options) {
        Ok(router) => {
            world.router = Some(router);
            world.engine = Some(engine);
            world.dir = Some(dir);
        }
        Err(error) => world.router_error = Some(error.to_string()),
    }
}

fn start_blog(world: &mut AppSyncWorld, options: RouterOptions, enforce_auth: bool) {
    build_blog_router(world, options, enforce_auth);
    if let Some(error) = &world.router_error {
        panic!("router failed to build: {error}");
    }
}

#[given("a blog engine")]
fn blog_engine(world: &mut AppSyncWorld) {
    start_blog(world, RouterOptions::default(), false);
}

#[given("a blog engine with API-key auth")]
fn blog_engine_with_api_key(world: &mut AppSyncWorld) {
    let options = RouterOptions {
        api_key: Some(TEST_API_KEY.to_string()),
        test_identities: false,
    };
    start_blog(world, options, false);
}

#[given("a blog engine with test identities")]
fn blog_engine_with_test_identities(world: &mut AppSyncWorld) {
    let options = RouterOptions {
        api_key: None,
        test_identities: true,
    };
    start_blog(world, options, false);
}

#[given("a blog engine enforcing authorization, with test identities")]
fn blog_engine_enforcing_auth(world: &mut AppSyncWorld) {
    let options = RouterOptions {
        api_key: None,
        test_identities: true,
    };
    start_blog(world, options, true);
}

#[given(regex = r#"^the blog contract with "([^"]+)" set to:$"#)]
fn blog_contract_with(world: &mut AppSyncWorld, pointer: String, step: &Step) {
    let value: Value = serde_json::from_str(step.docstring().expect("docstring")).expect("JSON");
    let target = contract_json(world)
        .pointer_mut(&pointer)
        .unwrap_or_else(|| panic!("no {pointer} in the blog contract"));
    *target = value;
}

#[given("the blog SDL with this appended:")]
fn blog_sdl_appended(world: &mut AppSyncWorld, step: &Step) {
    let extra = step.docstring().expect("docstring");
    world.router_sdl = Some(format!("{BLOG_SDL}\n{extra}"));
}

#[given("this router SDL:")]
fn router_sdl(world: &mut AppSyncWorld, step: &Step) {
    world.router_sdl = Some(step.docstring().expect("docstring").to_string());
}

#[when("I build the blog router")]
fn build_router_step(world: &mut AppSyncWorld) {
    build_blog_router(world, RouterOptions::default(), false);
}

#[then("building the router fails with:")]
fn router_build_fails(world: &mut AppSyncWorld, step: &Step) {
    let expected = step.docstring().expect("docstring").trim();
    assert_eq!(world.router_error.as_deref(), Some(expected));
}

#[given(expr = "a {word} exists with:")]
fn record_exists(world: &mut AppSyncWorld, model: String, step: &Step) {
    let input: Value = serde_json::from_str(step.docstring().expect("docstring")).expect("JSON");
    let engine = world.engine.as_ref().expect("engine");
    let (record, errors) = engine
        .lock()
        .unwrap()
        .call(&model, "create", &input, &Identity::ApiKey)
        .expect("engine create");
    assert!(errors.is_none(), "creating {model} failed: {errors:?}");
    assert!(!record.is_null(), "creating {model} returned null");
}

#[given(regex = r#"^user "([^"]+)" has created a (\w+) with:$"#)]
fn user_created(world: &mut AppSyncWorld, sub: String, model: String, step: &Step) {
    let input: Value = serde_json::from_str(step.docstring().expect("docstring")).expect("JSON");
    let engine = world.engine.as_ref().expect("engine");
    let identity = Identity::user(sub.clone(), sub, Vec::new());
    let (_, errors) = engine
        .lock()
        .unwrap()
        .call(&model, "create", &input, &identity)
        .expect("engine create");
    assert!(errors.is_none(), "creating {model} failed: {errors:?}");
}

// ── Requests ───────────────────────────────────────────────────────────────

async fn post_body(world: &mut AppSyncWorld, body: String, headers: Vec<(&str, HeaderValue)>) {
    let router = world.router.clone().expect("router");
    let mut request = http::Request::post("/graphql").header("content-type", "application/json");
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = router
        .oneshot(request.body(Body::from(body)).expect("request"))
        .await
        .expect("response");
    world.http_status = Some(response.status().as_u16());
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    world.response = Some(serde_json::from_slice(&bytes).expect("JSON response body"));
}

/// Send a query with the configured API key (unless the step sets its own) plus `headers`.
async fn send_query(world: &mut AppSyncWorld, body: Value, mut headers: Vec<(&str, HeaderValue)>) {
    if let Some(key) = world.api_key.clone() {
        if !headers.iter().any(|(name, _)| *name == "x-api-key") {
            headers.push(("x-api-key", HeaderValue::from_str(&key).unwrap()));
        }
    }
    post_body(world, body.to_string(), headers).await;
}

fn query_body(step: &Step) -> Value {
    json!({ "query": step.docstring().expect("docstring") })
}

#[when("I send the GraphQL request:")]
async fn send(world: &mut AppSyncWorld, step: &Step) {
    send_query(world, query_body(step), Vec::new()).await;
}

#[when(regex = r#"^I send the GraphQL request with variables (\{.*\}):$"#)]
async fn send_with_variables(world: &mut AppSyncWorld, variables: String, step: &Step) {
    let mut body = query_body(step);
    body["variables"] = serde_json::from_str(&variables).expect("variables JSON");
    send_query(world, body, Vec::new()).await;
}

#[when(regex = r#"^I send the GraphQL request with operation "([^"]+)":$"#)]
async fn send_with_operation(world: &mut AppSyncWorld, operation: String, step: &Step) {
    let mut body = query_body(step);
    body["operationName"] = json!(operation);
    send_query(world, body, Vec::new()).await;
}

#[when("I post this raw body:")]
async fn send_raw(world: &mut AppSyncWorld, step: &Step) {
    let body = step.docstring().expect("docstring").to_string();
    post_body(world, body, Vec::new()).await;
}

#[when("I send the GraphQL request using the previous nextToken:")]
async fn send_with_previous_token(world: &mut AppSyncWorld, step: &Step) {
    let token = world.next_token.clone().expect("a captured nextToken");
    let query = step
        .docstring()
        .expect("docstring")
        .replace("$nextToken", &token);
    send_query(world, json!({ "query": query }), Vec::new()).await;
}

#[when("I send a request without the x-api-key header to:")]
async fn send_without_api_key(world: &mut AppSyncWorld, step: &Step) {
    post_body(world, query_body(step).to_string(), Vec::new()).await;
}

#[when(regex = r#"^I send a request with x-api-key "([^"]*)" to:$"#)]
async fn send_with_api_key(world: &mut AppSyncWorld, key: String, step: &Step) {
    let header = ("x-api-key", HeaderValue::from_str(&key).unwrap());
    send_query(world, query_body(step), vec![header]).await;
}

#[when(regex = r"^I send a request with x-apricity-identity (.+) to:$")]
async fn send_with_identity(world: &mut AppSyncWorld, identity: String, step: &Step) {
    let header = (
        "x-apricity-identity",
        HeaderValue::from_str(&identity).unwrap(),
    );
    send_query(world, query_body(step), vec![header]).await;
}

#[when("I send a request with a non-UTF-8 x-apricity-identity to:")]
async fn send_with_non_utf8_identity(world: &mut AppSyncWorld, step: &Step) {
    let header = (
        "x-apricity-identity",
        HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
    );
    send_query(world, query_body(step), vec![header]).await;
}

// ── Responses ──────────────────────────────────────────────────────────────

fn response(world: &AppSyncWorld) -> &Value {
    world.response.as_ref().expect("a response")
}

fn assert_json_eq(actual: &Value, expected: &Value) {
    assert_eq!(
        actual,
        expected,
        "\nactual:   {}\nexpected: {}",
        serde_json::to_string(actual).unwrap(),
        serde_json::to_string(expected).unwrap()
    );
}

#[then("the GraphQL response is:")]
fn response_is(world: &mut AppSyncWorld, step: &Step) {
    let expected: Value = serde_json::from_str(step.docstring().expect("docstring")).expect("JSON");
    assert_json_eq(response(world), &expected);
}

/// Exact comparison where `$nextToken` in the expected JSON stands for the
/// (opaque) token at `path`, which must be a non-empty string. The token is
/// kept for the next request.
#[then(regex = r#"^the GraphQL response, with the nextToken at "([^"]+)", is:$"#)]
fn response_with_token_is(world: &mut AppSyncWorld, path: String, step: &Step) {
    let pointer = format!("/{}", path.replace('.', "/"));
    let token = response(world)
        .pointer(&pointer)
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .unwrap_or_else(|| panic!("no nextToken string at {path}: {}", response(world)))
        .to_string();
    let expected_text = step
        .docstring()
        .expect("docstring")
        .replace("$nextToken", &token);
    let expected: Value = serde_json::from_str(&expected_text).expect("JSON");
    assert_json_eq(response(world), &expected);
    world.next_token = Some(token);
}

#[then(regex = r"^the HTTP status is (\d+)$")]
fn http_status_is(world: &mut AppSyncWorld, status: u16) {
    assert_eq!(world.http_status, Some(status));
}

// ── Schema scenarios ───────────────────────────────────────────────────────

#[given("the blog SDL")]
fn load_blog_sdl(world: &mut AppSyncWorld) {
    world.sdl = Some(BLOG_SDL.to_string());
}

#[given("the apricity SDL")]
fn load_apricity_sdl(world: &mut AppSyncWorld) {
    world.sdl = Some(APRICITY_SDL.to_string());
}

#[given("a minimal schema with only types and no Query type")]
fn schema_without_query(world: &mut AppSyncWorld) {
    world.sdl = Some("type User {\n  id: ID!\n  name: String!\n}\n".to_string());
}

#[given("a schema with an interface type")]
fn schema_with_interface(world: &mut AppSyncWorld) {
    world.sdl = Some(
        "type Query {\n  user: User\n}\n\ninterface Node {\n  id: ID!\n}\n\ntype User implements Node {\n  id: ID!\n  name: String!\n}\n"
            .to_string(),
    );
}

#[when("I build the schema")]
fn build_schema_step(world: &mut AppSyncWorld) {
    match build_schema(world.sdl.as_deref().expect("SDL")) {
        Ok(schema) => world.schema = Some(schema),
        Err(error) => world.schema_error = Some(error.to_string()),
    }
}

#[given("this SDL:")]
fn given_sdl(world: &mut AppSyncWorld, step: &Step) {
    world.sdl = Some(step.docstring().expect("docstring").to_string());
}

#[when("I run this query against the schema:")]
async fn run_against_schema(world: &mut AppSyncWorld, step: &Step) {
    let schema = world.schema.as_ref().expect("schema");
    let response = schema.execute(step.docstring().expect("docstring")).await;
    world.response = Some(serde_json::to_value(&response).expect("response JSON"));
}

#[then("the schema builds successfully")]
fn schema_builds(world: &mut AppSyncWorld) {
    assert_eq!(world.schema_error, None);
    assert!(world.schema.is_some());
}

#[then("introspection of the schema matches the input SDL after normalization")]
fn introspection_matches_sdl(world: &mut AppSyncWorld) {
    let schema = world.schema.as_ref().expect("schema");
    let expected = normalize_sdl(world.sdl.as_deref().expect("SDL")).expect("input SDL");
    let actual = normalize_sdl(&schema.sdl()).expect("schema SDL");
    let differing: Vec<String> = expected
        .lines()
        .zip(actual.lines())
        .filter(|(e, a)| e != a)
        .take(20)
        .map(|(e, a)| format!("- {e}\n+ {a}"))
        .collect();
    assert!(
        actual == expected,
        "introspection differs from the input SDL:\n{}",
        differing.join("\n")
    );
}

/// The built schema's type definitions by name: (kind, field names).
fn schema_types(world: &AppSyncWorld) -> BTreeMap<String, (&'static str, Vec<String>)> {
    let sdl = world.schema.as_ref().expect("schema").sdl();
    let document = async_graphql_parser::parse_schema(&sdl).expect("schema SDL parses");
    let mut types = BTreeMap::new();
    for definition in document.definitions {
        if let TypeSystemDefinition::Type(type_def) = definition {
            let (kind, fields) = match &type_def.node.kind {
                TypeKind::Object(object) => (
                    "type",
                    object
                        .fields
                        .iter()
                        .map(|f| f.node.name.node.to_string())
                        .collect(),
                ),
                TypeKind::InputObject(input) => (
                    "input",
                    input
                        .fields
                        .iter()
                        .map(|f| f.node.name.node.to_string())
                        .collect(),
                ),
                TypeKind::Enum(_) => ("enum", Vec::new()),
                TypeKind::Scalar => ("scalar", Vec::new()),
                _ => ("other", Vec::new()),
            };
            types.insert(type_def.node.name.node.to_string(), (kind, fields));
        }
    }
    types
}

fn assert_schema_has(world: &AppSyncWorld, kind: &str, names: &str) {
    let types = schema_types(world);
    for name in names.split(", ") {
        assert_eq!(
            types.get(name).map(|(k, _)| *k),
            Some(kind),
            "expected {kind} {name} in the schema"
        );
    }
}

#[then(regex = r"^the schema has these types: (.+)$")]
fn schema_has_types(world: &mut AppSyncWorld, names: String) {
    assert_schema_has(world, "type", &names);
}

#[then(regex = r"^the schema has these input types: (.+)$")]
fn schema_has_input_types(world: &mut AppSyncWorld, names: String) {
    assert_schema_has(world, "input", &names);
}

#[then(regex = r"^the schema has these enum types: (.+)$")]
fn schema_has_enum_types(world: &mut AppSyncWorld, names: String) {
    assert_schema_has(world, "enum", &names);
}

#[then(regex = r"^the schema has these scalar types: (.+)$")]
fn schema_has_scalar_types(world: &mut AppSyncWorld, names: String) {
    assert_schema_has(world, "scalar", &names);
}

#[then(regex = r"^the (\w+) type has fields: (.+)$")]
fn type_has_fields(world: &mut AppSyncWorld, type_name: String, fields: String) {
    let types = schema_types(world);
    let (_, actual) = types
        .get(&type_name)
        .unwrap_or_else(|| panic!("no type {type_name}"));
    let expected: Vec<&str> = fields.split(", ").collect();
    assert_eq!(actual, &expected);
}

// ── Scalars ────────────────────────────────────────────────────────────────

#[when(regex = r#"^I parse the value "(.*)" as (\w+)$"#)]
fn parse_scalar_value(world: &mut AppSyncWorld, value: String, scalar: String) {
    world.scalar_check = Some((scalar, value));
}

fn scalar_is_valid(world: &AppSyncWorld) -> bool {
    let (scalar, value) = world.scalar_check.as_ref().expect("a parsed value");
    let validate = AwsScalarValidator::for_scalar(scalar).expect("an AWS scalar");
    validate(value).is_ok()
}

#[then("it should be valid")]
fn scalar_valid(world: &mut AppSyncWorld) {
    assert!(
        scalar_is_valid(world),
        "{:?} should be valid",
        world.scalar_check
    );
}

#[then("it should be invalid")]
fn scalar_invalid(world: &mut AppSyncWorld) {
    assert!(
        !scalar_is_valid(world),
        "{:?} should be invalid",
        world.scalar_check
    );
}

#[tokio::main]
async fn main() {
    let features = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../features/appsync");
    AppSyncWorld::cucumber()
        .fail_on_skipped()
        .run_and_exit(features)
        .await;
}
