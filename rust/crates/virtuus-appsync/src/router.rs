//! HTTP GraphQL router for AppSync SDL on the Virtuus storage engine.
//!
//! Root Query/Mutation fields are bound to engine operations from the contract:
//! `get<Model>`, `create<Model>`, `update<Model>`, `delete<Model>`, every index
//! `queryField`, and each Query field returning `Model<Model>Connection` (list).
//! Relationship fields resolve through the target model: `hasMany` queries the
//! child index, `belongsTo` gets the parent record. Every other field reads the
//! value of the same name from its parent record.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, ResolverContext, Schema, TypeRef, ValueAccessor,
};
use async_graphql::ErrorExtensions;
use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    routing::post,
    Json, Router,
};
use serde_json::{json, Map, Value};
use virtuus_amplify::{Contract, Engine, Identity};

use crate::schema::{build_schema_with, SchemaBuildError};

/// Router configuration options.
#[derive(Clone, Debug, Default)]
pub struct RouterOptions {
    /// When set, every request must carry this key in `x-api-key`.
    pub api_key: Option<String>,
    /// Honour the `x-apricity-identity` header (a JSON `{sub, username, groups}`)
    /// so tests can act as a signed-in user. Never enable this in a server
    /// reachable from outside.
    pub test_identities: bool,
}

/// An HTTP rejection: the status and an AppSync-shaped error body.
type Rejection = (StatusCode, Json<Value>);

/// The list arguments shared by list fields, index queries and hasMany fields.
const LIST_ARGUMENTS: [&str; 4] = ["filter", "sortDirection", "limit", "nextToken"];

/// A root field's engine operation.
#[derive(Clone)]
enum Operation {
    Get,
    Mutation(&'static str),
    List,
    /// An index query; `composite` names the argument holding a composite
    /// sort-key condition (e.g. `statusCreatedAt`), whose operator merges into the key.
    Index {
        query_field: String,
        composite: Option<String>,
    },
}

/// A relationship field's resolution.
#[derive(Clone)]
enum Relation {
    HasMany {
        target: String,
        index: String,
        references: String,
        parent_key: String,
    },
    BelongsTo {
        target: String,
        references: String,
        target_key: String,
    },
}

#[derive(Clone)]
struct AppSyncState {
    schema: Schema,
    options: RouterOptions,
}

/// Build an HTTP router serving `POST /graphql` for the AppSync SDL, with
/// resolvers bound to the engine through the contract.
pub fn router(
    engine: Arc<Mutex<Engine>>,
    sdl: &str,
    contract: &Contract,
    options: RouterOptions,
) -> Result<Router, SchemaBuildError> {
    let operations = root_operations(contract);
    let relations = relations(contract)?;
    let schema = build_schema_with(sdl, &mut |type_name, field_name, field_type| {
        let root = type_name == "Query" || type_name == "Mutation";
        let operation = operations
            .get(field_name)
            .cloned()
            .or_else(|| list_operation(type_name, &field_type, contract));
        match (root, operation, relations.get(&(type_name, field_name))) {
            (true, Some((model, operation)), _) => {
                root_field(&engine, model, operation, field_name, field_type)
            }
            (false, _, Some(relation)) => {
                relation_field(&engine, relation.clone(), field_name, field_type)
            }
            _ => parent_field(field_name, field_type),
        }
    })?;
    Ok(Router::new()
        .route("/graphql", post(graphql_handler))
        .with_state(AppSyncState { schema, options }))
}

/// Root field name → (model, operation) for each contract model's
/// get/create/update/delete and index query fields.
fn root_operations(contract: &Contract) -> BTreeMap<String, (String, Operation)> {
    let mut operations = BTreeMap::new();
    for (model_name, model) in contract.models() {
        let bind = |operation| (model_name.clone(), operation);
        operations.insert(format!("get{model_name}"), bind(Operation::Get));
        for verb in ["create", "update", "delete"] {
            operations.insert(
                format!("{verb}{model_name}"),
                bind(Operation::Mutation(verb)),
            );
        }
        for index in model.indexes() {
            let sort_fields = index.sort_fields();
            let composite = (sort_fields.len() > 1).then(|| composite_argument(sort_fields));
            operations.insert(
                index.query_field().to_string(),
                bind(Operation::Index {
                    query_field: index.query_field().to_string(),
                    composite,
                }),
            );
        }
    }
    operations
}

/// A Query field returning `Model<X>Connection` for a contract model X lists X.
fn list_operation(
    type_name: &str,
    field_type: &TypeRef,
    contract: &Contract,
) -> Option<(String, Operation)> {
    let return_type = field_type.to_string();
    let model = return_type
        .strip_prefix("Model")?
        .strip_suffix("Connection")?;
    (type_name == "Query" && contract.models().contains_key(model))
        .then(|| (model.to_string(), Operation::List))
}

/// The GraphQL argument name for a composite sort key: `["status", "createdAt"]`
/// → `statusCreatedAt`.
fn composite_argument(sort_fields: &[String]) -> String {
    let mut name = sort_fields[0].clone();
    for field in &sort_fields[1..] {
        let mut chars = field.chars();
        name.extend(chars.next().map(|c| c.to_ascii_uppercase()));
        name.push_str(chars.as_str());
    }
    name
}

/// (model, relationship field) → how to resolve it.
fn relations(contract: &Contract) -> Result<BTreeMap<(&str, &str), Relation>, SchemaBuildError> {
    let models = contract.models();
    let mut relations = BTreeMap::new();
    for (model_name, model) in models {
        for rel in model.relationships() {
            let relation = match (rel.kind(), rel.child_index()) {
                ("hasMany", Some(index)) => Relation::HasMany {
                    target: rel.target().to_string(),
                    index: index.to_string(),
                    references: rel.references().to_string(),
                    parent_key: model.primary_key()[0].clone(),
                },
                ("belongsTo", _) => Relation::BelongsTo {
                    target: rel.target().to_string(),
                    references: rel.references().to_string(),
                    target_key: models[rel.target()].primary_key()[0].clone(),
                },
                (kind, _) => {
                    return Err(SchemaBuildError::BuildError(format!(
                        "{model_name}.{}: {kind} relationships are not supported",
                        rel.field()
                    )))
                }
            };
            relations.insert((model_name.as_str(), rel.field()), relation);
        }
    }
    Ok(relations)
}

/// A root field that calls the engine.
fn root_field(
    engine: &Arc<Mutex<Engine>>,
    model: String,
    operation: Operation,
    name: &str,
    ty: TypeRef,
) -> Field {
    let engine = engine.clone();
    Field::new(name, ty, move |ctx| {
        let result = resolve_root(&ctx, &engine, &model, &operation);
        FieldFuture::Value(field_result(&ctx, result))
    })
}

fn resolve_root(
    ctx: &ResolverContext,
    engine: &Mutex<Engine>,
    model: &str,
    operation: &Operation,
) -> async_graphql::Result<Value> {
    let (op, args) = match operation {
        Operation::Get => ("get", Value::Object(arguments(ctx, |_| true)?)),
        Operation::Mutation(verb) => (*verb, mutation_input(ctx)?),
        Operation::List => ("list", Value::Object(list_arguments(ctx)?)),
        Operation::Index {
            query_field,
            composite,
        } => (
            query_field.as_str(),
            index_arguments(ctx, composite.as_deref())?,
        ),
    };
    call_engine(engine, model, op, &args, ctx.data::<Identity>()?)
}

/// A relationship field resolved through the target model.
fn relation_field(
    engine: &Arc<Mutex<Engine>>,
    relation: Relation,
    name: &str,
    ty: TypeRef,
) -> Field {
    let engine = engine.clone();
    Field::new(name, ty, move |ctx| {
        let result = resolve_relation(&ctx, &engine, &relation);
        FieldFuture::Value(field_result(&ctx, result))
    })
}

fn resolve_relation(
    ctx: &ResolverContext,
    engine: &Mutex<Engine>,
    relation: &Relation,
) -> async_graphql::Result<Value> {
    let identity = ctx.data::<Identity>()?;
    let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
    match relation {
        Relation::HasMany {
            target,
            index,
            references,
            parent_key,
        } => {
            let mut args = list_arguments(ctx)?;
            args.insert("key".into(), json!({ references: parent[parent_key] }));
            call_engine(engine, target, index, &Value::Object(args), identity)
        }
        Relation::BelongsTo {
            target,
            references,
            target_key,
        } => match &parent[references] {
            Value::Null => Ok(Value::Null),
            key => call_engine(engine, target, "get", &json!({ target_key: key }), identity),
        },
    }
}

/// Resolver errors become AppSync field errors: the field resolves to null and
/// the error, with its path, joins the response (AppSync's partial results).
/// async-graphql's dynamic schema would otherwise fail the whole response.
fn field_result(
    ctx: &ResolverContext,
    result: async_graphql::Result<Value>,
) -> Option<FieldValue<'static>> {
    match result {
        Ok(value) => json_to_field_value(value),
        Err(error) => {
            let error_type = error
                .extensions
                .as_ref()
                .and_then(|ext| ext.get("errorType"))
                .map_or(json!("InternalFailure"), |t| json!(t));
            let entry = json!({
                "message": error.message,
                "errorType": error_type,
                "path": ctx.ctx.path_node,
                "locations": [ctx.ctx.item.pos],
            });
            let sink = &ctx.ctx.data_unchecked::<FieldErrors>().0;
            sink.lock().unwrap().push(entry);
            None
        }
    }
}

/// The field errors of one request.
#[derive(Clone, Default)]
struct FieldErrors(Arc<Mutex<Vec<Value>>>);

/// A field that reads the value of the same name from its parent record.
fn parent_field(name: &str, ty: TypeRef) -> Field {
    let name = name.to_string();
    Field::new(name.clone(), ty, move |ctx| {
        let name = name.clone();
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
            Ok(json_to_field_value(parent[&name].clone()))
        })
    })
}

/// The field's arguments whose name passes `keep`; explicit nulls are dropped.
fn arguments(
    ctx: &ResolverContext,
    keep: impl Fn(&str) -> bool,
) -> async_graphql::Result<Map<String, Value>> {
    let mut args = Map::new();
    for (name, value) in ctx.args.iter() {
        if keep(name.as_str()) && !value.is_null() {
            args.insert(name.to_string(), argument_json(name.as_str(), value)?);
        }
    }
    Ok(args)
}

/// One argument as JSON. `sortDirection` is an enum; `limit` must not be negative.
fn argument_json(name: &str, value: ValueAccessor) -> async_graphql::Result<Value> {
    match name {
        "sortDirection" => Ok(json!(value.enum_name()?)),
        "limit" if value.i64()? < 0 => Err(bad_request("limit must be non-negative")),
        _ => Ok(value.deserialize::<Value>()?),
    }
}

/// filter / sortDirection / limit / nextToken.
fn list_arguments(ctx: &ResolverContext) -> async_graphql::Result<Map<String, Value>> {
    arguments(ctx, |name| LIST_ARGUMENTS.contains(&name))
}

/// An index query's arguments: the list arguments plus a `key` built from the
/// partition argument and the sort-key condition.
fn index_arguments(ctx: &ResolverContext, composite: Option<&str>) -> async_graphql::Result<Value> {
    let mut args = list_arguments(ctx)?;
    let mut key = Map::new();
    for (name, value) in arguments(ctx, |name| !LIST_ARGUMENTS.contains(&name))? {
        match (composite == Some(name.as_str()), value) {
            (true, Value::Object(operator)) => key.extend(operator),
            (_, value) => {
                key.insert(name, value);
            }
        }
    }
    args.insert("key".into(), Value::Object(key));
    Ok(Value::Object(args))
}

/// A mutation's `input`. Conditional writes are not supported, so a
/// `condition` argument is rejected rather than silently ignored.
fn mutation_input(ctx: &ResolverContext) -> async_graphql::Result<Value> {
    if ctx.args.get("condition").is_some_and(|c| !c.is_null()) {
        return Err(bad_request("condition expressions are not supported"));
    }
    ctx.args.try_get("input")?.deserialize::<Value>()
}

/// Call the engine; its first reported error becomes the GraphQL error.
fn call_engine(
    engine: &Mutex<Engine>,
    model: &str,
    op: &str,
    args: &Value,
    identity: &Identity,
) -> async_graphql::Result<Value> {
    // A poisoned lock is safe to reuse: each engine call leaves the tables consistent.
    let (result, errors) = engine
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .call(model, op, args, identity)?;
    match errors.as_deref() {
        Some([first, ..]) => Err(async_graphql::Error::new(
            first["message"].as_str().unwrap_or_default(),
        )
        .extend_with(|_, ext| {
            ext.set("errorType", first["errorType"].as_str().unwrap_or_default())
        })),
        _ => Ok(result),
    }
}

fn bad_request(message: &str) -> async_graphql::Error {
    async_graphql::Error::new(message).extend_with(|_, ext| ext.set("errorType", "BadRequest"))
}

/// Convert an engine JSON value to a GraphQL field value. Objects stay JSON so
/// their fields resolve from them.
fn json_to_field_value(value: Value) -> Option<FieldValue<'static>> {
    match value {
        Value::Null => None,
        Value::Object(_) => Some(FieldValue::owned_any(value)),
        Value::Array(items) => {
            Some(FieldValue::list(items.into_iter().map(|item| {
                json_to_field_value(item).unwrap_or(FieldValue::NULL)
            })))
        }
        scalar => Some(FieldValue::value(
            async_graphql::Value::from_json(scalar).unwrap_or_default(),
        )),
    }
}

async fn graphql_handler(
    State(state): State<AppSyncState>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<Value>, Rejection> {
    let identity = identity_from_headers(&headers, &state.options)?;
    let request: async_graphql::Request = serde_json::from_str(&body).map_err(|e| {
        rejection(
            StatusCode::BAD_REQUEST,
            "MalformedHttpRequestException",
            &e.to_string(),
        )
    })?;
    if request.query.trim().is_empty() {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "MalformedHttpRequestException",
            "The request has no query",
        ));
    }
    let field_errors = FieldErrors::default();
    let request = request.data(identity).data(field_errors.clone());
    let response = state.schema.execute(request).await;
    // Request errors (parse, validation) carry no errorType, as in AppSync.
    let mut errors: Vec<Value> = response
        .errors
        .iter()
        .map(|error| json!({ "message": error.message, "locations": error.locations }))
        .collect();
    errors.append(&mut field_errors.0.lock().unwrap());
    let mut body = json!({ "data": response.data });
    if !errors.is_empty() {
        body["errors"] = Value::Array(errors);
    }
    Ok(Json(body))
}

/// The caller's identity. `x-apricity-identity` (only with `test_identities`)
/// wins over `x-api-key`; a configured API key must match exactly.
fn identity_from_headers(
    headers: &HeaderMap,
    options: &RouterOptions,
) -> Result<Identity, Rejection> {
    if let Some(value) = headers.get("x-apricity-identity") {
        if !options.test_identities {
            return Err(unauthorized("Test identity headers not enabled"));
        }
        return test_identity(value);
    }
    match (&options.api_key, headers.get("x-api-key")) {
        (None, _) => Ok(Identity::ApiKey),
        (Some(expected), Some(given)) if given.as_bytes() == expected.as_bytes() => {
            Ok(Identity::ApiKey)
        }
        (Some(_), _) => Err(unauthorized("Unauthorized")),
    }
}

/// Parse `{"sub", "username", "groups"?}` from the test identity header.
fn test_identity(value: &HeaderValue) -> Result<Identity, Rejection> {
    let text = value
        .to_str()
        .map_err(|_| unauthorized("Invalid identity header encoding"))?;
    let json: Value =
        serde_json::from_str(text).map_err(|_| unauthorized("Invalid identity header JSON"))?;
    let object = json
        .as_object()
        .ok_or_else(|| unauthorized("Identity header must be a JSON object"))?;
    let required = |name: &str| {
        object.get(name).and_then(Value::as_str).ok_or_else(|| {
            unauthorized(&format!("Identity header missing required '{name}' field"))
        })
    };
    let (sub, username) = (required("sub")?, required("username")?);
    let groups = object
        .get("groups")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    Ok(Identity::user(sub, username, groups))
}

fn unauthorized(message: &str) -> Rejection {
    rejection(StatusCode::UNAUTHORIZED, "UnauthorizedException", message)
}

fn rejection(status: StatusCode, error_type: &str, message: &str) -> Rejection {
    let body = json!({
        "data": null,
        "errors": [{ "message": message, "errorType": error_type }],
    });
    (status, Json(body))
}
