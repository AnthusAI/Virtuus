//! HTTP GraphQL router for AppSync SDL on Virtuus storage engine.

use async_graphql::dynamic::{Field, FieldFuture, Object, Schema};
use async_graphql::ErrorExtensions;
use async_graphql_parser::parse_schema;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use virtuus_amplify::{Contract, Engine};

use crate::schema::convert_type;

/// API key authentication
#[derive(Clone)]
pub struct ApiKeyAuth {
    pub key: String,
}

/// Convert a serde_json::Value to async_graphql FieldValue
fn json_to_field_value(v: Value) -> Option<async_graphql::dynamic::FieldValue<'static>> {
    use async_graphql::dynamic::FieldValue;
    match v {
        Value::Null => None,
        Value::Object(_) => Some(FieldValue::owned_any(v)),
        Value::Array(items) => {
            let field_items: Vec<_> = items.into_iter().filter_map(json_to_field_value).collect();
            Some(FieldValue::list(field_items))
        }
        scalar => async_graphql::to_value(&scalar).ok().map(FieldValue::value),
    }
}

/// Convert engine errors to a GraphQL error with AppSync errorType
/// Takes a non-empty slice of errors (caller must check is_empty first)
fn engine_error_to_graphql_error(errors: &[Value]) -> async_graphql::Error {
    let first = &errors[0];
    let msg = first["message"].as_str().unwrap_or("error").to_string();
    let ty = first["errorType"]
        .as_str()
        .unwrap_or("InternalFailure")
        .to_string();
    async_graphql::Error::new(msg).extend_with(|_, e| e.set("errorType", ty))
}

/// Acquire engine lock, recovering from poisoning if needed.
/// Poisoning is safe here: each engine call leaves tables consistent, so
/// we can extract and use the guard from a poisoned lock.
fn acquire_engine_lock(engine: &Arc<Mutex<Engine>>) -> std::sync::MutexGuard<'_, Engine> {
    engine
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Convert engine operational errors to GraphQL errors
fn internal_failure(e: impl std::fmt::Display) -> async_graphql::Error {
    async_graphql::Error::new(format!("Engine error: {}", e))
        .extend_with(|_, ext| ext.set("errorType", "InternalFailure"))
}

/// Build engine args based on operation type and GraphQL context
fn build_engine_args(
    op: &str,
    sort_fields: Option<&[String]>,
    ctx: &async_graphql::dynamic::ResolverContext,
) -> Result<Value, async_graphql::Error> {
    let mut args = json!({});

    match op {
        "get" => {
            // Extract key fields (typically "id", or composite key fields) from top-level args
            // GraphQL type system ensures these deserialize correctly
            for (key, accessor) in ctx.args.iter() {
                if key.as_str() != "condition" {
                    args[key.as_str()] = accessor.deserialize::<Value>()?;
                }
            }
        }
        "create" | "update" | "delete" => {
            // Extract "input" argument - engine expects the full input object
            // GraphQL type system ensures this deserializes correctly
            if let Some(input_accessor) = ctx.args.get("input") {
                return input_accessor.deserialize::<Value>();
            }
        }
        "list" => {
            // Extract filter, limit, nextToken
            // GraphQL type system ensures these deserialize correctly
            if let Some(filter_accessor) = ctx.args.get("filter") {
                args["filter"] = filter_accessor.deserialize::<Value>()?;
            }
            if let Some(limit_accessor) = ctx.args.get("limit") {
                let limit_val = limit_accessor.deserialize::<i32>()?;
                if limit_val < 0 {
                    return Err(async_graphql::Error::new("limit must be non-negative")
                        .extend_with(|_, ext| ext.set("errorType", "BadRequest")));
                }
                args["limit"] = Value::Number((limit_val as u32).into());
            }
            if let Some(nexttoken_accessor) = ctx.args.get("nextToken") {
                args["nextToken"] = Value::String(nexttoken_accessor.deserialize::<String>()?);
            }
        }
        _ => {
            // Index operation: build key conditions and list args
            let mut key_obj = json!({});

            // Extract filter, sortDirection, limit, nextToken
            // GraphQL type system ensures these deserialize correctly
            if let Some(filter_accessor) = ctx.args.get("filter") {
                args["filter"] = filter_accessor.deserialize::<Value>()?;
            }
            // sortDirection is an enum - use enum_name() to read it
            if let Some(sort_accessor) = ctx.args.get("sortDirection") {
                args["sortDirection"] = Value::String(sort_accessor.enum_name()?.to_string());
            }
            if let Some(limit_accessor) = ctx.args.get("limit") {
                let limit_val = limit_accessor.deserialize::<i32>()?;
                if limit_val < 0 {
                    return Err(async_graphql::Error::new("limit must be non-negative")
                        .extend_with(|_, ext| ext.set("errorType", "BadRequest")));
                }
                args["limit"] = Value::Number((limit_val as u32).into());
            }
            if let Some(nexttoken_accessor) = ctx.args.get("nextToken") {
                args["nextToken"] = Value::String(nexttoken_accessor.deserialize::<String>()?);
            }

            // Build key conditions from remaining arguments
            // For composite sort fields, merge the operator into key
            // For single sort fields, nest the condition
            let (is_composite, composite_arg_name): (bool, Option<String>) =
                if let Some(fields) = sort_fields {
                    if fields.len() > 1 {
                        // Composite: compute camelCase argument name (e.g., "statusCreatedAt" for ["status", "createdAt"])
                        let mut arg_name = fields[0].to_string();
                        for field in &fields[1..] {
                            // Capitalize first letter of each field after the first
                            if let Some(first_char) = field.chars().next() {
                                arg_name.push_str(&format!(
                                    "{}{}",
                                    first_char.to_uppercase(),
                                    &field[1..]
                                ));
                            }
                        }
                        (true, Some(arg_name))
                    } else {
                        (false, None)
                    }
                } else {
                    (false, None)
                };

            for arg_name in ctx.args.keys() {
                let arg_str = arg_name.as_str();
                if arg_str == "filter"
                    || arg_str == "sortDirection"
                    || arg_str == "limit"
                    || arg_str == "nextToken"
                {
                    continue;
                }

                if let Some(arg_value) = ctx.args.get(arg_name) {
                    let val = arg_value.deserialize::<Value>()?;

                    if is_composite && composite_arg_name.as_ref() == Some(&arg_str.to_string()) {
                        // For composite sort fields, merge the operator and sort fields into key
                        // val is {eq: {status: "...", createdAt: "..."}} or similar
                        if let Some(obj) = val.as_object() {
                            for (op_key, op_val) in obj {
                                key_obj[op_key] = op_val.clone();
                            }
                        }
                    } else {
                        // For single sort fields or partition fields, nest the condition
                        key_obj[arg_str] = val;
                    }
                }
            }

            if !key_obj.is_null() && key_obj.as_object().is_some_and(|o| !o.is_empty()) {
                args["key"] = key_obj;
            }
        }
    }

    Ok(args)
}

#[derive(Clone)]
struct AppSyncState {
    schema: Schema,
    #[allow(dead_code)]
    engine: Arc<Mutex<Engine>>,
    auth: Option<ApiKeyAuth>,
}

/// Build an HTTP router serving `/graphql` with AppSync SDL and Engine-backed resolvers.
pub fn router(
    engine: Arc<Mutex<Engine>>,
    sdl: &str,
    contract: &Contract,
    auth: Option<ApiKeyAuth>,
) -> Result<Router, crate::schema::SchemaBuildError> {
    let document = parse_schema(sdl)
        .map_err(|e| crate::schema::SchemaBuildError::ParseError(e.to_string()))?;

    // Build field binding map from contract: field_name → (model_name, operation)
    // Initial bindings for CRUD and index operations
    let mut field_bindings: BTreeMap<String, (String, String)> = BTreeMap::new();

    // Get models from contract and build bindings
    for (model_name, model) in contract.models() {
        // CRUD operations: get, create, update, delete
        field_bindings.insert(
            format!("get{}", model_name),
            (model_name.clone(), "get".to_string()),
        );
        field_bindings.insert(
            format!("create{}", model_name),
            (model_name.clone(), "create".to_string()),
        );
        field_bindings.insert(
            format!("update{}", model_name),
            (model_name.clone(), "update".to_string()),
        );
        field_bindings.insert(
            format!("delete{}", model_name),
            (model_name.clone(), "delete".to_string()),
        );

        // Index operations - get queryField from index definitions
        for index in model.indexes() {
            field_bindings.insert(
                index.query_field().to_string(),
                (model_name.clone(), index.query_field().to_string()),
            );
        }
    }

    // Ensure all AWS scalars are registered
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
    let mut scalars: BTreeMap<String, async_graphql::dynamic::Scalar> = BTreeMap::new();
    for aws_scalar in aws_scalars {
        scalars.insert(
            aws_scalar.to_string(),
            async_graphql::dynamic::Scalar::new(aws_scalar),
        );
    }

    let mut query_type: Option<Object> = None;
    let mut mutation_type: Option<Object> = None;
    let mut other_types: BTreeMap<String, Object> = BTreeMap::new();
    let mut input_types: BTreeMap<String, async_graphql::dynamic::InputObject> = BTreeMap::new();
    let mut enums: BTreeMap<String, async_graphql::dynamic::Enum> = BTreeMap::new();

    // Detect list operations by checking Query field return types (only if not already bound as index)
    for definition in &document.definitions {
        if let async_graphql_parser::types::TypeSystemDefinition::Type(type_def) = definition {
            let type_def = &type_def.node;
            if type_def.name.to_string() == "Query" {
                if let async_graphql_parser::types::TypeKind::Object(obj_type) = &type_def.kind {
                    for field in &obj_type.fields {
                        let field_name = field.node.name.to_string();
                        // Skip if already bound as an index operation
                        if field_bindings.contains_key(&field_name) {
                            continue;
                        }

                        // Get the base type name from the field's type by converting to string
                        let return_type_str = field.node.ty.node.to_string();
                        // Extract the base type name (without ! or [])
                        let base_type = return_type_str
                            .trim_end_matches('!')
                            .trim_end_matches(']')
                            .split('[')
                            .next()
                            .unwrap_or("");

                        // Check if return type ends with "Connection"
                        if base_type.starts_with("Model") && base_type.ends_with("Connection") {
                            // Extract model name: "ModelPostConnection" -> "Post"
                            if base_type.len() > 15 {
                                // "Model" (5) + ModelName + "Connection" (10)
                                let model_name = base_type[5..base_type.len() - 10].to_string();
                                if !model_name.is_empty() {
                                    field_bindings.insert(
                                        field_name.clone(),
                                        (model_name.clone(), "list".to_string()),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let engine_clone = engine.clone();
    let field_bindings_clone = field_bindings.clone();

    // Build a map of index sort fields from the contract
    // Key: (model_name, operation) -> Value: Vec<String> (sort field names)
    let mut index_sort_fields: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for (model_name, model) in contract.models() {
        for index in model.indexes() {
            let sort_fields: Vec<String> =
                index.sort_fields().iter().map(|s| s.to_string()).collect();
            index_sort_fields.insert(
                (model_name.to_string(), index.query_field().to_string()),
                sort_fields,
            );
        }
    }
    let index_sort_fields_clone = index_sort_fields.clone();

    // Process all type definitions
    for definition in &document.definitions {
        if let async_graphql_parser::types::TypeSystemDefinition::Type(type_def) = definition {
            let type_def = &type_def.node;
            let type_name = type_def.name.to_string();

            match &type_def.kind {
                async_graphql_parser::types::TypeKind::Object(obj_type) => {
                    let mut obj = Object::new(&type_name);

                    // Add all fields
                    for field in &obj_type.fields {
                        let field_name = field.node.name.to_string();
                        let field_type = convert_type(&field.node.ty.node);

                        // Check if this field is engine-bound using the contract bindings
                        let field_def = if let Some((model_name, operation)) =
                            field_bindings_clone.get(&field_name)
                        {
                            let engine_for_field = engine_clone.clone();
                            let model = model_name.clone();
                            let op = operation.clone();
                            let sort_fields = index_sort_fields_clone
                                .get(&(model_name.clone(), operation.clone()))
                                .cloned();

                            Field::new(field_name, field_type, move |ctx| {
                                let engine = engine_for_field.clone();
                                let model = model.clone();
                                let op = op.clone();
                                let sort_fields = sort_fields.clone();

                                FieldFuture::new(async move {
                                    // Determine operation type
                                    let is_list_op = op == "list";
                                    let is_index_op = op != "get"
                                        && op != "create"
                                        && op != "update"
                                        && op != "delete"
                                        && op != "list";

                                    // Build engine args based on operation type
                                    let args =
                                        build_engine_args(&op, sort_fields.as_deref(), &ctx)?;

                                    // Scope the lock tightly - drop before returning FieldValue
                                    let (result, errors) = {
                                        let mut eng = acquire_engine_lock(&engine);
                                        eng.call(&model, &op, &args)
                                    }
                                    .map_err(internal_failure)?;
                                    // Lock is now dropped

                                    // Return error if engine reported any errors
                                    if let Some(err_vec) = errors {
                                        if !err_vec.is_empty() {
                                            return Err(engine_error_to_graphql_error(&err_vec));
                                        }
                                    }

                                    // No errors - return result based on operation type
                                    if is_list_op || is_index_op {
                                        Ok(json_to_field_value(result))
                                    } else if result.is_null() {
                                        Ok(None)
                                    } else {
                                        Ok(json_to_field_value(result))
                                    }
                                })
                            })
                        } else {
                            // Generic field resolver: read field value from parent object
                            let name = field_name.clone();
                            Field::new(field_name, field_type, move |ctx| {
                                let name = name.clone();
                                FieldFuture::new(async move {
                                    let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
                                    let value = parent.get(&name).cloned().unwrap_or(Value::Null);
                                    Ok(json_to_field_value(value))
                                })
                            })
                        };

                        let mut field_with_args = field_def;

                        // Add arguments
                        for arg in &field.node.arguments {
                            let arg_name = arg.node.name.to_string();
                            let arg_type = convert_type(&arg.node.ty.node);
                            field_with_args = field_with_args.argument(
                                async_graphql::dynamic::InputValue::new(arg_name, arg_type),
                            );
                        }

                        obj = obj.field(field_with_args);
                    }

                    if type_name == "Query" {
                        query_type = Some(obj);
                    } else if type_name == "Mutation" {
                        mutation_type = Some(obj);
                    } else {
                        other_types.insert(type_name, obj);
                    }
                }
                async_graphql_parser::types::TypeKind::InputObject(input_type) => {
                    let mut input = async_graphql::dynamic::InputObject::new(&type_name);
                    for field in &input_type.fields {
                        let field_name = field.node.name.to_string();
                        let field_type = convert_type(&field.node.ty.node);
                        input = input.field(async_graphql::dynamic::InputValue::new(
                            field_name, field_type,
                        ));
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
                async_graphql_parser::types::TypeKind::Scalar
                    if !scalars.contains_key(&type_name) =>
                {
                    scalars.insert(
                        type_name.clone(),
                        async_graphql::dynamic::Scalar::new(&type_name),
                    );
                }
                _ => {}
            }
        }
    }

    // Ensure Query type exists
    let query = query_type.ok_or(crate::schema::SchemaBuildError::MissingQuery)?;

    // Mutation type is optional
    let mutation = mutation_type;

    let mutation_name = mutation.as_ref().map(|_| "Mutation");
    let mut schema_builder = Schema::build("Query", mutation_name, None);

    // Register types in order
    for (_, scalar) in scalars {
        schema_builder = schema_builder.register(scalar);
    }
    for (_, enum_def) in enums {
        schema_builder = schema_builder.register(enum_def);
    }
    for (_, input) in input_types {
        schema_builder = schema_builder.register(input);
    }
    for (_, obj) in other_types {
        schema_builder = schema_builder.register(obj);
    }

    schema_builder = schema_builder.register(query);

    if let Some(mutation) = mutation {
        schema_builder = schema_builder.register(mutation);
    }

    let schema = schema_builder
        .finish()
        .map_err(|e| crate::schema::SchemaBuildError::BuildError(e.to_string()))?;

    let state = AppSyncState {
        schema,
        engine,
        auth,
    };

    Ok(Router::new()
        .route("/graphql", post(graphql_handler))
        .with_state(state))
}

async fn graphql_handler(
    State(state): State<AppSyncState>,
    headers: HeaderMap,
    body: String,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Check API key auth if configured
    if let Some(auth) = &state.auth {
        let api_key = headers.get("x-api-key").and_then(|v| v.to_str().ok());

        if api_key != Some(&auth.key) {
            let response = json!({
                "data": null,
                "errors": [{
                    "message": "Unauthorized",
                    "errorType": "UnauthorizedException"
                }]
            });
            return Err((
                StatusCode::UNAUTHORIZED,
                serde_json::to_string(&response).unwrap(),
            ));
        }
    }

    let req: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let query = req
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing query".to_string()))?;

    let result = state.schema.execute(query).await;

    let mut response = json!({
        "data": result.data
    });

    if !result.errors.is_empty() {
        let errors: Vec<Value> = result
            .errors
            .iter()
            .map(|e| {
                let mut error = json!({
                    "message": e.message,
                    "errorType": "InternalFailure"
                });

                // Extract errorType from extensions if present
                if let Some(extensions) = &e.extensions {
                    if let Some(error_type_val) = extensions.get("errorType") {
                        // Convert the async_graphql::Value to string representation
                        let et_str = format!("{}", error_type_val);
                        // Remove surrounding quotes if present (async_graphql may add them)
                        let et_str = et_str.trim_matches('"').to_string();
                        if !et_str.is_empty() {
                            error["errorType"] = Value::String(et_str);
                        }
                    }
                }

                // Add path and locations if present
                if !e.path.is_empty() {
                    error["path"] = serde_json::to_value(&e.path).unwrap_or(Value::Null);
                }
                if !e.locations.is_empty() {
                    let locations: Vec<Value> = e
                        .locations
                        .iter()
                        .map(|loc| {
                            json!({
                                "line": loc.line,
                                "column": loc.column,
                            })
                        })
                        .collect();
                    error["locations"] = Value::Array(locations);
                }

                error
            })
            .collect();

        response["errors"] = Value::Array(errors);
    }

    Ok(Json(response))
}
