//! HTTP GraphQL router for AppSync SDL on Virtuus storage engine.

use async_graphql::dynamic::{Field, FieldFuture, Object, Schema, TypeRef};
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
fn engine_error_to_graphql_error(errors: Option<Vec<Value>>) -> Option<async_graphql::Error> {
    errors.as_ref().and_then(|errs| {
        if errs.is_empty() {
            None
        } else {
            let first = &errs[0];
            let msg = first["message"].as_str().unwrap_or("error").to_string();
            let ty = first["errorType"]
                .as_str()
                .unwrap_or("InternalFailure")
                .to_string();
            Some(async_graphql::Error::new(msg).extend_with(|_, e| e.set("errorType", ty)))
        }
    })
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
    _contract: &Contract,
    auth: Option<ApiKeyAuth>,
) -> Result<Router, crate::schema::SchemaBuildError> {
    let document = parse_schema(sdl)
        .map_err(|e| crate::schema::SchemaBuildError::ParseError(e.to_string()))?;

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

    let engine_clone = engine.clone();

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

                        // For Query.get<Model>, Query.list<Model>, Query.index<Model> and Mutation.create/update/delete<Model> fields, bind to engine
                        let field_def = if (type_name == "Query"
                            && field_name.starts_with("list")
                            && field_name.len() > 4)
                            || (type_name == "Query"
                                && field_name.starts_with("index")
                                && field_name.len() > 5)
                        {
                            let model_name = if let Some(stripped) = field_name.strip_prefix("list")
                            {
                                stripped.to_string()
                            } else if let Some(stripped) = field_name.strip_prefix("index") {
                                stripped.to_string()
                            } else {
                                unreachable!()
                            };
                            let engine_for_field = engine_clone.clone();

                            Field::new(field_name, field_type, move |ctx| {
                                let engine = engine_for_field.clone();
                                let model = model_name.clone();

                                FieldFuture::new(async move {
                                    // Build filter/pagination args from context
                                    let mut args = json!({});

                                    // Extract filter if present
                                    if let Some(filter_accessor) = ctx.args.get("filter") {
                                        if let Ok(filter_val) =
                                            filter_accessor.deserialize::<Value>()
                                        {
                                            args["filter"] = filter_val;
                                        }
                                    }

                                    // Extract pagination args if present
                                    if let Some(limit_accessor) = ctx.args.get("limit") {
                                        if let Ok(limit_val) = limit_accessor.deserialize::<u32>() {
                                            args["limit"] = Value::Number(limit_val.into());
                                        }
                                    }

                                    if let Some(nexttoken_accessor) = ctx.args.get("nextToken") {
                                        if let Ok(nexttoken_val) =
                                            nexttoken_accessor.deserialize::<String>()
                                        {
                                            args["nextToken"] = Value::String(nexttoken_val);
                                        }
                                    }

                                    let mut eng = match engine.lock() {
                                        Ok(e) => e,
                                        Err(_) => return Ok(None),
                                    };

                                    match eng.call(&model, "list", &args) {
                                        Ok((records, errors)) => {
                                            if let Some(err) = engine_error_to_graphql_error(errors)
                                            {
                                                Err(err)
                                            } else if records.is_null() || !records.is_array() {
                                                Ok(None)
                                            } else {
                                                Ok(json_to_field_value(records))
                                            }
                                        }
                                        Err(_) => Ok(None),
                                    }
                                })
                            })
                        } else if type_name == "Query"
                            && field_name.starts_with("get")
                            && field_name.len() > 3
                        {
                            let model_name = field_name[3..].to_string();
                            let engine_for_field = engine_clone.clone();

                            Field::new(field_name, field_type, move |ctx| {
                                let engine = engine_for_field.clone();
                                let model = model_name.clone();

                                FieldFuture::new(async move {
                                    // Get the id argument
                                    let id = ctx
                                        .args
                                        .get("id")
                                        .and_then(|v| v.string().ok())
                                        .map(|s| s.to_string());

                                    let mut args = json!({});
                                    if let Some(id_val) = id {
                                        args["id"] = Value::String(id_val);
                                    }

                                    let mut eng = match engine.lock() {
                                        Ok(e) => e,
                                        Err(_) => return Ok(None),
                                    };

                                    match eng.call(&model, "get", &args) {
                                        Ok((record, errors)) => {
                                            if let Some(err) = engine_error_to_graphql_error(errors)
                                            {
                                                Err(err)
                                            } else if record.is_null() {
                                                Ok(None)
                                            } else {
                                                Ok(json_to_field_value(record))
                                            }
                                        }
                                        Err(_) => Ok(None),
                                    }
                                })
                            })
                        } else if type_name == "Mutation"
                            && field_name.starts_with("create")
                            && field_name.len() > 6
                        {
                            let model_name = field_name[6..].to_string();
                            let engine_for_field = engine_clone.clone();

                            Field::new(field_name, field_type, move |ctx| {
                                let engine = engine_for_field.clone();
                                let model = model_name.clone();

                                FieldFuture::new(async move {
                                    // Extract the input argument - try to deserialize from ValueAccessor
                                    let input = if let Some(input_accessor) = ctx.args.get("input")
                                    {
                                        // Try to deserialize as a Value using serde
                                        match input_accessor.deserialize::<Value>() {
                                            Ok(v) => v,
                                            Err(_) => Value::Null,
                                        }
                                    } else {
                                        Value::Null
                                    };

                                    let mut eng = match engine.lock() {
                                        Ok(e) => e,
                                        Err(_) => return Ok(None),
                                    };

                                    match eng.call(&model, "create", &input) {
                                        Ok((record, errors)) => {
                                            if let Some(err) = engine_error_to_graphql_error(errors)
                                            {
                                                Err(err)
                                            } else if record.is_null() {
                                                Ok(None)
                                            } else {
                                                Ok(json_to_field_value(record))
                                            }
                                        }
                                        Err(_) => Ok(None),
                                    }
                                })
                            })
                        } else if type_name == "Mutation"
                            && field_name.starts_with("update")
                            && field_name.len() > 6
                        {
                            let model_name = field_name[6..].to_string();
                            let engine_for_field = engine_clone.clone();

                            Field::new(field_name, field_type, move |ctx| {
                                let engine = engine_for_field.clone();
                                let model = model_name.clone();

                                FieldFuture::new(async move {
                                    // Extract the input argument
                                    let input = if let Some(input_accessor) = ctx.args.get("input")
                                    {
                                        match input_accessor.deserialize::<Value>() {
                                            Ok(v) => v,
                                            Err(_) => Value::Null,
                                        }
                                    } else {
                                        Value::Null
                                    };

                                    let mut eng = match engine.lock() {
                                        Ok(e) => e,
                                        Err(_) => return Ok(None),
                                    };

                                    match eng.call(&model, "update", &input) {
                                        Ok((record, errors)) => {
                                            if let Some(err) = engine_error_to_graphql_error(errors)
                                            {
                                                Err(err)
                                            } else if record.is_null() {
                                                Ok(None)
                                            } else {
                                                Ok(json_to_field_value(record))
                                            }
                                        }
                                        Err(_) => Ok(None),
                                    }
                                })
                            })
                        } else if type_name == "Mutation"
                            && field_name.starts_with("delete")
                            && field_name.len() > 6
                        {
                            let model_name = field_name[6..].to_string();
                            let engine_for_field = engine_clone.clone();

                            Field::new(field_name, field_type, move |ctx| {
                                let engine = engine_for_field.clone();
                                let model = model_name.clone();

                                FieldFuture::new(async move {
                                    // Extract the input argument
                                    let input = if let Some(input_accessor) = ctx.args.get("input")
                                    {
                                        match input_accessor.deserialize::<Value>() {
                                            Ok(v) => v,
                                            Err(_) => Value::Null,
                                        }
                                    } else {
                                        Value::Null
                                    };

                                    let mut eng = match engine.lock() {
                                        Ok(e) => e,
                                        Err(_) => return Ok(None),
                                    };

                                    match eng.call(&model, "delete", &input) {
                                        Ok((record, errors)) => {
                                            if let Some(err) = engine_error_to_graphql_error(errors)
                                            {
                                                Err(err)
                                            } else if record.is_null() {
                                                Ok(None)
                                            } else {
                                                Ok(json_to_field_value(record))
                                            }
                                        }
                                        Err(_) => Ok(None),
                                    }
                                })
                            })
                        } else {
                            // Generic field resolver: read field value from parent object
                            let name = field_name.clone();
                            Field::new(field_name, field_type, move |ctx| {
                                let name = name.clone();
                                FieldFuture::new(async move {
                                    if let Ok(parent) = ctx.parent_value.try_downcast_ref::<Value>()
                                    {
                                        let value =
                                            parent.get(&name).cloned().unwrap_or(Value::Null);
                                        Ok(json_to_field_value(value))
                                    } else {
                                        Ok(None)
                                    }
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
    let query = query_type.unwrap_or_else(|| {
        Object::new("Query").field(Field::new("_empty", TypeRef::named("String"), |_| {
            FieldFuture::new(async { Ok(None::<async_graphql::Value>) })
        }))
    });

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
