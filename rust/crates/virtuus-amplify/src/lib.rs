//! Virtuus amplify: Amplify Gen2-shaped operations on the Virtuus storage engine.
//!
//! V8b populates composite sort attributes on write with v1#v2 format.

use base64::engine::general_purpose;
use base64::Engine as _;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;
use virtuus::table::Table;
use virtuus::SortCondition;

/// A typed authorization operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthOp {
    Create,
    Read,
    Update,
    Delete,
}

/// Contract validation and loading errors.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Invalid contract JSON: {0}")]
    InvalidJson(String),

    #[error("JSON schema validation failed: {0}")]
    SchemaValidation(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Identity for authentication and authorization.
#[derive(Debug, Clone)]
pub enum Identity {
    /// User identity with sub, username, and groups.
    User {
        sub: String,
        username: String,
        groups: Vec<String>,
    },
    /// API key identity for public/apiKey auth rules.
    ApiKey,
}

impl Default for Identity {
    fn default() -> Self {
        Identity::User {
            sub: "test-sub".to_string(),
            username: "testuser".to_string(),
            groups: vec![],
        }
    }
}

impl Identity {
    /// Create a user identity with the given sub, username, and groups.
    pub fn user(sub: impl Into<String>, username: impl Into<String>, groups: Vec<String>) -> Self {
        Identity::User {
            sub: sub.into(),
            username: username.into(),
            groups,
        }
    }

    /// Get the owner value as `sub::username` format, or None for API key.
    pub fn owner_value(&self) -> Option<String> {
        match self {
            Identity::User { sub, username, .. } => Some(format!("{}::{}", sub, username)),
            Identity::ApiKey => None,
        }
    }

    /// Check if this identity is in the given groups.
    pub fn has_group(&self, group: &str) -> bool {
        match self {
            Identity::User { groups, .. } => groups.contains(&group.to_string()),
            Identity::ApiKey => false,
        }
    }
}

/// Allowed operations for an authorization rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ops(Vec<AuthOp>);

impl Ops {
    /// Create a new Ops from a slice of operations.
    pub fn from_operations(ops: Vec<AuthOp>) -> Self {
        Ops(ops)
    }

    /// Check if an operation is allowed.
    pub fn allows(&self, operation: AuthOp) -> bool {
        self.0.contains(&operation)
    }
}

/// Typed authorization rule.
#[derive(Debug, Clone)]
pub enum AuthRule {
    Owner { owner_field: String, ops: Ops },
    Groups { groups: Vec<String>, ops: Ops },
    Private { ops: Ops },
    Public { ops: Ops },
}

impl AuthRule {
    /// Parse an authorization rule from JSON.
    pub fn from_json(value: &Value) -> Result<Self> {
        let rule_obj = value
            .as_object()
            .ok_or_else(|| Error::Validation("Auth rule must be an object".to_string()))?;

        let allow = rule_obj
            .get("allow")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Auth rule missing 'allow'".to_string()))?;

        let operations_value = rule_obj
            .get("operations")
            .ok_or_else(|| Error::Validation("Auth rule missing 'operations'".to_string()))?;

        let operations: Vec<AuthOp> = serde_json::from_value(operations_value.clone())
            .map_err(|_| Error::Validation("Invalid operations format".to_string()))?;

        let ops = Ops::from_operations(operations);

        match allow {
            "owner" => {
                let owner_field = rule_obj
                    .get("ownerField")
                    .and_then(|v| v.as_str())
                    .unwrap_or("owner")
                    .to_string();
                Ok(AuthRule::Owner { owner_field, ops })
            }
            "groups" => {
                let groups = rule_obj
                    .get("groups")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(AuthRule::Groups { groups, ops })
            }
            "private" | "authenticated" => Ok(AuthRule::Private { ops }),
            "public" | "apiKey" => Ok(AuthRule::Public { ops }),
            // The contract schema only permits "custom" beyond the arms above.
            other => Err(Error::Validation(format!(
                "{other} (lambda) authorization is not supported"
            ))),
        }
    }
}

/// Check if an operation is allowed based on authorization rules.
/// Returns true if any rule allows the operation.
fn allowed(
    auth_rules: &[AuthRule],
    operation: AuthOp,
    identity: &Identity,
    record: Option<&Value>,
) -> bool {
    for rule in auth_rules {
        if !rule.allows(operation) {
            continue;
        }

        match rule {
            AuthRule::Owner { owner_field, .. } => {
                // For create, owner is allowed only if identity has an owner_value
                if operation == AuthOp::Create {
                    if identity.owner_value().is_some() {
                        return true;
                    }
                } else {
                    // For other ops, check if record's owner matches identity's owner value
                    if let Some(rec) = record {
                        if let Some(owner_value) = identity.owner_value() {
                            if let Some(rec_owner) = rec.get(owner_field).and_then(|v| v.as_str()) {
                                if rec_owner == owner_value {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            AuthRule::Groups { groups, .. } => {
                // Check if identity is in any of the rule's groups
                for group in groups {
                    if identity.has_group(group) {
                        return true;
                    }
                }
            }
            AuthRule::Private { .. } => {
                // Any user identity is allowed
                if matches!(identity, Identity::User { .. }) {
                    return true;
                }
            }
            AuthRule::Public { .. } => {
                // Only API key identity is allowed
                if matches!(identity, Identity::ApiKey) {
                    return true;
                }
            }
        }
    }

    false
}

impl AuthRule {
    /// Check if this rule allows the given operation.
    pub fn allows(&self, operation: AuthOp) -> bool {
        match self {
            AuthRule::Owner { ops, .. } => ops.allows(operation),
            AuthRule::Groups { ops, .. } => ops.allows(operation),
            AuthRule::Private { ops } => ops.allows(operation),
            AuthRule::Public { ops } => ops.allows(operation),
        }
    }
}

/// Encode a value as a base64url-encoded JSON token.
fn encode_token(value: &Value) -> String {
    let json_str = value.to_string();
    general_purpose::URL_SAFE_NO_PAD.encode(json_str.as_bytes())
}

/// Decode a base64url-encoded JSON token to a value.
fn decode_token(token: &str) -> std::result::Result<Value, String> {
    let decoded = general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| "Invalid token encoding".to_string())?;
    let json_str = String::from_utf8(decoded).map_err(|_| "Invalid token UTF-8".to_string())?;
    serde_json::from_str(&json_str).map_err(|_| "Invalid token JSON".to_string())
}

/// Extract key field values from a record.
fn key_of(record: &Value, fields: &[String]) -> Vec<Value> {
    fields
        .iter()
        .map(|f| record.get(f).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Compare two keys lexicographically, with proper value ordering.
fn cmp_keys(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (av, bv) in a.iter().zip(b.iter()) {
        if let Some(cmp) = compare_values(av, bv) {
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
    }
    a.len().cmp(&b.len())
}

/// A decoded `nextToken`: the last evaluated key, as an object with every key field.
type TokenKey = serde_json::Map<String, Value>;

/// An operation response: `(data, errors)`.
type OpResponse = (Value, Option<Vec<Value>>);

/// Parse and validate nextToken parameter.
/// Returns Option<Map> where Map is guaranteed to be a valid object with required key fields.
fn parse_next_token(
    args: &Value,
    key_fields: &[String],
) -> std::result::Result<Option<TokenKey>, OpResponse> {
    if let Some(token_val) = args.get("nextToken") {
        if token_val.is_null() {
            // null means first page
            return Ok(None);
        }
        if let Some(token_str) = token_val.as_str() {
            match decode_token(token_str) {
                Ok(key) => {
                    // Validate token has required key fields and extract as Map
                    if let Value::Object(key_obj) = key {
                        for key_field in key_fields {
                            if !key_obj.contains_key(key_field) {
                                return Err((
                                    Value::Null,
                                    Some(vec![json!({
                                        "errorType": "ValidationException",
                                        "message": format!("Invalid token: missing field '{}'", key_field)
                                    })]),
                                ));
                            }
                        }
                        Ok(Some(key_obj))
                    } else {
                        Err((
                            Value::Null,
                            Some(vec![json!({
                                "errorType": "ValidationException",
                                "message": "Invalid token: not a JSON object"
                            })]),
                        ))
                    }
                }
                Err(msg) => Err((
                    Value::Null,
                    Some(vec![json!({
                        "errorType": "ValidationException",
                        "message": msg
                    })]),
                )),
            }
        } else {
            Err((
                Value::Null,
                Some(vec![json!({
                    "errorType": "ValidationException",
                    "message": "nextToken must be a string"
                })]),
            ))
        }
    } else {
        Ok(None)
    }
}

/// Parse selectionSet from args; returns list of field paths or None if not specified.
fn parse_selection_set(args: &Value) -> Option<Vec<String>> {
    args.get("selectionSet")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .filter(|v: &Vec<String>| !v.is_empty())
}

/// Return all scalar fields (default when no selectionSet is provided).
fn filter_selection_default(record: &Value, _model: &Model) -> Value {
    let mut result = serde_json::Map::new();

    if let Some(obj) = record.as_object() {
        for (field_name, value) in obj.iter() {
            if field_name.starts_with("__") {
                continue; // Skip system fields
            }

            // Include all non-system fields (including composite sort attributes)
            result.insert(field_name.clone(), value.clone());
        }
    }

    Value::Object(result)
}

/// A data contract describing models, indexes, and relationships.
#[derive(Debug, Clone)]
pub struct Contract {
    models: HashMap<String, Model>,
    enums: HashMap<String, Enum>,
    custom_types: HashMap<String, CustomType>,
}

#[derive(Debug, Clone)]
pub struct Model {
    name: String,
    fields: HashMap<String, Field>,
    primary_key: Vec<String>,
    indexes: Vec<Index>,
    relationships: Vec<Relationship>,
    auth_rules: Vec<AuthRule>,
    owner_fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Field {
    field_type: String,
    kind: String,
    is_required: bool,
}

#[derive(Debug, Clone)]
pub struct Index {
    name: String,
    query_field: String,
    partition_field: String,
    sort_fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Relationship {
    field: String,
    kind: String,
    target: String,
    references: String,
    child_index: Option<String>,
    implicit: bool,
}

#[derive(Debug, Clone)]
pub struct Enum {
    values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CustomType;

impl Contract {
    /// Load and validate a contract from JSON string.
    pub fn from_json(json_str: &str) -> Result<Self> {
        let value: Value =
            serde_json::from_str(json_str).map_err(|e| Error::InvalidJson(e.to_string()))?;

        // Validate against schema
        Self::validate_schema(&value)?;

        // Parse models
        let mut models = HashMap::new();
        if let Some(models_obj) = value.get("models").and_then(|v| v.as_object()) {
            for (name, model_val) in models_obj {
                models.insert(name.clone(), Self::parse_model(model_val)?);
            }
        }

        // Parse enums
        let mut enums = HashMap::new();
        if let Some(enums_obj) = value.get("enums").and_then(|v| v.as_object()) {
            for (name, enum_val) in enums_obj {
                enums.insert(name.clone(), Self::parse_enum(enum_val, name)?);
            }
        }

        // Parse custom types
        let mut custom_types = HashMap::new();
        if let Some(types_obj) = value.get("customTypes").and_then(|v| v.as_object()) {
            for (name, _type_val) in types_obj {
                custom_types.insert(name.clone(), CustomType);
            }
        }

        // Semantic validation
        Self::validate_semantics(&models, &enums, &custom_types)?;

        Ok(Contract {
            models,
            enums,
            custom_types,
        })
    }

    fn validate_schema(value: &Value) -> Result<()> {
        let schema_str = include_str!("../contract/contract.schema.json");
        let schema: Value = serde_json::from_str(schema_str)
            .map_err(|e| Error::Internal(format!("Failed to parse schema: {}", e)))?;

        // Create a JSON schema validator using Draft202012
        let validator = jsonschema::JSONSchema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .compile(&schema)
            .map_err(|e| Error::Internal(format!("Failed to compile schema: {}", e)))?;

        // Validate the contract and collect errors
        let mut errors = Vec::new();
        if let Err(validation_errors) = validator.validate(value) {
            for error in validation_errors {
                let path = error.instance_path.to_string();
                let path_str = if path.is_empty() { "/" } else { &path };
                let message = format!("{}: {}", path_str, error);
                errors.push(message);
            }
        }

        if !errors.is_empty() {
            Err(Error::Validation(errors.join("; ")))
        } else {
            Ok(())
        }
    }

    /// Semantic validation checks that go beyond schema validation.
    fn validate_semantics(
        models: &HashMap<String, Model>,
        enums: &HashMap<String, Enum>,
        custom_types: &HashMap<String, CustomType>,
    ) -> Result<()> {
        // Check that primary key fields exist for each model
        for (model_name, model) in models {
            for pk_field in &model.primary_key {
                if !model.fields.contains_key(pk_field) {
                    return Err(Error::Validation(format!(
                        "Model '{}' primaryKey field '{}' does not exist in fields",
                        model_name, pk_field
                    )));
                }
            }

            // Check that index partition and sort fields exist
            for index in &model.indexes {
                if !model.fields.contains_key(&index.partition_field) {
                    return Err(Error::Validation(format!(
                        "Index '{}' on model '{}' has partition field '{}' that does not exist",
                        index.name, model_name, index.partition_field
                    )));
                }
                for sort_field in &index.sort_fields {
                    if !model.fields.contains_key(sort_field) {
                        return Err(Error::Validation(format!(
                            "Index '{}' on model '{}' has sort field '{}' that does not exist",
                            index.name, model_name, sort_field
                        )));
                    }
                }
            }

            // Check that relationships reference existing models
            for rel in &model.relationships {
                if !models.contains_key(&rel.target) {
                    return Err(Error::Validation(format!(
                        "Relationship in model '{}' targets non-existent model '{}'",
                        model_name, rel.target
                    )));
                }
            }

            // Check that fields with enum/customType kind reference defined types
            for (field_name, field) in &model.fields {
                #[allow(clippy::collapsible_match)]
                match field.kind.as_str() {
                    "enum" => {
                        if !enums.contains_key(&field.field_type) {
                            return Err(Error::Validation(format!(
                                "Field '{}' in model '{}' references undefined enum '{}'",
                                field_name, model_name, field.field_type
                            )));
                        }
                    }
                    "customType" => {
                        if !custom_types.contains_key(&field.field_type) {
                            return Err(Error::Validation(format!(
                                "Field '{}' in model '{}' references undefined customType '{}'",
                                field_name, model_name, field.field_type
                            )));
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    fn parse_model(value: &Value) -> Result<Model> {
        let name = value
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Model missing name".to_string()))?
            .to_string();

        let mut fields = HashMap::new();
        if let Some(fields_obj) = value.get("fields").and_then(|v| v.as_object()) {
            for (field_name, field_val) in fields_obj {
                fields.insert(field_name.clone(), Self::parse_field(field_val)?);
            }
        }

        let primary_key = value
            .get("primaryKey")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Error::Validation(format!("Model {} missing primaryKey", name)))?
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();

        let mut indexes = Vec::new();
        if let Some(indexes_arr) = value.get("indexes").and_then(|v| v.as_array()) {
            for idx_val in indexes_arr {
                indexes.push(Self::parse_index(idx_val)?);
            }
        }

        let mut relationships = Vec::new();
        if let Some(rels_arr) = value.get("relationships").and_then(|v| v.as_array()) {
            for rel_val in rels_arr {
                relationships.push(Self::parse_relationship(rel_val)?);
            }
        }

        let auth_rules = value
            .get("authRules")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(AuthRule::from_json)
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();

        let owner_fields = value
            .get("ownerFields")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Model {
            name,
            fields,
            primary_key,
            indexes,
            relationships,
            auth_rules,
            owner_fields,
        })
    }

    fn parse_field(value: &Value) -> Result<Field> {
        let field_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("String")
            .to_string();

        let kind = value
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("scalar")
            .to_string();

        let is_required = value
            .get("isRequired")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Field {
            field_type,
            kind,
            is_required,
        })
    }

    fn parse_index(value: &Value) -> Result<Index> {
        let name = value
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Index missing name".to_string()))?
            .to_string();

        let query_field = value
            .get("queryField")
            .and_then(|v| v.as_str())
            .unwrap_or(&name)
            .to_string();

        let partition_field = value
            .get("partitionField")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Index missing partitionField".to_string()))?
            .to_string();

        let sort_fields = value
            .get("sortFields")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        Ok(Index {
            name,
            query_field,
            partition_field,
            sort_fields,
        })
    }

    fn parse_relationship(value: &Value) -> Result<Relationship> {
        let field = value
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Relationship missing field".to_string()))?
            .to_string();

        let kind = value
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Relationship missing kind".to_string()))?
            .to_string();

        let target = value
            .get("target")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Relationship missing target".to_string()))?
            .to_string();

        let references = value
            .get("references")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let child_index = value
            .get("childIndex")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let implicit = value
            .get("implicit")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Relationship {
            field,
            kind,
            target,
            references,
            child_index,
            implicit,
        })
    }

    fn parse_enum(value: &Value, _name: &str) -> Result<Enum> {
        // Enums are guaranteed to be arrays by schema validation
        let arr = value
            .as_array()
            .ok_or_else(|| Error::Validation("Enum must be an array".to_string()))?;
        let values = arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        Ok(Enum { values })
    }

    /// Get the models in this contract.
    pub fn models(&self) -> &HashMap<String, Model> {
        &self.models
    }

    /// Get the enums in this contract.
    pub fn enums(&self) -> &HashMap<String, Enum> {
        &self.enums
    }

    /// Get the custom types in this contract.
    pub fn custom_types(&self) -> &HashMap<String, CustomType> {
        &self.custom_types
    }
}

impl Model {
    /// Get the model name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the primary key fields.
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Get the indexes.
    pub fn indexes(&self) -> &[Index] {
        &self.indexes
    }

    /// Get the relationships.
    pub fn relationships(&self) -> &[Relationship] {
        &self.relationships
    }

    /// Get the authorization rules.
    pub fn auth_rules(&self) -> &[AuthRule] {
        &self.auth_rules
    }

    /// Get the owner field names.
    pub fn owner_fields(&self) -> &[String] {
        &self.owner_fields
    }
}

impl Index {
    /// Get the index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the query field name.
    pub fn query_field(&self) -> &str {
        &self.query_field
    }

    /// Get the partition field.
    pub fn partition_field(&self) -> &str {
        &self.partition_field
    }

    /// Get the sort fields.
    pub fn sort_fields(&self) -> &[String] {
        &self.sort_fields
    }

    /// Get the index key (partition field + sort fields).
    pub fn index_key(&self) -> Vec<String> {
        let mut key = vec![self.partition_field.clone()];
        key.extend(self.sort_fields.clone());
        key
    }
}

impl Relationship {
    /// Get the field name.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Get the relationship kind (hasMany, belongsTo, etc).
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Get the target model name.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Get the references field.
    pub fn references(&self) -> &str {
        &self.references
    }

    /// Is this an implicit relationship?
    pub fn is_implicit(&self) -> bool {
        self.implicit
    }

    /// Get the child index name for implicit hasMany relationships.
    pub fn child_index(&self) -> Option<&str> {
        self.child_index.as_deref()
    }
}

impl Enum {
    /// Get the enum values.
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

/// Engine options.
/// Engine options for authorization and behavior control.
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// Whether to enforce authorization rules. If false (default), all operations are allowed.
    pub enforce_auth: bool,
}

/// Operation result containing data, errors, and optional pagination token.
#[derive(Debug, Clone)]
pub struct OpResult {
    pub data: Value,
    pub errors: Option<Vec<Value>>,
    pub next_token: Option<String>,
}

/// Typed filter representation for parsing and evaluation.
#[derive(Debug, Clone)]
enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Field(String, Vec<Op>),
}

/// Filter operators.
#[derive(Debug, Clone)]
enum Op {
    Eq(Value),
    Ne(Value),
    Lt(Value),
    Le(Value),
    Gt(Value),
    Ge(Value),
    Between(Value, Value),
    BeginsWith(String),
    Contains(Value),
    NotContains(Value),
    AttributeExists(bool),
    Size(Box<Op>),
    InvalidOperand, // Represents an operand that failed type validation
}

/// Parse filter JSON into typed Filter enum.
fn parse_filter(filter: &Value) -> std::result::Result<Filter, String> {
    match filter {
        Value::Object(obj) => {
            // Single-pass parsing: iterate once and match each key
            let mut parts: Vec<Filter> = Vec::new();

            for (key, value) in obj.iter() {
                match key.as_str() {
                    "and" => {
                        if let Value::Array(conditions) = value {
                            let mut parsed = Vec::new();
                            for cond in conditions {
                                parsed.push(parse_filter(cond)?);
                            }
                            parts.push(Filter::And(parsed));
                        } else {
                            return Err("and must be an array".to_string());
                        }
                    }
                    "or" => {
                        if let Value::Array(conditions) = value {
                            let mut parsed = Vec::new();
                            for cond in conditions {
                                parsed.push(parse_filter(cond)?);
                            }
                            parts.push(Filter::Or(parsed));
                        } else {
                            return Err("or must be an array".to_string());
                        }
                    }
                    "not" => {
                        parts.push(Filter::Not(Box::new(parse_filter(value)?)));
                    }
                    field_name => {
                        // Regular field condition
                        let field_ops = parse_condition(value)?;
                        parts.push(Filter::Field(field_name.to_string(), field_ops));
                    }
                }
            }

            if parts.is_empty() {
                return Err(
                    "Filter object must contain at least one field or logical operator".to_string(),
                );
            }

            // Return single part directly, or combine multiple parts with And
            if parts.len() == 1 {
                Ok(parts.into_iter().next().unwrap())
            } else {
                Ok(Filter::And(parts))
            }
        }
        _ => Err("Filter must be an object".to_string()),
    }
}

/// Parse condition operators for a field.
fn parse_condition(condition: &Value) -> std::result::Result<Vec<Op>, String> {
    match condition {
        Value::Object(ops_obj) => {
            let mut result = Vec::new();
            for (op_name, op_value) in ops_obj.iter() {
                let op = match op_name.as_str() {
                    "eq" => Op::Eq(op_value.clone()),
                    "ne" => Op::Ne(op_value.clone()),
                    "lt" => Op::Lt(op_value.clone()),
                    "le" => Op::Le(op_value.clone()),
                    "gt" => Op::Gt(op_value.clone()),
                    "ge" => Op::Ge(op_value.clone()),
                    "between" => {
                        if let Value::Array(bounds) = op_value {
                            if bounds.len() == 2 {
                                Op::Between(bounds[0].clone(), bounds[1].clone())
                            } else {
                                // Invalid bounds count is a validation error
                                return Err("between requires a 2-element array".to_string());
                            }
                        } else {
                            // Non-array value is a validation error
                            return Err("between requires a 2-element array".to_string());
                        }
                    }
                    "beginsWith" => {
                        // For beginsWith, if we can extract a string, use it; otherwise use a marker
                        if let Value::String(s) = op_value {
                            Op::BeginsWith(s.clone())
                        } else {
                            // Non-string operand will never match - use a sentinel marker
                            Op::BeginsWith("\x00INVALID_OPERAND_TYPE\x00".to_string())
                        }
                    }
                    "contains" => Op::Contains(op_value.clone()),
                    "notContains" => Op::NotContains(op_value.clone()),
                    "attributeExists" => {
                        if let Value::Bool(b) = op_value {
                            Op::AttributeExists(*b)
                        } else {
                            // Non-boolean values: mark as invalid operand
                            Op::InvalidOperand
                        }
                    }
                    "size" => {
                        if let Value::Object(_) = op_value {
                            // Parse the nested size condition
                            let size_ops = parse_condition(op_value)?;
                            if size_ops.len() != 1 {
                                return Err(
                                    "size operator must have exactly one condition".to_string()
                                );
                            }
                            Op::Size(Box::new(size_ops.into_iter().next().unwrap()))
                        } else {
                            return Err("size operator requires an object condition".to_string());
                        }
                    }
                    _ => return Err(format!("Unknown filter operator: {}", op_name)),
                };
                result.push(op);
            }
            Ok(result)
        }
        _ => Err("Field condition must be an object".to_string()),
    }
}

/// Filter evaluation: supports eq, ne, lt, le, gt, ge, between, beginsWith, contains,
/// notContains, attributeExists, size, and logical operators and/or/not.
/// Evaluate typed filter against a record.
fn evaluate_filter_typed(filter: &Filter, record: &Value) -> bool {
    match filter {
        Filter::And(filters) => filters.iter().all(|f| evaluate_filter_typed(f, record)),
        Filter::Or(filters) => filters.iter().any(|f| evaluate_filter_typed(f, record)),
        Filter::Not(f) => !evaluate_filter_typed(f, record),
        Filter::Field(field_name, ops) => {
            let field_value = record.get(field_name);
            ops.iter().all(|op| evaluate_op(op, field_value))
        }
    }
}

/// Evaluate operator against field value.
fn evaluate_op(op: &Op, field_value: Option<&Value>) -> bool {
    match op {
        Op::Eq(op_value) => {
            if let Some(fv) = field_value {
                fv == op_value
            } else {
                false
            }
        }
        Op::Ne(op_value) => !field_value.is_some_and(|fv| fv == op_value),
        Op::Lt(op_value) => {
            if let Some(fv) = field_value {
                compare_values(fv, op_value) == Some(std::cmp::Ordering::Less)
            } else {
                false
            }
        }
        Op::Le(op_value) => {
            if let Some(fv) = field_value {
                matches!(
                    compare_values(fv, op_value),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            } else {
                false
            }
        }
        Op::Gt(op_value) => {
            if let Some(fv) = field_value {
                compare_values(fv, op_value) == Some(std::cmp::Ordering::Greater)
            } else {
                false
            }
        }
        Op::Ge(op_value) => {
            if let Some(fv) = field_value {
                matches!(
                    compare_values(fv, op_value),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            } else {
                false
            }
        }
        Op::Between(lower, upper) => {
            if let Some(fv) = field_value {
                matches!(
                    (compare_values(fv, lower), compare_values(fv, upper)),
                    (
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
                    )
                )
            } else {
                false
            }
        }
        Op::BeginsWith(prefix) => {
            if let Some(Value::String(fv)) = field_value {
                fv.starts_with(prefix)
            } else {
                false
            }
        }
        Op::Contains(op_value) => {
            if let Some(fv) = field_value {
                match fv {
                    Value::String(s) => {
                        if let Value::String(substr) = op_value {
                            s.contains(substr)
                        } else {
                            false
                        }
                    }
                    Value::Array(arr) => arr.iter().any(|v| v == op_value),
                    _ => false,
                }
            } else {
                false
            }
        }
        Op::NotContains(op_value) => {
            if let Some(fv) = field_value {
                match fv {
                    Value::String(s) => {
                        if let Value::String(substr) = op_value {
                            !s.contains(substr)
                        } else {
                            false
                        }
                    }
                    Value::Array(arr) => !arr.iter().any(|v| v == op_value),
                    _ => false,
                }
            } else {
                true
            }
        }
        Op::AttributeExists(should_exist) => field_value.is_some() == *should_exist,
        Op::Size(inner_op) => {
            if let Some(fv) = field_value {
                let len = match fv {
                    Value::String(s) => s.len() as i64,
                    Value::Array(a) => a.len() as i64,
                    _ => return false,
                };
                let len_val = Value::Number(len.into());
                evaluate_op(inner_op, Some(&len_val))
            } else {
                false
            }
        }
        Op::InvalidOperand => {
            // Invalid operands never match
            false
        }
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => an
            .as_f64()
            .and_then(|af| bn.as_f64().map(|bf| (af, bf)))
            .and_then(|(af, bf)| af.partial_cmp(&bf)),
        (Value::String(as_), Value::String(bs)) => Some(as_.cmp(bs)),
        _ => None,
    }
}

/// Parse key condition for index queries into SortCondition.
fn parse_key_condition(
    key_arg: Option<&Value>,
    index: &Index,
) -> std::result::Result<(Value, Option<SortCondition>), String> {
    // Validate key is provided and is an object
    let key_obj = match key_arg {
        Some(key) => key,
        None => {
            return Err(format!(
                "Index query requires partition key {}",
                index.partition_field()
            ));
        }
    };

    let key_map = match key_obj {
        Value::Object(map) => map,
        _ => {
            return Err("Index query requires a key object".to_string());
        }
    };

    // Validate partition field is present
    let partition = match key_map.get(index.partition_field()) {
        Some(val) => val.clone(),
        None => {
            return Err(format!(
                "Index query requires partition key {}",
                index.partition_field()
            ));
        }
    };

    if index.sort_fields.is_empty() {
        return Ok((partition, None));
    }

    // Parse sort condition from key_map
    let sort_condition = if index.sort_fields.len() == 1 {
        // Single sort field case
        let sort_field = &index.sort_fields[0];
        if let Some(sort_value) = key_map.get(sort_field) {
            Some(parse_single_sort_condition_value(sort_value)?)
        } else {
            // No sort key condition provided
            None
        }
    } else {
        // Composite sort key case
        Some(parse_composite_key_condition(
            key_map,
            &index.sort_fields,
            index.partition_field(),
        )?)
    };

    Ok((partition, sort_condition))
}

fn parse_single_sort_condition_value(value: &Value) -> std::result::Result<SortCondition, String> {
    match value {
        Value::Object(op_map) => {
            // Must have exactly one operator entry
            if op_map.is_empty() {
                return Err("Sort key condition object must not be empty".to_string());
            }
            match (op_map.len(), op_map.iter().next()) {
                (1, Some((op, op_value))) => parse_single_sort_operator(op, op_value),
                _ => Err("Sort key condition must have exactly one operator".to_string()),
            }
        }
        // Bare values are not allowed - must be wrapped in {op: value}
        _ => Err("Sort key condition must be an object with an operator".to_string()),
    }
}

fn parse_single_sort_operator(
    op: &str,
    value: &Value,
) -> std::result::Result<SortCondition, String> {
    match op {
        "eq" => Ok(SortCondition::Eq(value.clone())),
        "lt" => Ok(SortCondition::Lt(value.clone())),
        "le" => Ok(SortCondition::Lte(value.clone())),
        "gt" => Ok(SortCondition::Gt(value.clone())),
        "ge" => Ok(SortCondition::Gte(value.clone())),
        "between" => {
            if let Value::Array(bounds) = value {
                if bounds.len() != 2 {
                    return Err("between operator requires exactly 2 values".to_string());
                }
                Ok(SortCondition::Between(bounds[0].clone(), bounds[1].clone()))
            } else {
                Err("between operator value must be an array".to_string())
            }
        }
        "beginsWith" => {
            if let Value::String(s) = value {
                Ok(SortCondition::BeginsWith(s.clone()))
            } else {
                Err("beginsWith operator requires a string value".to_string())
            }
        }
        _ => Err(format!("Unknown sort operator: {}", op)),
    }
}

fn parse_composite_key_condition(
    key_map: &serde_json::Map<String, Value>,
    sort_fields: &[String],
    partition_field: &str,
) -> std::result::Result<SortCondition, String> {
    // Find the operator key (any key that isn't a sort field or partition field)
    let mut op_key: Option<&str> = None;
    let mut op_value: Option<&Value> = None;

    for (key, value) in key_map.iter() {
        if !sort_fields.contains(&key.to_string()) && key != partition_field {
            if op_key.is_some() {
                return Err("exactly one sort key operator".to_string());
            }
            op_key = Some(key);
            op_value = Some(value);
        }
    }

    if let Some(op) = op_key {
        let op_value = op_value.unwrap();
        // Handle between specially - it has array value, not object
        if op == "between" {
            if let Value::Array(bounds) = op_value {
                if bounds.len() != 2 {
                    return Err("between requires exactly 2 bounds".to_string());
                }

                let mut lower_values = Vec::new();
                let mut upper_values = Vec::new();

                // Parse lower bound
                if let Value::Object(lower_fields) = &bounds[0] {
                    for field in sort_fields {
                        if let Some(v) = lower_fields.get(field) {
                            if let Value::String(s) = v {
                                lower_values.push(s.clone());
                            } else {
                                return Err(format!("Bound field {} must be a string", field));
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    return Err("between bounds must be objects".to_string());
                }

                // Parse upper bound
                if let Value::Object(upper_fields) = &bounds[1] {
                    for field in sort_fields {
                        if let Some(v) = upper_fields.get(field) {
                            if let Value::String(s) = v {
                                upper_values.push(s.clone());
                            } else {
                                return Err(format!("Bound field {} must be a string", field));
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    return Err("between bounds must be objects".to_string());
                }

                if lower_values.is_empty() || upper_values.is_empty() {
                    return Err("between requires at least one field in each bound".to_string());
                }

                return Ok(SortCondition::Between(
                    Value::String(lower_values.join("#")),
                    Value::String(upper_values.join("#")),
                ));
            } else {
                return Err("between operator requires an array value".to_string());
            }
        }

        // Other operators: expect object value
        if let Value::Object(fields) = op_value {
            return parse_composite_operator_format(op, fields, sort_fields);
        } else {
            return Err(format!("Operator {} requires an object value", op));
        }
    }

    // No operator found - error
    Err("No sort key operator specified".to_string())
}

fn parse_composite_operator_format(
    op: &str,
    fields: &serde_json::Map<String, Value>,
    sort_fields: &[String],
) -> std::result::Result<SortCondition, String> {
    // Parse operator string into enum to ensure exhaustive matching
    enum CompositeOp {
        Eq,
        Lt,
        Lte,
        Gt,
        Gte,
        BeginsWith,
    }

    let op_enum = match op {
        "eq" => CompositeOp::Eq,
        "lt" => CompositeOp::Lt,
        "le" => CompositeOp::Lte,
        "gt" => CompositeOp::Gt,
        "ge" => CompositeOp::Gte,
        "beginsWith" => CompositeOp::BeginsWith,
        _ => return Err(format!("Unknown sort operator: {}", op)),
    };

    // Collect values from fields
    let mut values = Vec::new();
    for field in sort_fields {
        if let Some(Value::String(v)) = fields.get(field) {
            values.push(v.clone());
        } else if fields.contains_key(field) {
            return Err(format!("Sort field {} must be a string", field));
        } else {
            // End of prefix
            break;
        }
    }

    if values.is_empty() {
        return Err("Operator requires at least one sort key field".to_string());
    }

    let composite_value = values.join("#");

    // Match exhaustively on enum
    Ok(match op_enum {
        CompositeOp::Eq => SortCondition::Eq(Value::String(composite_value)),
        CompositeOp::Lt => SortCondition::Lt(Value::String(composite_value)),
        CompositeOp::Lte => SortCondition::Lte(Value::String(composite_value)),
        CompositeOp::Gt => SortCondition::Gt(Value::String(composite_value)),
        CompositeOp::Gte => SortCondition::Gte(Value::String(composite_value)),
        CompositeOp::BeginsWith => SortCondition::BeginsWith(composite_value),
    })
}

/// Storage-less or file-backed engine for Amplify-shaped operations.
/// Composite sort attributes are synthetic and will be populated by writes (V8b) as `v1#v2`.
#[derive(Debug)]
pub struct Engine {
    contract: Contract,
    tables: BTreeMap<String, Table>,
    options: EngineOptions,
}

impl Engine {
    /// Open or create an engine with the given contract.
    pub fn open(
        directory: Option<PathBuf>,
        contract: Contract,
        options: EngineOptions,
    ) -> Result<Self> {
        // Create one table per model
        let mut tables: BTreeMap<String, Table> = BTreeMap::new();

        for (model_name, model) in contract.models() {
            let primary_key = model.primary_key();

            // Handle composite/compound primary keys
            let pk_sort_key_name: Option<String>;
            let (pk, partition, sort) = match primary_key.len() {
                1 => {
                    // Simple primary key
                    (Some(primary_key[0].as_str()), None, None)
                }
                2 => {
                    // Composite key: partition and sort
                    (
                        None,
                        Some(primary_key[0].as_str()),
                        Some(primary_key[1].as_str()),
                    )
                }
                _ => {
                    // More than 2 fields: first is partition, rest are joined as sort key attribute name
                    pk_sort_key_name = Some(primary_key[1..].join("#"));
                    (
                        None,
                        Some(primary_key[0].as_str()),
                        pk_sort_key_name.as_deref(),
                    )
                }
            };

            // Create per-model directory if directory is provided
            let model_directory = directory.as_ref().map(|d| d.join(model_name));

            // The Amplify engine validates records against the contract itself (required fields,
            // types, enums), and DynamoDB GSIs are sparse: a record missing an index's key
            // attributes is simply not in that index. Silent mode allows this sparse indexing.
            let mut table = Table::new(
                model_name,
                pk,
                partition,
                sort,
                model_directory,
                virtuus::table::ValidationMode::Silent,
            );

            // Add GSIs for explicit indexes, registered by queryField
            for index in model.indexes() {
                let index_sort_key_name: Option<String>;
                let sort_key = if index.sort_fields().is_empty() {
                    None
                } else if index.sort_fields().len() == 1 {
                    Some(index.sort_fields()[0].as_str())
                } else {
                    // Multiple sort fields: use joined name as the attribute name
                    index_sort_key_name = Some(index.sort_fields().join("#"));
                    index_sort_key_name.as_deref()
                };
                // Register GSI by queryField, not by name
                table.add_gsi(index.query_field(), index.partition_field(), sort_key);
            }

            tables.insert(model_name.to_string(), table);
        }

        // Add implicit indexes from hasMany relationships
        // These indexes are on the child table, keyed by the foreign key
        // Only add if no explicit index with that queryField already exists
        for model in contract.models().values() {
            for rel in model.relationships() {
                if rel.kind() == "hasMany" && rel.is_implicit() {
                    // Add the index to the child table (referenced model)
                    if let Some(table) = tables.get_mut(rel.target()) {
                        if let Some(child_index) = rel.child_index() {
                            // Check if an explicit index with this queryField already exists on the TARGET model
                            let target_model = contract.models().get(rel.target()).unwrap();
                            let has_explicit = target_model
                                .indexes()
                                .iter()
                                .any(|idx| idx.query_field() == child_index);
                            if !has_explicit {
                                table.add_gsi(child_index, rel.references(), None);
                            }
                        }
                    }
                }
            }
        }

        // Warm all tables to load existing records from disk
        for table in tables.values_mut() {
            table.warm();
        }

        Ok(Engine {
            contract,
            tables,
            options,
        })
    }

    /// Get a table by model name.
    pub fn table(&self, model: &str) -> Option<&Table> {
        self.tables.get(model)
    }

    /// Get a mutable table by model name.
    pub fn table_mut(&mut self, model: &str) -> Option<&mut Table> {
        self.tables.get_mut(model)
    }

    /// Describe the engine's tables and indexes.
    /// Computed from the constructed tables, not the contract.
    pub fn describe(&self) -> Value {
        let mut result = serde_json::Map::new();
        let mut tables_array = Vec::new();

        // Iterate through tables in sorted order (BTreeMap is ordered)
        for (model_name, table) in &self.tables {
            let mut table_info = serde_json::Map::new();
            table_info.insert("name".to_string(), Value::String(model_name.clone()));

            // Get primary key info from the table
            // Determine if we have a primary key or partition/sort keys
            let model = self.contract.models().get(model_name).unwrap();
            let primary_key = model.primary_key();

            if primary_key.len() == 1 {
                table_info.insert(
                    "primaryKey".to_string(),
                    Value::String(primary_key[0].clone()),
                );
            } else {
                table_info.insert(
                    "partitionKey".to_string(),
                    Value::String(primary_key[0].clone()),
                );
                table_info.insert(
                    "sortKey".to_string(),
                    Value::String(if primary_key.len() > 2 {
                        primary_key[1..].join("#")
                    } else {
                        primary_key[1].clone()
                    }),
                );
            }

            // Get indexes from the actual table
            // Sort by: explicit indexes first (by contract order), then implicit indexes
            let mut indexes_array = Vec::new();
            let mut gsi_entries: Vec<_> = table.gsis().keys().cloned().collect();

            // Sort so that explicit indexes come first, then implicit, and within each group sort alphabetically
            gsi_entries.sort_by_key(|name| {
                let is_explicit = model.indexes().iter().any(|idx| idx.query_field() == name);
                // (is_implicit ? 1 : 0, name) ensures explicit (false/0) come before implicit (true/1)
                (!is_explicit, name.clone())
            });

            for gsi_name in gsi_entries {
                if let Some(gsi) = table.gsis().get(&gsi_name) {
                    let mut idx_info = serde_json::Map::new();
                    idx_info.insert("name".to_string(), Value::String(gsi_name.clone()));
                    idx_info.insert(
                        "partitionKey".to_string(),
                        Value::String(gsi.partition_key().to_string()),
                    );

                    if let Some(sort_key) = gsi.sort_key() {
                        idx_info.insert("sortKey".to_string(), Value::String(sort_key.to_string()));
                    }

                    // Check if this is an implicit index by checking:
                    // 1. Is there an explicit index with this queryField in the contract for this model?
                    // 2. If not, is it created by a hasMany relationship?
                    let is_explicit = model
                        .indexes()
                        .iter()
                        .any(|idx| idx.query_field() == gsi_name);

                    let mut is_implicit = false;
                    if !is_explicit {
                        // Check if this index was created by an implicit hasMany relationship
                        for other_model in self.contract.models().values() {
                            for rel in other_model.relationships() {
                                if rel.kind() == "hasMany"
                                    && rel.is_implicit()
                                    && rel.target() == model_name
                                {
                                    if let Some(child_index) = rel.child_index() {
                                        if child_index == gsi_name {
                                            is_implicit = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            if is_implicit {
                                break;
                            }
                        }
                    }

                    if is_implicit {
                        idx_info.insert("implicit".to_string(), Value::Bool(true));
                    }

                    indexes_array.push(Value::Object(idx_info));
                }
            }

            table_info.insert("indexes".to_string(), Value::Array(indexes_array));
            tables_array.push(Value::Object(table_info));
        }

        result.insert("tables".to_string(), Value::Array(tables_array));
        Value::Object(result)
    }

    /// Apply selectionSet filtering and resolve belongsTo and hasMany relationships.
    fn apply_selection_with_relationships(
        &self,
        record: &Value,
        model: &Model,
        selection: &[String],
        _args: &Value,
        identity: &Identity,
    ) -> Result<Value> {
        let mut result = serde_json::Map::new();

        for path in selection {
            if path == "*" {
                return Ok(filter_selection_default(record, model));
            }

            if path.contains('.') {
                // Dotted path like "blog.name" or "comments.*": resolve belongsTo or hasMany relationship
                let parts: Vec<&str> = path.split('.').collect();
                if parts.len() >= 2 {
                    let rel_field = parts[0];
                    let rel_path = parts[1..].join(".");

                    // Find relationship definition
                    if let Some(rel) = model.relationships.iter().find(|r| r.field == rel_field) {
                        if rel.kind == "belongsTo" {
                            // Fetch related record via foreign key
                            if let Some(fk_value) = record.get(&rel.references) {
                                let fk_str = fk_value.as_str().map(|s| s.to_string());
                                if let Some(fk) = fk_str {
                                    let target_model =
                                        self.contract.models().get(&rel.target).cloned();
                                    if let Some(tm) = target_model {
                                        let target_table = self.tables.get(&rel.target);
                                        if let Some(table) = target_table {
                                            if let Some(related) = table.get(&fk, None) {
                                                // Check authorization for read
                                                let can_read = if self.options.enforce_auth {
                                                    allowed(
                                                        tm.auth_rules(),
                                                        AuthOp::Read,
                                                        identity,
                                                        Some(&related),
                                                    )
                                                } else {
                                                    true
                                                };

                                                if can_read {
                                                    if rel_path == "*" {
                                                        // Include all scalar fields of related model
                                                        result.insert(
                                                            rel_field.to_string(),
                                                            filter_selection_default(&related, &tm),
                                                        );
                                                    } else {
                                                        // Nested field selection
                                                        let empty_args =
                                                            Value::Object(serde_json::Map::new());
                                                        let nested = self
                                                            .apply_selection_with_relationships(
                                                                &related,
                                                                &tm,
                                                                &[rel_path],
                                                                &empty_args,
                                                                identity,
                                                            )?;
                                                        result
                                                            .insert(rel_field.to_string(), nested);
                                                    }
                                                } else {
                                                    result
                                                        .insert(rel_field.to_string(), Value::Null);
                                                }
                                            } else {
                                                result.insert(rel_field.to_string(), Value::Null);
                                            }
                                        }
                                    }
                                }
                            } else {
                                result.insert(rel_field.to_string(), Value::Null);
                            }
                        } else if rel.kind == "hasMany" {
                            // HasMany: return an empty connection for now (will be populated in call method)
                            let conn = json!({
                                "items": [],
                                "nextToken": null
                            });
                            result.insert(rel_field.to_string(), conn);
                        }
                    }
                }
            } else {
                // Simple field: include if scalar/enum/customType
                if let Some(field) = model.fields.get(path) {
                    if field.kind == "scalar" || field.kind == "enum" || field.kind == "customType"
                    {
                        if let Some(value) = record.get(path) {
                            result.insert(path.clone(), value.clone());
                        }
                    }
                }
            }
        }

        Ok(Value::Object(result))
    }

    /// Populate hasMany relationships in a result object by querying child indexes.
    fn populate_has_many_relationships(
        &mut self,
        mut result: Value,
        record: &Value,
        model: &Model,
        selection: &[String],
        args: &Value,
        identity: &Identity,
    ) -> Result<Value> {
        for path in selection {
            if path.contains('.') {
                let parts: Vec<&str> = path.split('.').collect();
                if parts.len() >= 2 {
                    let rel_field = parts[0];
                    let rel_path = parts[1..].join(".");

                    // Only handle hasMany relationships ending with ".*"
                    if rel_path == "*" {
                        if let Some(rel) = model.relationships.iter().find(|r| r.field == rel_field)
                        {
                            if rel.kind == "hasMany" {
                                // Get the parent id
                                if let Some(parent_id) = record.get(&model.primary_key[0]) {
                                    // Build key query for the child index
                                    let mut key_condition = serde_json::Map::new();
                                    key_condition.insert(rel.references.clone(), parent_id.clone());

                                    let mut query_args = json!({
                                        "key": Value::Object(key_condition),
                                    });

                                    // Add relationshipArgs if provided
                                    if let Some(rel_args_obj) = args.get("relationshipArgs") {
                                        if let Some(field_args) = rel_args_obj.get(rel_field) {
                                            if let Some(limit) = field_args.get("limit") {
                                                query_args["limit"] = limit.clone();
                                            }
                                            if let Some(filter) = field_args.get("filter") {
                                                query_args["filter"] = filter.clone();
                                            }
                                            if let Some(sort_dir) = field_args.get("sortDirection")
                                            {
                                                query_args["sortDirection"] = sort_dir.clone();
                                            }
                                            if let Some(next_token) = field_args.get("nextToken") {
                                                query_args["nextToken"] = next_token.clone();
                                            }
                                        }
                                    }

                                    // Get the target model to find its primary key
                                    if let Some(target_model) =
                                        self.contract.models().get(&rel.target)
                                    {
                                        // Find the child index for this relationship
                                        let child_index =
                                            target_model.indexes().iter().find(|idx| {
                                                idx.index_key().contains(&rel.references.clone())
                                            });

                                        if let Some(child_idx) = child_index {
                                            // Execute the index query
                                            let table = self.tables.get_mut(&rel.target).expect(
                                                "Table should exist for relationship target",
                                            );

                                            // Parse key condition
                                            let (partition, sort_condition) = parse_key_condition(
                                                query_args.get("key"),
                                                child_idx,
                                            )
                                            .expect(
                                                "Key condition should parse for valid relationship",
                                            );

                                            let sort_direction = query_args
                                                .get("sortDirection")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s == "DESC")
                                                .unwrap_or(false);

                                            let limit = query_args
                                                .get("limit")
                                                .and_then(|v| v.as_u64())
                                                .unwrap_or(100)
                                                as usize;

                                            let index_key = child_idx.index_key();

                                            // Parse and validate nextToken
                                            let last_evaluated_key =
                                                parse_next_token(&query_args, &index_key)
                                                    .ok()
                                                    .and_then(|tuple| tuple);

                                            // Parse filter if provided
                                            let parsed_filter = if let Some(filter_val) =
                                                query_args.get("filter")
                                            {
                                                parse_filter(filter_val).ok()
                                            } else {
                                                None
                                            };

                                            // Get all matching results from the index
                                            let all_results: Vec<Value> = table.query_gsi(
                                                child_idx.query_field(),
                                                &partition,
                                                sort_condition.as_ref(),
                                                sort_direction,
                                            );

                                            // Find start index based on lek
                                            let start_index =
                                                if let Some(lek_obj) = &last_evaluated_key {
                                                    let mut lek_key_fields = index_key.clone();
                                                    lek_key_fields
                                                        .extend(target_model.primary_key.clone());
                                                    let lek_key: Vec<Value> = lek_key_fields
                                                        .iter()
                                                        .map(|f| {
                                                            lek_obj
                                                                .get(f)
                                                                .cloned()
                                                                .unwrap_or(Value::Null)
                                                        })
                                                        .collect();

                                                    use std::cmp::Ordering;

                                                    all_results
                                                        .iter()
                                                        .position(|r| {
                                                            let r_key = key_of(r, &lek_key_fields);
                                                            let cmp = cmp_keys(&r_key, &lek_key);
                                                            if sort_direction {
                                                                // DESC: strictly less than lek
                                                                cmp == Ordering::Less
                                                            } else {
                                                                // ASC: strictly greater than lek
                                                                cmp == Ordering::Greater
                                                            }
                                                        })
                                                        .unwrap_or(0)
                                                } else {
                                                    0
                                                };

                                            // Apply filter and collect up to limit items
                                            let mut items = Vec::new();
                                            let mut last_record: Option<Value> = None;
                                            let mut has_more = false;
                                            for record in all_results.iter().skip(start_index) {
                                                if items.len() >= limit {
                                                    // We've collected enough items, check if there's one more
                                                    if let Some(ref filter) = parsed_filter {
                                                        if evaluate_filter_typed(filter, record) {
                                                            has_more = true;
                                                        }
                                                    } else {
                                                        has_more = true;
                                                    }
                                                    break;
                                                }

                                                // Check authorization for read
                                                if self.options.enforce_auth
                                                    && !allowed(
                                                        target_model.auth_rules(),
                                                        AuthOp::Read,
                                                        identity,
                                                        Some(record),
                                                    )
                                                {
                                                    continue; // Skip records user cannot read
                                                }

                                                if let Some(ref filter) = parsed_filter {
                                                    if !evaluate_filter_typed(filter, record) {
                                                        continue;
                                                    }
                                                }
                                                items.push(record.clone());
                                                last_record = Some(record.clone());
                                            }

                                            // Build nextToken only if we know there are more items
                                            let next_token = if has_more {
                                                last_record.as_ref().map(|r| {
                                                    let mut lek_obj = serde_json::Map::new();
                                                    for key_field in &index_key {
                                                        if let Some(v) = r.get(key_field) {
                                                            lek_obj.insert(
                                                                key_field.clone(),
                                                                v.clone(),
                                                            );
                                                        }
                                                    }
                                                    for pk_field in &target_model.primary_key {
                                                        if let Some(v) = r.get(pk_field) {
                                                            lek_obj.insert(
                                                                pk_field.clone(),
                                                                v.clone(),
                                                            );
                                                        }
                                                    }
                                                    encode_token(&Value::Object(lek_obj))
                                                })
                                            } else {
                                                None
                                            };

                                            // Apply selectionSet to each item
                                            let selected_items = items
                                                .iter()
                                                .map(|item| {
                                                    filter_selection_default(item, target_model)
                                                })
                                                .collect::<Vec<_>>();

                                            // Update result with connection
                                            if let Some(result_obj) = result.as_object_mut() {
                                                result_obj.insert(
                                                    rel_field.to_string(),
                                                    json!({
                                                        "items": selected_items,
                                                        "nextToken": next_token
                                                    }),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute a CRUD operation on the engine.
    /// Returns (data, errors) where data is the result and errors is optional list of operation errors.
    pub fn call(
        &mut self,
        model_name: &str,
        op: &str,
        args: &Value,
        identity: &Identity,
    ) -> Result<(Value, Option<Vec<Value>>)> {
        let model = self
            .contract
            .models()
            .get(model_name)
            .ok_or_else(|| Error::Internal(format!("Unknown model: {}", model_name)))?
            .clone();

        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

        // Check if this is an index query (queryField name)
        for index in model.indexes() {
            if index.query_field() == op {
                // This is an index query
                let table = self
                    .tables
                    .get_mut(model_name)
                    .ok_or_else(|| Error::Internal(format!("Table not found: {}", model_name)))?;

                // Parse key condition (validates key, partition, and sort)
                let (partition, sort_condition) = match parse_key_condition(args.get("key"), index)
                {
                    Ok(result) => result,
                    Err(msg) => {
                        return Ok((
                            Value::Null,
                            Some(vec![json!({
                                "errorType": "ValidationException",
                                "message": msg
                            })]),
                        ));
                    }
                };

                let sort_direction = args
                    .get("sortDirection")
                    .and_then(|v| v.as_str())
                    .map(|s| s == "DESC")
                    .unwrap_or(false);

                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;

                let index_key = index.index_key();

                // Parse and validate nextToken (checking index key fields)
                let last_evaluated_key = match parse_next_token(args, &index_key) {
                    Ok(key) => key,
                    Err((data, errors)) => return Ok((data, errors)),
                };

                // Parse filter if provided
                let parsed_filter = if let Some(filter_val) = args.get("filter") {
                    match parse_filter(filter_val) {
                        Ok(f) => Some(f),
                        Err(msg) => {
                            return Ok((
                                Value::Null,
                                Some(vec![json!({
                                    "errorType": "ValidationException",
                                    "message": msg
                                })]),
                            ));
                        }
                    }
                } else {
                    None
                };

                // Parse selectionSet if provided
                let parsed_selection = parse_selection_set(args);

                // Get all matching results from the index (already sorted by query_gsi)
                let all_results: Vec<Value> = table.query_gsi(
                    index.query_field(),
                    &partition,
                    sort_condition.as_ref(),
                    sort_direction,
                );

                // Find start index based on lek and sort direction
                let start_index = if let Some(lek_obj) = &last_evaluated_key {
                    // Build full lek key (index + primary)
                    let mut lek_key_fields = index_key.clone();
                    lek_key_fields.extend(model.primary_key.clone());
                    let lek_key: Vec<Value> = lek_key_fields
                        .iter()
                        .map(|f| lek_obj.get(f).cloned().unwrap_or(Value::Null))
                        .collect();

                    all_results
                        .iter()
                        .position(|r| {
                            let r_key = key_of(r, &lek_key_fields);
                            if sort_direction {
                                // DESC: strictly less than lek
                                cmp_keys(&r_key, &lek_key) == std::cmp::Ordering::Less
                            } else {
                                // ASC: strictly greater than lek
                                cmp_keys(&r_key, &lek_key) == std::cmp::Ordering::Greater
                            }
                        })
                        .unwrap_or(all_results.len())
                } else {
                    0
                };

                // Take limit records from start index, then filter them
                let end_index = std::cmp::min(start_index + limit, all_results.len());
                let taken_records = &all_results[start_index..end_index];
                let mut items = Vec::new();
                let mut last_read_key: Option<Value> = None;

                for record in taken_records {
                    // Check authorization for read
                    if self.options.enforce_auth
                        && !allowed(model.auth_rules(), AuthOp::Read, identity, Some(record))
                    {
                        continue; // Skip records user cannot read
                    }

                    // Include both index key and primary key for uniqueness
                    let mut full_key = serde_json::Map::new();
                    for k in index_key.iter() {
                        full_key.insert(k.clone(), record.get(k).cloned().unwrap_or(Value::Null));
                    }
                    for k in model.primary_key.iter() {
                        full_key.insert(k.clone(), record.get(k).cloned().unwrap_or(Value::Null));
                    }
                    last_read_key = Some(Value::Object(full_key));

                    // Apply filter
                    if let Some(ref f) = &parsed_filter {
                        if !evaluate_filter_typed(f, record) {
                            continue;
                        }
                    }

                    // Apply selectionSet filtering and resolve relationships
                    let filtered = if let Some(sel) = parsed_selection.as_ref() {
                        self.apply_selection_with_relationships(
                            record, &model, sel, args, identity,
                        )?
                    } else {
                        filter_selection_default(record, &model)
                    };
                    items.push(filtered);
                }

                // Generate nextToken if we read exactly limit items (even if no more records exist)
                let next_token = if end_index - start_index == limit {
                    last_read_key.map(|k| encode_token(&k))
                } else {
                    None
                };

                let result = json!({
                    "items": items,
                    "nextToken": next_token
                });

                return Ok((result, None));
            }
        }

        match op {
            "create" => {
                let mut record = args.clone();
                let mut errors = Vec::new();

                // Validate required fields and types
                if let Some(validation_errors) = self.validate_record(&record, &model, true) {
                    errors.extend(validation_errors);
                }

                if !errors.is_empty() {
                    return Ok((Value::Null, Some(errors)));
                }

                // Check authorization for create
                if self.options.enforce_auth
                    && !allowed(model.auth_rules(), AuthOp::Create, identity, None)
                {
                    let error = json!({
                        "message": format!("Not Authorized to access create on type {}", model_name),
                        "errorType": "Unauthorized"
                    });
                    return Ok((Value::Null, Some(vec![error])));
                }

                // Fill in auto-generated fields
                if !record.get("id").is_some_and(|v| !v.is_null()) {
                    record["id"] = Value::String(Uuid::new_v4().to_string());
                }

                // Handle createdAt: use provided value if present, otherwise generate
                // (validation catches invalid datetime formats before reaching here)
                let created_at = record
                    .get("createdAt")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .unwrap_or_else(|| Value::String(now.clone()));
                record["createdAt"] = created_at.clone();

                // Handle updatedAt: use provided value if present, otherwise use createdAt
                // (validation catches invalid datetime formats before reaching here)
                let updated_at = record
                    .get("updatedAt")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .unwrap_or_else(|| created_at.clone());
                record["updatedAt"] = updated_at;
                record["__typename"] = Value::String(model_name.to_string());

                // Handle owner fields: validate provided values and fill if needed
                if let Some(owner_value) = identity.owner_value() {
                    for owner_field in model.owner_fields() {
                        let provided_owner = record
                            .get(owner_field)
                            .filter(|v| !v.is_null())
                            .and_then(|v| v.as_str());

                        // Check if a different owner was provided (spoofing attempt)
                        if let Some(provided) = provided_owner {
                            if provided != owner_value && self.options.enforce_auth {
                                let error = json!({
                                    "message": format!("Not Authorized to access create on type {}", model_name),
                                    "errorType": "Unauthorized"
                                });
                                return Ok((Value::Null, Some(vec![error])));
                            }
                            // If enforce_auth is false, keep the provided owner (for migrations)
                        } else {
                            // Fill owner field if not provided
                            if let Some(obj) = record.as_object_mut() {
                                obj.insert(
                                    owner_field.to_string(),
                                    Value::String(owner_value.clone()),
                                );
                            }
                        }
                    }
                }

                // Populate composite sort attributes (status#createdAt format)
                self.populate_composite_sort_attributes(&mut record, &model)?;

                // Check for duplicate (conditional put)
                let (pk, sort) = self.extract_pk_parts(&record, &model);
                let table = self
                    .tables
                    .get_mut(model_name)
                    .ok_or_else(|| Error::Internal(format!("Table not found: {}", model_name)))?;

                if table.get(&pk, sort.as_deref()).is_some() {
                    let error = json!({
                        "message": "An item with this id already exists",
                        "errorType": "DynamoDB:ConditionalCheckFailedException"
                    });
                    return Ok((Value::Null, Some(vec![error])));
                }

                // Put the record
                table.put(record.clone());

                Ok((record, None))
            }
            "get" => {
                let (pk, sort) = self.extract_pk_parts(args, &model);

                let table = self
                    .tables
                    .get(model_name)
                    .ok_or_else(|| Error::Internal(format!("Table not found: {}", model_name)))?;

                if let Some(record) = table.get(&pk, sort.as_deref()) {
                    // Check authorization
                    if self.options.enforce_auth
                        && !allowed(model.auth_rules(), AuthOp::Read, identity, Some(&record))
                    {
                        let error = json!({
                            "message": format!("Not Authorized to access read on type {}", model_name),
                            "errorType": "Unauthorized"
                        });
                        return Ok((Value::Null, Some(vec![error])));
                    }

                    // Apply selectionSet filtering and resolve relationships
                    let selection = parse_selection_set(args);
                    let mut result = if let Some(sel) = selection.clone() {
                        self.apply_selection_with_relationships(
                            &record, &model, &sel, args, identity,
                        )?
                    } else {
                        filter_selection_default(&record, &model)
                    };

                    // Post-process to populate hasMany relationships
                    if let Some(sel) = selection {
                        result = self.populate_has_many_relationships(
                            result, &record, &model, &sel, args, identity,
                        )?;
                    }

                    Ok((result, None))
                } else {
                    Ok((Value::Null, None))
                }
            }
            "update" => {
                let mut errors = Vec::new();
                let (pk, sort) = self.extract_pk_parts(args, &model);

                // Get existing record
                let table = self
                    .tables
                    .get(model_name)
                    .ok_or_else(|| Error::Internal(format!("Table not found: {}", model_name)))?;

                let existing = table.get(&pk, sort.as_deref());
                if existing.is_none() {
                    let error = json!({
                        "message": "An item with this id does not exist",
                        "errorType": "DynamoDB:ConditionalCheckFailedException"
                    });
                    return Ok((Value::Null, Some(vec![error])));
                }

                let mut record = existing.unwrap();

                // Check authorization
                if self.options.enforce_auth
                    && !allowed(model.auth_rules(), AuthOp::Update, identity, Some(&record))
                {
                    let error = json!({
                        "message": format!("Not Authorized to access update on type {}", model_name),
                        "errorType": "Unauthorized"
                    });
                    return Ok((Value::Null, Some(vec![error])));
                }

                // Apply partial updates
                if let Some(obj) = args.as_object() {
                    for (key, value) in obj {
                        if key == "id" || key.starts_with("__") {
                            continue; // Skip id and system fields
                        }
                        if value.is_null() {
                            // Remove the field
                            if let Some(rec_obj) = record.as_object_mut() {
                                rec_obj.remove(key);
                            }
                        } else {
                            record[key] = value.clone();
                        }
                    }
                }

                // Update timestamp: use provided value if present, otherwise use now
                // (validation catches invalid datetime formats before reaching here)
                let updated_at = args
                    .get("updatedAt")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .unwrap_or_else(|| Value::String(now.clone()));
                record["updatedAt"] = updated_at;

                // Revalidate the updated record
                if let Some(validation_errors) = self.validate_record(&record, &model, false) {
                    errors.extend(validation_errors);
                }

                if !errors.is_empty() {
                    return Ok((Value::Null, Some(errors)));
                }

                // Repopulate composite sort attributes
                self.populate_composite_sort_attributes(&mut record, &model)?;

                // Put the updated record
                let table = self
                    .tables
                    .get_mut(model_name)
                    .ok_or_else(|| Error::Internal(format!("Table not found: {}", model_name)))?;
                table.put(record.clone());

                Ok((record, None))
            }
            "delete" => {
                let (pk, sort) = self.extract_pk_parts(args, &model);

                let table = self
                    .tables
                    .get_mut(model_name)
                    .ok_or_else(|| Error::Internal(format!("Table not found: {}", model_name)))?;

                if let Some(record) = table.get(&pk, sort.as_deref()) {
                    // Check authorization
                    if self.options.enforce_auth
                        && !allowed(model.auth_rules(), AuthOp::Delete, identity, Some(&record))
                    {
                        let error = json!({
                            "message": format!("Not Authorized to access delete on type {}", model_name),
                            "errorType": "Unauthorized"
                        });
                        return Ok((Value::Null, Some(vec![error])));
                    }

                    table.delete(&pk, sort.as_deref());
                    Ok((record, None))
                } else {
                    let error = json!({
                        "message": "An item with this id does not exist",
                        "errorType": "DynamoDB:ConditionalCheckFailedException"
                    });
                    Ok((Value::Null, Some(vec![error])))
                }
            }
            "list" => {
                let table = self
                    .tables
                    .get_mut(model_name)
                    .ok_or_else(|| Error::Internal(format!("Table not found: {}", model_name)))?;

                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;

                let primary_key = &model.primary_key;

                // Parse and validate nextToken
                let last_evaluated_key = match parse_next_token(args, primary_key) {
                    Ok(key) => key,
                    Err((data, errors)) => return Ok((data, errors)),
                };

                // Parse filter if provided
                let parsed_filter = if let Some(filter_val) = args.get("filter") {
                    match parse_filter(filter_val) {
                        Ok(f) => Some(f),
                        Err(msg) => {
                            return Ok((
                                Value::Null,
                                Some(vec![json!({
                                    "errorType": "ValidationException",
                                    "message": msg
                                })]),
                            ));
                        }
                    }
                } else {
                    None
                };

                // Parse selectionSet if provided
                let parsed_selection = parse_selection_set(args);

                // Collect and sort all records by primary key
                let mut records = table.scan();
                records.sort_by(|a, b| {
                    let a_key = key_of(a, primary_key);
                    let b_key = key_of(b, primary_key);
                    cmp_keys(&a_key, &b_key)
                });

                // Find start index: first record with key > lek
                let start_index = if let Some(lek_obj) = &last_evaluated_key {
                    let lek_key: Vec<Value> = primary_key
                        .iter()
                        .map(|f| lek_obj.get(f).cloned().unwrap_or(Value::Null))
                        .collect();
                    records
                        .iter()
                        .position(|r| {
                            cmp_keys(&key_of(r, primary_key), &lek_key)
                                == std::cmp::Ordering::Greater
                        })
                        .unwrap_or(records.len())
                } else {
                    0
                };

                // Take limit records from start index, then filter them
                let end_index = std::cmp::min(start_index + limit, records.len());
                let taken_records = &records[start_index..end_index];
                let mut items = Vec::new();
                let mut last_read_key: Option<Value> = None;

                for record in taken_records {
                    last_read_key = Some(json!(primary_key
                        .iter()
                        .map(|k| { (k.clone(), record.get(k).cloned().unwrap_or(Value::Null)) })
                        .collect::<serde_json::Map<String, Value>>()));

                    // Check authorization for read
                    // Check authorization for read
                    if self.options.enforce_auth
                        && !allowed(model.auth_rules(), AuthOp::Read, identity, Some(record))
                    {
                        continue;
                    }

                    // Apply filter
                    if let Some(ref f) = &parsed_filter {
                        if !evaluate_filter_typed(f, record) {
                            continue;
                        }
                    }

                    // Apply selectionSet filtering and resolve relationships
                    let filtered = if let Some(sel) = parsed_selection.as_ref() {
                        self.apply_selection_with_relationships(
                            record, &model, sel, args, identity,
                        )?
                    } else {
                        filter_selection_default(record, &model)
                    };
                    items.push(filtered);
                }

                // Generate nextToken if we read exactly limit items (even if no more records exist)
                let next_token = if end_index - start_index == limit {
                    last_read_key.map(|k| encode_token(&k))
                } else {
                    None
                };

                let result = json!({
                    "items": items,
                    "nextToken": next_token
                });

                Ok((result, None))
            }
            _ => Err(Error::Internal(format!("Unknown operation: {}", op))),
        }
    }

    /// Validate a record against the model schema.
    fn validate_record(
        &self,
        record: &Value,
        model: &Model,
        is_create: bool,
    ) -> Option<Vec<Value>> {
        let mut errors = Vec::new();

        // Check required fields
        for (field_name, field) in &model.fields {
            // Skip relationship fields and system fields
            if field.kind == "model" || field_name.starts_with("__") {
                continue;
            }

            // Check required fields
            if field.is_required && is_create {
                let value = record.get(field_name);
                if value.is_none() || value.is_some_and(|v| v.is_null()) {
                    // Skip auto-filled fields
                    if field_name != "id"
                        && field_name != "createdAt"
                        && field_name != "updatedAt"
                        && field_name != "owner"
                    {
                        errors.push(json!({
                            "message": format!("Field '{}' is required", field_name),
                            "errorType": "ValidationException"
                        }));
                    }
                }
            }

            // Validate enum values
            if field.kind == "enum" {
                if let Some(value) = record.get(field_name) {
                    if !value.is_null() {
                        if let Some(value_str) = value.as_str() {
                            // Get the enum values from the contract
                            if let Some(enum_values) = self.get_enum_values(&field.field_type) {
                                if !enum_values.contains(&value_str.to_string()) {
                                    errors.push(json!({
                                        "message": format!(
                                            "Field '{}' must be one of enum values {:?}",
                                            field_name, enum_values
                                        ),
                                        "errorType": "ValidationException"
                                    }));
                                }
                            }
                        } else {
                            errors.push(json!({
                                "message": format!("Field '{}' must be a string for enum type", field_name),
                                "errorType": "ValidationException"
                            }));
                        }
                    }
                }
            }

            // Validate AWS scalar types
            if let Some(value) = record.get(field_name) {
                if !value.is_null() {
                    #[allow(clippy::collapsible_match)]
                    match field.field_type.as_str() {
                        "AWSDateTime" => {
                            if let Some(dt_str) = value.as_str() {
                                if chrono::DateTime::parse_from_rfc3339(dt_str).is_err() {
                                    errors.push(json!({
                                        "message": format!(
                                            "Field '{}' must be a valid AWSDateTime (RFC 3339 datetime)",
                                            field_name
                                        ),
                                        "errorType": "ValidationException"
                                    }));
                                }
                            } else {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be a string", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "AWSDate" => {
                            if let Some(date_str) = value.as_str() {
                                if date_str.chars().count() != 10
                                    || date_str.chars().nth(4) != Some('-')
                                    || date_str.chars().nth(7) != Some('-')
                                {
                                    errors.push(json!({
                                        "message": format!(
                                            "Field '{}' must be a valid AWSDate (YYYY-MM-DD)",
                                            field_name
                                        ),
                                        "errorType": "ValidationException"
                                    }));
                                }
                            } else {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be a string", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "AWSJSON" => {
                            if let Some(json_str) = value.as_str() {
                                if serde_json::from_str::<Value>(json_str).is_err() {
                                    errors.push(json!({
                                        "message": format!(
                                            "Field '{}' must be a valid AWSJSON string",
                                            field_name
                                        ),
                                        "errorType": "ValidationException"
                                    }));
                                }
                            } else {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be a string", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "AWSEmail" => {
                            if let Some(email_str) = value.as_str() {
                                let at_count = email_str.chars().filter(|c| *c == '@').count();
                                let parts: Vec<&str> = email_str.split('@').collect();
                                if at_count != 1 || parts[0].is_empty() || parts[1].is_empty() {
                                    errors.push(json!({
                                        "message": format!("Field '{}' must be a valid AWSEmail", field_name),
                                        "errorType": "ValidationException"
                                    }));
                                }
                            } else {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be a string", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "AWSURL" => {
                            if let Some(url_str) = value.as_str() {
                                if url::Url::parse(url_str).is_err() {
                                    errors.push(json!({
                                        "message": format!("Field '{}' must be a valid AWSURL", field_name),
                                        "errorType": "ValidationException"
                                    }));
                                }
                            } else {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be a string", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "Int" => {
                            if !value.is_i64() && !value.is_u64() {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be an integer", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "Float" => {
                            if !value.is_f64() && !value.is_i64() && !value.is_u64() {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be a number", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "Boolean" => {
                            if !value.is_boolean() {
                                errors.push(json!({
                                    "message": format!("Field '{}' must be a boolean", field_name),
                                    "errorType": "ValidationException"
                                }));
                            }
                        }
                        "String" if !value.is_string() => {
                            errors.push(json!({
                                "message": format!("Field '{}' must be a string", field_name),
                                "errorType": "ValidationException"
                            }));
                        }
                        "String" => {}
                        _ => {}
                    }
                }
            }
        }

        if errors.is_empty() {
            None
        } else {
            Some(errors)
        }
    }

    /// Get enum values from the contract.
    fn get_enum_values(&self, enum_name: &str) -> Option<Vec<String>> {
        self.contract
            .enums()
            .get(enum_name)
            .map(|e| e.values().to_vec())
    }

    /// Extract partition key and sort key from a record.
    fn extract_pk_parts(&self, record: &Value, model: &Model) -> (String, Option<String>) {
        let pk = model.primary_key();
        if pk.len() == 1 {
            let pk_value = record
                .get(&pk[0])
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            (pk_value, None)
        } else {
            // Composite key: partition is first, sort is rest
            let partition = record
                .get(&pk[0])
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let sort_parts: Vec<String> = pk[1..]
                .iter()
                .map(|key| {
                    record
                        .get(key)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                })
                .collect();
            // sort_parts is guaranteed to have at least one element since pk[1..] is non-empty
            let sort = Some(sort_parts.join("#"));
            (partition, sort)
        }
    }

    /// Populate composite sort attributes (e.g., status#createdAt).
    fn populate_composite_sort_attributes(&self, record: &mut Value, model: &Model) -> Result<()> {
        // Check primary key for composite sort
        if model.primary_key().len() > 2 {
            let sort_key_name = model.primary_key()[1..].join("#");
            let sort_parts: Vec<String> = model.primary_key()[1..]
                .iter()
                .map(|key| {
                    record
                        .get(key)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                })
                .collect();
            if !sort_parts.is_empty() {
                record[&sort_key_name] = Value::String(sort_parts.join("#"));
            }
        }

        // Check indexes for composite sort attributes
        for index in model.indexes() {
            if index.sort_fields().len() > 1 {
                let sort_attr_name = index.sort_fields().join("#");
                let sort_parts: Vec<String> = index
                    .sort_fields()
                    .iter()
                    .map(|key| {
                        record
                            .get(key)
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_default()
                    })
                    .collect();
                if !sort_parts.is_empty() {
                    record[&sort_attr_name] = Value::String(sort_parts.join("#"));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_from_json() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "User": {
                    "name": "User",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let contract = Contract::from_json(json).expect("Failed to parse contract");
        assert!(contract.models().contains_key("User"));
    }

    #[test]
    fn test_invalid_json() {
        let json = "not valid json";
        let result = Contract::from_json(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_missing_required() {
        let json = r#"{"version": "test"}"#;
        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(
                msg.contains("Missing required field")
                    || msg.contains("required")
                    || msg.contains("Required")
            );
        }
    }

    #[test]
    fn test_engine_accessors() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "TestModel": {
                    "name": "TestModel",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        },
                        "name": {
                            "name": "name",
                            "type": "String",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [
                        {
                            "name": "testIndex",
                            "queryField": "testQuery",
                            "partitionField": "name",
                            "sortFields": ["id"]
                        }
                    ],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {
                "Status": ["ACTIVE", "INACTIVE"]
            },
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let contract = Contract::from_json(json).expect("Failed to parse contract");
        let engine = Engine::open(
            None,
            contract.clone(),
            EngineOptions {
                enforce_auth: false,
            },
        )
        .expect("Failed to open engine");
        let description = engine.describe();
        assert!(description.get("tables").is_some());

        // Test model accessors
        let model = contract.models().get("TestModel").expect("Model not found");
        assert_eq!(model.name(), "TestModel");
        assert_eq!(model.primary_key(), ["id"]);
        assert!(!model.indexes().is_empty());
        assert!(model.relationships().is_empty());

        // Test index accessors
        let index = &model.indexes()[0];
        assert_eq!(index.name(), "testIndex");
        assert_eq!(index.query_field(), "testQuery");
        assert_eq!(index.partition_field(), "name");
        assert_eq!(index.sort_fields(), ["id"]);

        // Test enum accessors
        let enums = contract.enums();
        assert!(enums.contains_key("Status"));

        // Test custom type accessors
        let custom_types = contract.custom_types();
        assert!(custom_types.is_empty());
    }

    #[test]
    fn test_relationship_accessors() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "Parent": {
                    "name": "Parent",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [],
                    "relationships": [
                        {
                            "field": "children",
                            "kind": "hasMany",
                            "target": "Child",
                            "references": "parentId",
                            "childIndex": "childrenByParent",
                            "implicit": true
                        }
                    ],
                    "authRules": [],
                    "ownerFields": []
                },
                "Child": {
                    "name": "Child",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        },
                        "parentId": {
                            "name": "parentId",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let contract = Contract::from_json(json).expect("Failed to parse contract");
        let parent_model = contract
            .models()
            .get("Parent")
            .expect("Parent model not found");

        let rel = &parent_model.relationships()[0];
        assert_eq!(rel.field(), "children");
        assert_eq!(rel.kind(), "hasMany");
        assert_eq!(rel.target(), "Child");
        assert_eq!(rel.references(), "parentId");
        assert_eq!(rel.child_index(), Some("childrenByParent"));
        assert!(rel.is_implicit());
    }

    #[test]
    fn test_validation_model_missing_field() {
        let json = r#"{
            "version": "test",
            "models": {
                "BadModel": {
                    "name": "BadModel",
                    "fields": {},
                    "primaryKey": ["id"]
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(
                msg.contains("missing")
                    || msg.contains("required")
                    || msg.contains("does not")
                    || msg.contains("Required")
            );
        }
    }

    #[test]
    fn test_enum_validation_array_only() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "TestModel": {
                    "name": "TestModel",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {
                "BadEnum": {}
            },
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(
                msg.contains("must be an array") || msg.contains("Type") || msg.contains("type")
            );
        }
    }

    #[test]
    fn test_primarykey_field_must_exist() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "TestModel": {
                    "name": "TestModel",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["missingField"],
                    "indexes": [],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(msg.contains("primaryKey") && msg.contains("does not exist"));
        }
    }

    #[test]
    fn test_index_field_must_exist() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "TestModel": {
                    "name": "TestModel",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [
                        {
                            "name": "testIndex",
                            "queryField": "testQuery",
                            "partitionField": "missingField",
                            "sortFields": []
                        }
                    ],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(msg.contains("partition field") && msg.contains("does not exist"));
        }
    }

    #[test]
    fn test_relationship_target_must_exist() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "TestModel": {
                    "name": "TestModel",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [],
                    "relationships": [
                        {
                            "field": "other",
                            "kind": "belongsTo",
                            "target": "NonExistentModel",
                            "references": "otherId"
                        }
                    ],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(msg.contains("non-existent model"));
        }
    }

    #[test]
    fn test_field_enum_type_must_exist() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "TestModel": {
                    "name": "TestModel",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        },
                        "status": {
                            "name": "status",
                            "type": "UndefinedEnum",
                            "isRequired": false,
                            "isArray": false,
                            "kind": "enum"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(msg.contains("undefined enum"));
        }
    }

    #[test]
    fn test_field_custom_type_must_exist() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "TestModel": {
                    "name": "TestModel",
                    "fields": {
                        "id": {
                            "name": "id",
                            "type": "ID",
                            "isRequired": true,
                            "isArray": false,
                            "kind": "scalar"
                        },
                        "data": {
                            "name": "data",
                            "type": "UndefinedType",
                            "isRequired": false,
                            "isArray": false,
                            "kind": "customType"
                        }
                    },
                    "primaryKey": ["id"],
                    "indexes": [],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let result = Contract::from_json(json);
        assert!(result.is_err());
        if let Err(Error::Validation(msg)) = result {
            assert!(msg.contains("undefined customType"));
        }
    }

    #[test]
    fn test_composite_key_with_three_fields() {
        let json = r#"{
            "version": "test_v1",
            "models": {
                "CompositeModel": {
                    "name": "CompositeModel",
                    "fields": {
                        "pk": {"name": "pk", "type": "ID", "isRequired": true, "isArray": false, "kind": "scalar"},
                        "sk1": {"name": "sk1", "type": "String", "isRequired": true, "isArray": false, "kind": "scalar"},
                        "sk2": {"name": "sk2", "type": "String", "isRequired": true, "isArray": false, "kind": "scalar"}
                    },
                    "primaryKey": ["pk", "sk1", "sk2"],
                    "indexes": [],
                    "relationships": [],
                    "authRules": [],
                    "ownerFields": []
                }
            },
            "enums": {},
            "customTypes": {},
            "authRules": {},
            "storage": { "paths": [] }
        }"#;

        let contract = Contract::from_json(json).expect("Failed to parse contract");
        let engine = Engine::open(
            None,
            contract,
            EngineOptions {
                enforce_auth: false,
            },
        )
        .expect("Failed to open engine");
        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();
        let model = &tables[0];

        assert_eq!(
            model.get("name").unwrap().as_str().unwrap(),
            "CompositeModel"
        );
        assert_eq!(model.get("partitionKey").unwrap().as_str().unwrap(), "pk");
        assert_eq!(model.get("sortKey").unwrap().as_str().unwrap(), "sk1#sk2");
    }
}
