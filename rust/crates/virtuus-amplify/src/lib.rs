//! Virtuus amplify: Amplify Gen2-shaped operations on the Virtuus storage engine.
//!
//! V8b populates composite sort attributes on write with v1#v2 format.

use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use thiserror::Error;

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

/// A data contract describing models, indexes, and relationships.
#[derive(Debug, Clone)]
pub struct Contract {
    #[allow(dead_code)]
    version: String,
    models: HashMap<String, Model>,
    enums: HashMap<String, Enum>,
    custom_types: HashMap<String, CustomType>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Model {
    name: String,
    fields: HashMap<String, Field>,
    primary_key: Vec<String>,
    indexes: Vec<Index>,
    relationships: Vec<Relationship>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Field {
    name: String,
    field_type: String,
    is_required: bool,
    is_array: bool,
    kind: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Index {
    name: String,
    query_field: String,
    partition_field: String,
    sort_fields: Vec<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Relationship {
    field: String,
    kind: String,
    target: String,
    references: String,
    child_index: Option<String>,
    implicit: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Enum {
    name: String,
    values: Vec<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CustomType {
    name: String,
}

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
                custom_types.insert(name.clone(), CustomType { name: name.clone() });
            }
        }

        // Semantic validation
        Self::validate_semantics(&models, &enums, &custom_types)?;

        let version = value
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        Ok(Contract {
            version,
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

        Ok(Model {
            name,
            fields,
            primary_key,
            indexes,
            relationships,
        })
    }

    fn parse_field(value: &Value) -> Result<Field> {
        let name = value
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Validation("Field missing name".to_string()))?
            .to_string();

        let field_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("String")
            .to_string();

        let is_required = value
            .get("isRequired")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let is_array = value
            .get("isArray")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let kind = value
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("scalar")
            .to_string();

        Ok(Field {
            name,
            field_type,
            is_required,
            is_array,
            kind,
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

    fn parse_enum(value: &Value, name: &str) -> Result<Enum> {
        // Enums are stored as arrays of strings (the enum name is the map key)
        let values = value
            .as_array()
            .ok_or_else(|| {
                Error::Validation(format!(
                    "Enum '{}' must be an array of strings, not an object or other type",
                    name
                ))
            })?
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();

        Ok(Enum {
            name: name.to_string(),
            values,
        })
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
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the primary key fields.
    #[allow(dead_code)]
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Get the indexes.
    #[allow(dead_code)]
    pub fn indexes(&self) -> &[Index] {
        &self.indexes
    }

    /// Get the relationships.
    #[allow(dead_code)]
    pub fn relationships(&self) -> &[Relationship] {
        &self.relationships
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
}

impl Relationship {
    /// Get the field name.
    #[allow(dead_code)]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Get the relationship kind (hasMany, belongsTo, etc).
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn is_implicit(&self) -> bool {
        self.implicit
    }

    /// Get the child index name for implicit hasMany relationships.
    pub fn child_index(&self) -> Option<&str> {
        self.child_index.as_deref()
    }
}

/// Engine options.
pub struct EngineOptions {
    /// Whether to enforce authorization rules.
    pub enforce_auth: bool,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions { enforce_auth: true }
    }
}

/// Storage-less or file-backed engine for Amplify-shaped operations.
#[derive(Debug)]
pub struct Engine {
    contract: Contract,
    #[allow(dead_code)]
    tables: HashMap<String, virtuus::table::Table>,
}

impl Engine {
    /// Open or create an engine with the given contract.
    pub fn open(
        directory: Option<PathBuf>,
        contract: Contract,
        _options: EngineOptions,
    ) -> Result<Self> {
        // Create one table per model
        let mut tables_temp: HashMap<String, virtuus::table::Table> = HashMap::new();
        for (model_name, model) in contract.models() {
            let primary_key = &model.primary_key();

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

            let mut table = virtuus::table::Table::new(
                model_name,
                pk,
                partition,
                sort,
                directory.clone(),
                virtuus::table::ValidationMode::Error,
            );

            // Add GSIs for explicit indexes
            for index in model.indexes() {
                let index_sort_key_name: Option<String>;
                let sort_key = if index.sort_fields.is_empty() {
                    None
                } else if index.sort_fields.len() == 1 {
                    Some(index.sort_fields[0].as_str())
                } else {
                    // Multiple sort fields: use joined name as the attribute name
                    index_sort_key_name = Some(index.sort_fields.join("#"));
                    index_sort_key_name.as_deref()
                };
                table.add_gsi(index.query_field(), index.partition_field(), sort_key);
            }

            tables_temp.insert(model_name.to_string(), table);
        }

        // Add implicit indexes from hasMany relationships
        // These indexes are on the child table, keyed by the foreign key
        for model in contract.models().values() {
            for rel in model.relationships() {
                if rel.kind == "hasMany" && rel.implicit {
                    // Add the index to the child table (referenced model)
                    if let Some(table) = tables_temp.get_mut(rel.target()) {
                        if let Some(child_index) = rel.child_index() {
                            table.add_gsi(child_index, rel.references(), None);
                        }
                    }
                }
            }
        }

        Ok(Engine {
            contract,
            tables: tables_temp,
        })
    }

    /// Describe the engine's tables and indexes.
    pub fn describe(&self) -> Value {
        let mut result = serde_json::Map::new();
        let mut tables_array = Vec::new();

        // Use BTreeMap for deterministic ordering
        let sorted_models: BTreeMap<_, _> = self.contract.models().iter().collect();

        for (model_name, model) in &sorted_models {
            let mut table_info = serde_json::Map::new();
            table_info.insert("name".to_string(), Value::String((*model_name).clone()));

            let primary_key = &model.primary_key();
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

            // Indexes - sort by name for determinism
            let mut indexes_array = Vec::new();
            let mut sorted_indexes = model.indexes().to_vec();
            sorted_indexes.sort_by(|a, b| a.name().cmp(b.name()));

            for index in sorted_indexes {
                let mut idx_info = serde_json::Map::new();
                idx_info.insert("name".to_string(), Value::String(index.name().to_string()));
                idx_info.insert(
                    "queryField".to_string(),
                    Value::String(index.query_field().to_string()),
                );
                idx_info.insert(
                    "partitionField".to_string(),
                    Value::String(index.partition_field().to_string()),
                );
                idx_info.insert(
                    "sortFields".to_string(),
                    Value::Array(
                        index
                            .sort_fields()
                            .iter()
                            .map(|s| Value::String(s.clone()))
                            .collect(),
                    ),
                );
                indexes_array.push(Value::Object(idx_info));
            }

            // Implicit indexes from OTHER models' hasMany relationships that reference this model
            for (other_model_name, other_model) in &sorted_models {
                for rel in other_model.relationships() {
                    if rel.kind == "hasMany" && rel.implicit && rel.target() == model_name.as_str()
                    {
                        if let Some(child_index) = rel.child_index() {
                            let mut idx_info = serde_json::Map::new();
                            idx_info
                                .insert("name".to_string(), Value::String(child_index.to_string()));
                            idx_info.insert(
                                "queryField".to_string(),
                                Value::String(child_index.to_string()),
                            );
                            idx_info.insert(
                                "partitionField".to_string(),
                                Value::String(rel.references().to_string()),
                            );
                            idx_info.insert("implicit".to_string(), Value::Bool(true));
                            idx_info.insert(
                                "from".to_string(),
                                Value::String((*other_model_name).clone()),
                            );
                            indexes_array.push(Value::Object(idx_info));
                        }
                    }
                }
            }

            table_info.insert("indexes".to_string(), Value::Array(indexes_array));
            tables_array.push(Value::Object(table_info));
        }

        result.insert("tables".to_string(), Value::Array(tables_array));
        Value::Object(result)
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
            eprintln!("Actual error message: {}", msg);
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
        let engine = Engine::open(None, contract.clone(), EngineOptions::default())
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
            eprintln!("Enum error: {}", msg);
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
}
