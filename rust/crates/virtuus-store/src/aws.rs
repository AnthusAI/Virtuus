//! AWS adapters preserve canonical object bytes and keep catalogs disposable.

use crate::{
    validate_fields, validate_limit, validate_path, validate_prefix, Card, Catalog, CatalogPage,
    CatalogQuery, ObjectPage, ObjectStore, Result, Scope, StoreError, StoredObject, WriteCondition,
};
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::error::ProvideErrorMetadata;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Canonical S3 objects under `<tenant>/<project>/`, with separate control state.
#[derive(Clone)]
pub struct S3Store {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3Store {
    /// Use an already configured client; callers control endpoints and credentials.
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    async fn read(&self, key: String, max_bytes: Option<usize>) -> Result<Option<StoredObject>> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|e| matches!(e.code(), Some("NoSuchKey"))) =>
            {
                return Ok(None)
            }
            Err(error) => return Err(StoreError::Unavailable(error.to_string())),
        };
        let revision = output
            .e_tag()
            .ok_or_else(|| StoreError::Unavailable("S3 object has no ETag".into()))?
            .to_owned();
        let operation_id = output
            .metadata()
            .and_then(|m| m.get("operation-id"))
            .cloned();
        let content_type = output
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_owned();
        if let Some(limit) = max_bytes {
            if output
                .content_length()
                .is_some_and(|length| length >= 0 && length as u64 > limit as u64)
            {
                return Err(StoreError::TooLarge { limit });
            }
        }
        let mut body = output.body;
        let mut bytes = Vec::new();
        while let Some(chunk) = body
            .try_next()
            .await
            .map_err(|error| StoreError::Unavailable(error.to_string()))?
        {
            if let Some(limit) = max_bytes {
                if bytes
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|length| length > limit)
                {
                    return Err(StoreError::TooLarge { limit });
                }
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Some(StoredObject {
            bytes,
            revision,
            operation_id,
            content_type,
        }))
    }

    async fn write(
        &self,
        key: String,
        bytes: &[u8],
        content_type: &str,
        condition: WriteCondition,
        operation_id: Option<&str>,
    ) -> Result<StoredObject> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.to_vec().into())
            .content_type(content_type);
        request = match condition {
            WriteCondition::Absent => request.if_none_match("*"),
            WriteCondition::Revision(revision) => request.if_match(revision),
        };
        if let Some(operation_id) = operation_id {
            request = request.metadata("operation-id", operation_id);
        }
        let output = request
            .customize()
            .config_override(aws_sdk_s3::config::Builder::new().retry_config(
                aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
            ))
            .send()
            .await
            .map_err(|error| {
                if error.as_service_error().is_some_and(|e| {
                    matches!(
                        e.code(),
                        Some("PreconditionFailed" | "ConditionalRequestConflict" | "NoSuchKey")
                    )
                }) {
                    StoreError::Conflict
                } else {
                    if error.as_service_error().is_some_and(|e| {
                        matches!(
                            e.code(),
                            Some(
                                "AccessDenied"
                                    | "InvalidRequest"
                                    | "InvalidArgument"
                                    | "NoSuchBucket"
                            )
                        )
                    }) {
                        StoreError::Unavailable(error.to_string())
                    } else {
                        StoreError::Uncertain(error.to_string())
                    }
                }
            })?;
        let revision = output
            .e_tag()
            .ok_or_else(|| StoreError::Uncertain("S3 accepted the object without an ETag".into()))?
            .to_owned();
        Ok(StoredObject {
            bytes: bytes.to_vec(),
            revision,
            operation_id: operation_id.map(str::to_owned),
            content_type: content_type.to_owned(),
        })
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<StoredObject>> {
        validate_path(path)?;
        self.read(format!("{}/{path}", scope.namespace()), None)
            .await
    }

    async fn get_bounded(
        &self,
        scope: &Scope,
        path: &str,
        max_bytes: usize,
    ) -> Result<Option<StoredObject>> {
        validate_path(path)?;
        if max_bytes == 0 {
            return Err(StoreError::Invalid("read budget must be positive".into()));
        }
        self.read(format!("{}/{path}", scope.namespace()), Some(max_bytes))
            .await
    }

    async fn list(
        &self,
        scope: &Scope,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ObjectPage> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        if let Some(cursor) = cursor {
            validate_path(cursor)?;
            if !cursor.starts_with(prefix) {
                return Err(StoreError::Invalid(
                    "cursor does not belong to prefix".into(),
                ));
            }
        }
        let root = format!("{}/", scope.namespace());
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("{root}{prefix}"))
            .set_start_after(cursor.map(|p| format!("{root}{p}")))
            .max_keys(limit as i32)
            .send()
            .await
            .map_err(|error| StoreError::Unavailable(error.to_string()))?;
        let paths = output
            .contents()
            .iter()
            .map(|o| {
                o.key()
                    .and_then(|k| k.strip_prefix(&root))
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        StoreError::Unavailable(
                            "S3 returned an object outside the requested scope".into(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = if output.is_truncated() == Some(true) {
            paths.last().cloned()
        } else {
            None
        };
        Ok(ObjectPage { paths, next_cursor })
    }

    async fn put(
        &self,
        scope: &Scope,
        path: &str,
        bytes: &[u8],
        content_type: &str,
        condition: WriteCondition,
        operation_id: Option<&str>,
    ) -> Result<StoredObject> {
        validate_path(path)?;
        self.write(
            format!("{}/{path}", scope.namespace()),
            bytes,
            content_type,
            condition,
            operation_id,
        )
        .await
    }

    async fn get_control(&self, scope: &Scope, path: &str) -> Result<Option<StoredObject>> {
        validate_path(path)?;
        self.read(format!("_virtuus/{}/{path}", scope.namespace()), None)
            .await
    }

    async fn delete(&self, scope: &Scope, path: &str, expected_revision: &str) -> Result<()> {
        validate_path(path)?;
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(format!("{}/{path}", scope.namespace()))
            .if_match(expected_revision)
            .customize()
            .config_override(aws_sdk_s3::config::Builder::new().retry_config(
                aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
            ))
            .send()
            .await
            .map_err(|error| {
                if error.as_service_error().is_some_and(|e| {
                    matches!(
                        e.code(),
                        Some(
                            "PreconditionFailed"
                                | "ConditionalRequestConflict"
                                | "NoSuchKey"
                                | "NotFound"
                        )
                    )
                }) {
                    StoreError::Conflict
                } else if error.as_service_error().is_some_and(|e| {
                    matches!(
                        e.code(),
                        Some(
                            "AccessDenied" | "InvalidRequest" | "InvalidArgument" | "NoSuchBucket"
                        )
                    )
                }) {
                    StoreError::Unavailable(error.to_string())
                } else {
                    StoreError::Uncertain(error.to_string())
                }
            })?;
        Ok(())
    }

    async fn put_control(
        &self,
        scope: &Scope,
        path: &str,
        bytes: &[u8],
        condition: WriteCondition,
    ) -> Result<StoredObject> {
        validate_path(path)?;
        self.write(
            format!("_virtuus/{}/{path}", scope.namespace()),
            bytes,
            "application/json",
            condition,
            None,
        )
        .await
    }
}

/// DynamoDB card catalog. Every operation includes its authorized project scope.
#[derive(Clone)]
pub struct DynamoCatalog {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryStream {
    index: Option<&'static str>,
    partition_attribute: &'static str,
    partition_value: String,
}

fn field_key(scope: &Scope, field: &str, value: &str) -> String {
    format!("{}#{field}#{}:{value}", scope.namespace(), value.len())
}

fn combined_key(scope: &Scope, kind: &str, status: &str) -> String {
    format!(
        "{}#status#{}:{status}",
        field_key(scope, "type", kind),
        status.len()
    )
}

fn index_value(value: Option<&serde_json::Value>) -> Option<&str> {
    value
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 512)
}

fn query_streams(scope: &Scope, query: &CatalogQuery) -> Result<Vec<QueryStream>> {
    let types = query.issue_types.iter().collect::<BTreeSet<_>>();
    let statuses = query.statuses.iter().collect::<BTreeSet<_>>();
    if types.len().max(1).saturating_mul(statuses.len().max(1)) > 32
        || types
            .iter()
            .chain(statuses.iter())
            .any(|value| value.is_empty() || value.len() > 512)
    {
        return Err(StoreError::Invalid(
            "catalog selection permits at most 32 query streams and 512-byte values".into(),
        ));
    }
    let mut streams = Vec::new();
    match (types.is_empty(), statuses.is_empty()) {
        (true, true) => streams.push(QueryStream {
            index: None,
            partition_attribute: "scope",
            partition_value: scope.namespace(),
        }),
        (false, true) => {
            for kind in types {
                streams.push(QueryStream {
                    index: Some("by_type"),
                    partition_attribute: "type_scope",
                    partition_value: field_key(scope, "type", kind),
                });
            }
        }
        (true, false) => {
            for status in statuses {
                streams.push(QueryStream {
                    index: Some("by_status"),
                    partition_attribute: "status_scope",
                    partition_value: field_key(scope, "status", status),
                });
            }
        }
        (false, false) => {
            for kind in types {
                for status in &statuses {
                    streams.push(QueryStream {
                        index: Some("by_type_status"),
                        partition_attribute: "type_status_scope",
                        partition_value: combined_key(scope, kind, status),
                    });
                }
            }
        }
    }
    Ok(streams)
}

fn decode_card(scope: &Scope, item: &HashMap<String, AttributeValue>) -> Result<Card> {
    let text = |name: &str| {
        item.get(name)
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
            .ok_or_else(|| StoreError::Unavailable(format!("catalog row lacks {name}")))
    };
    if text("scope")? != scope.namespace() {
        return Err(StoreError::Unavailable(
            "catalog returned a different scope".into(),
        ));
    }
    let card: Card = serde_json::from_str(text("card")?)?;
    if card.path != text("path")? || card.revision != text("revision")? {
        return Err(StoreError::Unavailable(
            "catalog row metadata disagrees with its card".into(),
        ));
    }
    Ok(card)
}

impl DynamoCatalog {
    /// Use an existing table with the documented scope/path and three GSI keys.
    pub fn new(client: aws_sdk_dynamodb::Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    async fn read_stream(
        &self,
        scope: &Scope,
        stream: QueryStream,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage> {
        let mut cards = Vec::new();
        let mut requests = 0;
        let mut candidates = 0;
        let mut last_key = None;
        loop {
            let mut request = self
                .client
                .query()
                .table_name(&self.table)
                .set_index_name(stream.index.map(str::to_owned))
                .consistent_read(stream.index.is_none())
                .scan_index_forward(true)
                .expression_attribute_names("#partition", stream.partition_attribute)
                .expression_attribute_values(
                    ":partition",
                    AttributeValue::S(stream.partition_value.clone()),
                )
                .limit((limit - cards.len()) as i32)
                .set_exclusive_start_key(last_key);
            request = if let Some(cursor) = cursor {
                request
                    .key_condition_expression("#partition = :partition AND #path > :cursor")
                    .expression_attribute_names("#path", "path")
                    .expression_attribute_values(":cursor", AttributeValue::S(cursor.to_owned()))
            } else {
                request.key_condition_expression("#partition = :partition")
            };
            let output = request
                .send()
                .await
                .map_err(|error| StoreError::Unavailable(error.to_string()))?;
            requests += 1;
            candidates += output.scanned_count() as usize;
            for item in output.items() {
                cards.push(decode_card(scope, item)?);
            }
            last_key = output.last_evaluated_key;
            if cards.len() >= limit || last_key.as_ref().is_none_or(HashMap::is_empty) {
                break;
            }
            if requests > limit {
                return Err(StoreError::Unavailable(
                    "catalog pagination made no bounded progress".into(),
                ));
            }
        }
        Ok(CatalogPage {
            cards,
            requests,
            candidates,
            next_cursor: None,
        })
    }
}

#[async_trait]
impl Catalog for DynamoCatalog {
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<Card>> {
        validate_path(path)?;
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("scope", AttributeValue::S(scope.namespace()))
            .key("path", AttributeValue::S(path.to_owned()))
            .send()
            .await
            .map_err(|error| StoreError::Unavailable(error.to_string()))?;
        output
            .item
            .as_ref()
            .map(|item| decode_card(scope, item))
            .transpose()
    }

    async fn put(&self, scope: &Scope, card: &Card, condition: WriteCondition) -> Result<()> {
        validate_path(&card.path)?;
        validate_fields(&card.fields)?;
        crate::validate_omitted_fields(&card.omitted_fields)?;
        let mut request = self
            .client
            .put_item()
            .table_name(&self.table)
            .item("scope", AttributeValue::S(scope.namespace()))
            .item("path", AttributeValue::S(card.path.clone()))
            .item("revision", AttributeValue::S(card.revision.clone()))
            .item("card", AttributeValue::S(serde_json::to_string(card)?));
        let kind = index_value(card.fields.get("type"));
        let status = index_value(card.fields.get("status"));
        if let Some(kind) = kind {
            request = request.item(
                "type_scope",
                AttributeValue::S(field_key(scope, "type", kind)),
            );
        }
        if let Some(status) = status {
            request = request.item(
                "status_scope",
                AttributeValue::S(field_key(scope, "status", status)),
            );
        }
        if let (Some(kind), Some(status)) = (kind, status) {
            request = request.item(
                "type_status_scope",
                AttributeValue::S(combined_key(scope, kind, status)),
            );
        }
        request = match condition {
            WriteCondition::Absent => request
                .condition_expression("attribute_not_exists(#path)")
                .expression_attribute_names("#path", "path"),
            WriteCondition::Revision(revision) => request
                .condition_expression("#revision = :revision")
                .expression_attribute_names("#revision", "revision")
                .expression_attribute_values(":revision", AttributeValue::S(revision)),
        };
        request.send().await.map_err(|error| {
            if error
                .as_service_error()
                .is_some_and(|e| e.is_conditional_check_failed_exception())
            {
                StoreError::Conflict
            } else {
                StoreError::Unavailable(error.to_string())
            }
        })?;
        Ok(())
    }

    async fn delete(&self, scope: &Scope, path: &str, expected_revision: &str) -> Result<()> {
        validate_path(path)?;
        self.client
            .delete_item()
            .table_name(&self.table)
            .key("scope", AttributeValue::S(scope.namespace()))
            .key("path", AttributeValue::S(path.to_owned()))
            .condition_expression("#revision = :revision")
            .expression_attribute_names("#revision", "revision")
            .expression_attribute_values(
                ":revision",
                AttributeValue::S(expected_revision.to_owned()),
            )
            .send()
            .await
            .map_err(|error| {
                if error
                    .as_service_error()
                    .is_some_and(|e| e.is_conditional_check_failed_exception())
                {
                    StoreError::Conflict
                } else {
                    StoreError::Unavailable(error.to_string())
                }
            })?;
        Ok(())
    }

    async fn query(
        &self,
        scope: &Scope,
        query: &CatalogQuery,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage> {
        validate_limit(limit)?;
        if let Some(cursor) = cursor {
            validate_path(cursor)?;
        }
        let streams = query_streams(scope, query)?;
        let mut cards = BTreeMap::new();
        let mut requests = 0;
        let mut candidates = 0;
        for stream in streams {
            let page = self.read_stream(scope, stream, cursor, limit + 1).await?;
            requests += page.requests;
            candidates += page.candidates;
            for card in page.cards {
                cards.insert(card.path.clone(), card);
            }
        }
        let has_more = cards.len() > limit;
        let cards = cards.into_values().take(limit).collect::<Vec<_>>();
        let next_cursor = if has_more {
            cards.last().map(|card| card.path.clone())
        } else {
            None
        };
        Ok(CatalogPage {
            cards,
            next_cursor,
            requests,
            candidates,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_streams_are_scoped_and_delimiter_safe() {
        let scope = Scope::new("brand", "app").unwrap();
        let query = CatalogQuery {
            issue_types: vec!["a#status#b".into()],
            statuses: vec!["c".into()],
        };
        let streams = query_streams(&scope, &query).unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(
            streams[0].partition_value,
            "brand/app#type#10:a#status#b#status#1:c"
        );
        assert_ne!(
            combined_key(&scope, "a#status#b", "c"),
            combined_key(&scope, "a", "b#status#c")
        );
        let query = CatalogQuery {
            issue_types: (0..33).map(|i| i.to_string()).collect(),
            statuses: vec![],
        };
        assert!(matches!(
            query_streams(&scope, &query),
            Err(StoreError::Invalid(_))
        ));
    }
}
