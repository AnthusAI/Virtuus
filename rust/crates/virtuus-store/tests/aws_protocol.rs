#![cfg(feature = "aws")]

use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use virtuus_store::aws::{DynamoCatalog, S3Store};
use virtuus_store::{Card, Catalog, CatalogQuery, ObjectStore, Scope, StoreError, WriteCondition};

fn response(status: u16, body: impl Into<SdkBody>) -> http::Response<SdkBody> {
    http::Response::builder()
        .status(status)
        .body(body.into())
        .unwrap()
}

fn replay(responses: Vec<http::Response<SdkBody>>) -> StaticReplayClient {
    StaticReplayClient::new(
        responses
            .into_iter()
            .map(|response| {
                ReplayEvent::new(
                    http::Request::builder()
                        .uri("https://offline.invalid/")
                        .body(SdkBody::empty())
                        .unwrap(),
                    response,
                )
            })
            .collect(),
    )
}

fn s3(client: &StaticReplayClient) -> S3Store {
    S3Store::new(
        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::new()
                .behavior_version_latest()
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "synthetic",
                    "synthetic",
                    None,
                    None,
                    "offline",
                ))
                .retry_config(
                    aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                )
                .http_client(client.clone())
                .build(),
        ),
        "ordinary-files",
    )
}

fn dynamo(client: &StaticReplayClient) -> DynamoCatalog {
    DynamoCatalog::new(
        aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::config::Builder::new()
                .behavior_version_latest()
                .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                    "synthetic",
                    "synthetic",
                    None,
                    None,
                    "offline",
                ))
                .retry_config(
                    aws_sdk_dynamodb::config::retry::RetryConfig::standard().with_max_attempts(1),
                )
                .http_client(client.clone())
                .build(),
        ),
        "ordinary-cards",
    )
}

fn scope() -> Scope {
    Scope::new("brand", "board").unwrap()
}
fn object(body: &str) -> http::Response<SdkBody> {
    http::Response::builder()
        .status(200)
        .header("etag", "\"native-r\"")
        .header("x-amz-meta-operation-id", "original-operation")
        .body(SdkBody::from(body))
        .unwrap()
}
fn s3_error(code: &str) -> http::Response<SdkBody> {
    http::Response::builder()
        .status(400)
        .header("content-type", "application/xml")
        .body(SdkBody::from(format!(
            "<Error><Code>{code}</Code><Message>synthetic</Message></Error>"
        )))
        .unwrap()
}
fn dynamo_error(code: &str) -> http::Response<SdkBody> {
    response(
        400,
        json!({"__type":code,"message":"synthetic"}).to_string(),
    )
}
fn card(name: &str) -> Card {
    Card {
        path: format!("project/issues/{name}.json"),
        revision: "r1".into(),
        hash: "hash".into(),
        byte_length: 12,
        omitted_fields: vec![],
        fields: BTreeMap::from([
            ("type".into(), "task".into()),
            ("status".into(), "open".into()),
        ]),
    }
}
fn item(card: &Card) -> Value {
    json!({"scope":{"S":scope().namespace()},"path":{"S":card.path},"revision":{"S":card.revision},"card":{"S":serde_json::to_string(card).unwrap()}})
}
fn query_items(cards: &[Card]) -> http::Response<SdkBody> {
    response(200,json!({"Items":cards.iter().map(item).collect::<Vec<_>>(),"ScannedCount":cards.len(),"Count":cards.len()}).to_string())
}

#[tokio::test]
async fn s3_preserves_native_revisions_raw_paths_and_conditional_headers() {
    let client = replay(vec![
        object("raw bytes"),
        object(""),
        object(""),
        object("control"),
        response(204, ""),
    ]);
    let store = s3(&client);
    let scope = scope();
    let path = "project/issues/a.json";
    let read = store.get(&scope, path).await.unwrap().unwrap();
    assert_eq!(read.bytes, b"raw bytes");
    assert_eq!(read.revision, "\"native-r\"");
    assert_eq!(read.content_type, "application/octet-stream");
    assert_eq!(read.operation_id.as_deref(), Some("original-operation"));
    let created = store
        .put(
            &scope,
            path,
            b"new",
            "application/json",
            WriteCondition::Absent,
            Some("operation-1"),
        )
        .await
        .unwrap();
    assert_eq!(created.bytes, b"new");
    assert_eq!(created.operation_id.as_deref(), Some("operation-1"));
    store
        .put_control(
            &scope,
            "receipts/a.json",
            b"{}",
            WriteCondition::Revision("\"native-r\"".into()),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_control(&scope, "receipts/a.json")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"control"
    );
    store.delete(&scope, path, "\"native-r\"").await.unwrap();
    let requests = client.actual_requests().collect::<Vec<_>>();
    assert!(requests[0]
        .uri()
        .contains("/brand/board/project/issues/a.json"));
    assert_eq!(requests[1].headers().get("if-none-match"), Some("*"));
    assert_eq!(
        requests[1].headers().get("x-amz-meta-operation-id"),
        Some("operation-1")
    );
    assert!(requests[2].uri().contains("/_virtuus/brand/board/"));
    assert_eq!(requests[2].headers().get("if-match"), Some("\"native-r\""));
    assert_eq!(requests[4].headers().get("if-match"), Some("\"native-r\""));
}

#[tokio::test]
async fn s3_distinguishes_conflict_rejection_and_ambiguous_mutations_without_retry() {
    for code in [
        "PreconditionFailed",
        "ConditionalRequestConflict",
        "NoSuchKey",
        "AccessDenied",
        "InvalidRequest",
        "InvalidArgument",
        "NoSuchBucket",
        "InternalError",
    ] {
        let client = replay(vec![s3_error(code), s3_error(code)]);
        let store = s3(&client);
        let scope = scope();
        let put = store
            .put(
                &scope,
                "a",
                b"x",
                "text/plain",
                WriteCondition::Absent,
                None,
            )
            .await
            .unwrap_err();
        let delete = store.delete(&scope, "a", "r").await.unwrap_err();
        match code {
            "PreconditionFailed" | "ConditionalRequestConflict" | "NoSuchKey" => {
                assert!(matches!(put, StoreError::Conflict));
                assert!(matches!(delete, StoreError::Conflict));
            }
            "AccessDenied" | "InvalidRequest" | "InvalidArgument" | "NoSuchBucket" => {
                assert!(matches!(put, StoreError::Unavailable(_)));
                assert!(matches!(delete, StoreError::Unavailable(_)));
            }
            _ => {
                assert!(matches!(put, StoreError::Uncertain(_)));
                assert!(matches!(delete, StoreError::Uncertain(_)));
            }
        }
        assert_eq!(client.actual_requests().count(), 2);
    }
    let client = replay(vec![s3_error("NotFound"), response(200, "")]);
    let store = s3(&client);
    assert!(matches!(
        store.delete(&scope(), "a", "r").await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        store
            .put(
                &scope(),
                "a",
                b"x",
                "text/plain",
                WriteCondition::Absent,
                None
            )
            .await,
        Err(StoreError::Uncertain(_))
    ));
}

#[tokio::test]
async fn s3_bounded_reads_reject_missing_metadata_bad_streams_and_excess_bytes() {
    let client = replay(vec![
        s3_error("NoSuchKey"),
        s3_error("AccessDenied"),
        response(200, "no revision"),
        http::Response::builder()
            .status(200)
            .header("etag", "r")
            .header("content-length", "20")
            .body(SdkBody::from("01234567890123456789"))
            .unwrap(),
        object("01234567890123456789"),
        object("small"),
        http::Response::builder()
            .status(200)
            .header("etag", "r")
            .body(SdkBody::taken())
            .unwrap(),
    ]);
    let store = s3(&client);
    let scope = scope();
    assert!(store.get(&scope, "a").await.unwrap().is_none());
    assert!(matches!(
        store.get(&scope, "a").await,
        Err(StoreError::Unavailable(_))
    ));
    assert!(matches!(
        store.get(&scope, "a").await,
        Err(StoreError::Unavailable(_))
    ));
    assert!(matches!(
        store.get_bounded(&scope, "a", 0).await,
        Err(StoreError::Invalid(_))
    ));
    assert!(matches!(
        store.get_bounded(&scope, "a", 10).await,
        Err(StoreError::TooLarge { limit: 10 })
    ));
    assert!(matches!(
        store.get_bounded(&scope, "a", 10).await,
        Err(StoreError::TooLarge { limit: 10 })
    ));
    assert_eq!(
        store
            .get_bounded(&scope, "a", 5)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"small"
    );
    assert!(matches!(
        store.get(&scope, "a").await,
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(client.actual_requests().count(), 7);
}

#[tokio::test]
async fn s3_listing_retains_scoped_lexical_cursors_and_rejects_foreign_keys() {
    let client=replay(vec![response(200,"<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>brand/board/project/issues/a.json</Key></Contents></ListBucketResult>"),
        response(200,"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"),
        response(200,"<ListBucketResult><Contents><Key>other/board/foreign</Key></Contents></ListBucketResult>"),
        response(200,"<ListBucketResult><Contents/></ListBucketResult>"),s3_error("AccessDenied")]);
    let store = s3(&client);
    let scope = scope();
    let page = store
        .list(&scope, "project/issues/", None, 1)
        .await
        .unwrap();
    assert_eq!(page.paths, ["project/issues/a.json"]);
    assert_eq!(page.next_cursor.as_deref(), Some("project/issues/a.json"));
    assert!(store
        .list(&scope, "project/issues/", page.next_cursor.as_deref(), 1)
        .await
        .unwrap()
        .next_cursor
        .is_none());
    assert!(matches!(
        store
            .list(&scope, "project/issues/", Some("other/a"), 1)
            .await,
        Err(StoreError::Invalid(_))
    ));
    for _ in 0..3 {
        assert!(matches!(
            store.list(&scope, "", None, 1).await,
            Err(StoreError::Unavailable(_))
        ));
    }
    let requests = client.actual_requests().collect::<Vec<_>>();
    assert!(requests[1].uri().contains("start-after="));
}

#[tokio::test]
async fn dynamo_reads_fail_closed_for_missing_or_inconsistent_projection_metadata() {
    let valid = card("a");
    let mut foreign = item(&valid);
    foreign["scope"]["S"] = json!("other/board");
    let mut mismatch = item(&valid);
    mismatch["revision"]["S"] = json!("newer");
    let mut invalid = item(&valid);
    invalid["card"]["S"] = json!("not json");
    let rows = vec![
        json!({}),
        json!({"Item":item(&valid)}),
        json!({"Item":{}}),
        json!({"Item":foreign}),
        json!({"Item":mismatch}),
        json!({"Item":invalid}),
    ];
    let mut responses = rows
        .into_iter()
        .map(|v| response(200, v.to_string()))
        .collect::<Vec<_>>();
    responses.push(dynamo_error("AccessDeniedException"));
    let client = replay(responses);
    let catalog = dynamo(&client);
    let scope = scope();
    assert!(catalog.get(&scope, &valid.path).await.unwrap().is_none());
    assert_eq!(
        catalog.get(&scope, &valid.path).await.unwrap(),
        Some(valid.clone())
    );
    for _ in 0..3 {
        assert!(matches!(
            catalog.get(&scope, &valid.path).await,
            Err(StoreError::Unavailable(_))
        ));
    }
    assert!(matches!(
        catalog.get(&scope, &valid.path).await,
        Err(StoreError::Json(_))
    ));
    assert!(matches!(
        catalog.get(&scope, &valid.path).await,
        Err(StoreError::Unavailable(_))
    ));
}

#[tokio::test]
async fn dynamo_conditional_writes_use_sparse_scalar_indexes_and_report_conflicts() {
    let client = replay(vec![
        response(200, "{}"),
        response(200, "{}"),
        dynamo_error("ConditionalCheckFailedException"),
        dynamo_error("AccessDeniedException"),
        response(200, "{}"),
        dynamo_error("ConditionalCheckFailedException"),
        dynamo_error("AccessDeniedException"),
    ]);
    let catalog = dynamo(&client);
    let scope = scope();
    let indexed = card("a");
    catalog
        .put(&scope, &indexed, WriteCondition::Absent)
        .await
        .unwrap();
    let mut sparse = card("b");
    sparse.fields.clear();
    catalog
        .put(&scope, &sparse, WriteCondition::Revision("old".into()))
        .await
        .unwrap();
    assert!(matches!(
        catalog.put(&scope, &indexed, WriteCondition::Absent).await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        catalog.put(&scope, &indexed, WriteCondition::Absent).await,
        Err(StoreError::Unavailable(_))
    ));
    catalog.delete(&scope, &indexed.path, "r1").await.unwrap();
    assert!(matches!(
        catalog.delete(&scope, &indexed.path, "r1").await,
        Err(StoreError::Conflict)
    ));
    assert!(matches!(
        catalog.delete(&scope, &indexed.path, "r1").await,
        Err(StoreError::Unavailable(_))
    ));
    let requests = client.actual_requests().collect::<Vec<_>>();
    let first: Value = serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
    let second: Value = serde_json::from_slice(requests[1].body().bytes().unwrap()).unwrap();
    assert_eq!(first["Item"]["type_scope"]["S"], "brand/board#type#4:task");
    assert!(first["Item"].get("type_status_scope").is_some());
    assert!(second["Item"].get("type_scope").is_none());
    assert_eq!(second["ConditionExpression"], "#revision = :revision");
}

#[tokio::test]
async fn dynamo_queries_select_each_index_and_bound_pagination_progress() {
    let scope = scope();
    for (query, index) in [
        (CatalogQuery::default(), None),
        (
            CatalogQuery {
                issue_types: vec!["task".into()],
                statuses: vec![],
            },
            Some("by_type"),
        ),
        (
            CatalogQuery {
                issue_types: vec![],
                statuses: vec!["open".into()],
            },
            Some("by_status"),
        ),
        (
            CatalogQuery {
                issue_types: vec!["task".into()],
                statuses: vec!["open".into()],
            },
            Some("by_type_status"),
        ),
    ] {
        let client = replay(vec![
            query_items(&[card("a"), card("b")]),
            query_items(&[card("b")]),
        ]);
        let catalog = dynamo(&client);
        let page = catalog.query(&scope, &query, None, 1).await.unwrap();
        assert_eq!(page.cards.len(), 1);
        assert_eq!(page.candidates, 2);
        let next = catalog
            .query(&scope, &query, page.next_cursor.as_deref(), 1)
            .await
            .unwrap();
        assert!(next.next_cursor.is_none());
        let requests = client.actual_requests().collect::<Vec<_>>();
        let body: Value = serde_json::from_slice(requests[0].body().bytes().unwrap()).unwrap();
        assert_eq!(body.get("IndexName").and_then(Value::as_str), index);
        assert_eq!(body["ConsistentRead"], index.is_none());
        let body: Value = serde_json::from_slice(requests[1].body().bytes().unwrap()).unwrap();
        assert!(body["KeyConditionExpression"]
            .as_str()
            .unwrap()
            .contains("#path > :cursor"));
    }
    let continuation=json!({"Items":[],"ScannedCount":1,"LastEvaluatedKey":{"scope":{"S":scope.namespace()},"path":{"S":"a"}}}).to_string();
    let client = replay(vec![
        response(200, continuation.clone()),
        query_items(&[card("a")]),
    ]);
    let catalog = dynamo(&client);
    let page = catalog
        .query(&scope, &CatalogQuery::default(), None, 1)
        .await
        .unwrap();
    assert_eq!(page.requests, 2);
    assert_eq!(page.candidates, 2);
    let client = replay(
        (0..3)
            .map(|_| response(200, continuation.clone()))
            .collect(),
    );
    let catalog = dynamo(&client);
    assert!(matches!(
        catalog
            .query(&scope, &CatalogQuery::default(), None, 1)
            .await,
        Err(StoreError::Unavailable(_))
    ));
    let client = replay(vec![dynamo_error("AccessDeniedException")]);
    let catalog = dynamo(&client);
    assert!(matches!(
        catalog
            .query(&scope, &CatalogQuery::default(), None, 1)
            .await,
        Err(StoreError::Unavailable(_))
    ));
}
