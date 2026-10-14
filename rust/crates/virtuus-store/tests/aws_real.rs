#![cfg(feature = "aws")]

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use virtuus_store::aws::{DynamoCatalog, S3Store};
use virtuus_store::{
    AuthorizedProjects, BoardQuery, Card, Catalog, CatalogPage, CatalogQuery, DeleteMutation,
    EventObject, JournaledStore, Mutation, ObjectStore, ProjectSelection, QueryOrder, Result,
    ScalarPredicate, Scope, StoreError, WriteCondition,
};

struct FailCatalogOnce {
    inner: DynamoCatalog,
    fail: AtomicBool,
}

#[async_trait]
impl Catalog for FailCatalogOnce {
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<Card>> {
        self.inner.get(scope, path).await
    }
    async fn put(&self, scope: &Scope, card: &Card, condition: WriteCondition) -> Result<()> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Unavailable("injected catalog outage".into()));
        }
        self.inner.put(scope, card, condition).await
    }
    async fn delete(&self, scope: &Scope, path: &str, expected_revision: &str) -> Result<()> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Unavailable("injected catalog outage".into()));
        }
        self.inner.delete(scope, path, expected_revision).await
    }
    async fn query(
        &self,
        scope: &Scope,
        query: &CatalogQuery,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<CatalogPage> {
        self.inner.query(scope, query, cursor, limit).await
    }
}

fn credentials() -> aws_sdk_s3::config::Credentials {
    aws_sdk_s3::config::Credentials::new(
        std::env::var("AWS_ACCESS_KEY_ID").expect("temporary SSO key"),
        std::env::var("AWS_SECRET_ACCESS_KEY").expect("temporary SSO secret"),
        Some(std::env::var("AWS_SESSION_TOKEN").expect("temporary SSO token")),
        None,
        "isolated-real-aws-proof",
    )
}

async fn indexed(
    catalog: &DynamoCatalog,
    scope: &Scope,
    query: &CatalogQuery,
    count: usize,
) -> CatalogPage {
    for attempt in 0..40 {
        let page = catalog.query(scope, query, None, 1000).await.unwrap();
        if page.cards.len() == count {
            return page;
        }
        assert!(attempt < 39, "GSI did not converge to {count} cards");
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    unreachable!()
}

#[tokio::test]
#[ignore = "requires explicitly authorized isolated real AWS fixture; infra/test-store/run.py"]
async fn real_aws_canonical_cas_catalog_and_recovery() {
    let bucket = std::env::var("VIRTUUS_TEST_BUCKET").expect("isolated bucket");
    let table = std::env::var("VIRTUUS_TEST_TABLE").expect("isolated table");
    let region = std::env::var("VIRTUUS_TEST_REGION").unwrap();
    let run = std::env::var("VIRTUUS_TEST_RUN").unwrap();
    assert!(bucket.starts_with("virtuus-store-proof-20261014-aws01-"));
    assert!(table.starts_with("virtuus-store-proof-20261014-aws01-"));
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new(region.clone()))
            .credentials_provider(credentials())
            .build(),
    );
    let ddb = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_dynamodb::config::Region::new(region))
            .credentials_provider(credentials())
            .build(),
    );
    let objects = Arc::new(S3Store::new(s3.clone(), &bucket));
    let catalog = Arc::new(DynamoCatalog::new(ddb.clone(), &table));
    let scope = Scope::new(&run, "project-a").unwrap();
    let other_tenant = Scope::new(format!("other-{run}"), "project-a").unwrap();
    let other_project = Scope::new(&run, "project-b").unwrap();
    let raw = b"{ \"id\":\"a\", \"title\":\"Unknown fields preserved\", \"type\":\"task\", \"status\":\"open\", \"unknown\":{\"comments\":[\"hello\"],\"history\":[1,2]} }\n";
    let large_event = format!("{{\"history\":\"{}\"}}\n", "x".repeat(450_000)).into_bytes();
    let config = b"# retain comments\nproject_directory: project\nunknown_setting: true\n";
    objects
        .put(
            &scope,
            ".kanbus.yml",
            config,
            "application/yaml",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    let fail_once = Arc::new(FailCatalogOnce {
        inner: (*catalog).clone(),
        fail: AtomicBool::new(true),
    });
    let journal = JournaledStore::new(objects.clone(), fail_once);
    let mutation = Mutation {
        path: "project/issues/a.json".into(),
        bytes: raw.to_vec(),
        content_type: "application/json".into(),
        condition: WriteCondition::Absent,
        operation_id: "create-a".into(),
        request_hash: "request-a".into(),
        event_objects: vec![EventObject {
            path: "project/events/a.json".into(),
            bytes: large_event.clone(),
            content_type: "application/json".into(),
        }],
        fields: BTreeMap::new(),
    };
    let partial = journal.commit(&scope, &mutation).await.unwrap();
    assert!(partial.repair_pending);
    assert_eq!(
        objects
            .get(&scope, &mutation.path)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        raw
    );
    assert!(catalog.get(&scope, &mutation.path).await.unwrap().is_none());
    let recovered = journal.commit(&scope, &mutation).await.unwrap();
    assert!(!recovered.repair_pending);
    assert_eq!(partial.stored, recovered.stored);
    assert_eq!(
        journal
            .operation(&scope, "create-a", "request-a")
            .await
            .unwrap()
            .unwrap()
            .stored,
        partial.stored
    );
    assert!(matches!(
        journal.operation(&scope, "create-a", "different").await,
        Err(StoreError::OperationMismatch)
    ));
    assert_eq!(
        objects
            .get(&scope, "project/events/a.json")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        large_event
    );
    assert_eq!(
        objects
            .get(&scope, ".kanbus.yml")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        config
    );
    let attachment = [0, 255, 17, 128, 10];
    objects
        .put(
            &scope,
            "attachments/a.bin",
            &attachment,
            "application/octet-stream",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        objects
            .get(&scope, "attachments/a.bin")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        attachment
    );
    let direct = s3
        .get_object()
        .bucket(&bucket)
        .key(format!("{}/{}", scope.namespace(), mutation.path))
        .send()
        .await
        .unwrap();
    assert_eq!(
        direct.body.collect().await.unwrap().into_bytes().as_ref(),
        raw
    );
    let listed = objects.list(&scope, "", None, 100).await.unwrap();
    assert_eq!(
        listed.paths,
        [
            ".kanbus.yml",
            "attachments/a.bin",
            "project/events/a.json",
            "project/issues/a.json"
        ]
    );
    for other in [&other_tenant, &other_project] {
        assert!(objects.get(other, &mutation.path).await.unwrap().is_none());
        assert!(objects
            .list(other, "", None, 100)
            .await
            .unwrap()
            .paths
            .is_empty());
        assert!(catalog.get(other, &mutation.path).await.unwrap().is_none());
        assert!(catalog
            .query(other, &CatalogQuery::default(), None, 100)
            .await
            .unwrap()
            .cards
            .is_empty());
    }
    let rev = recovered.stored.revision.clone();
    let (first, second) = tokio::join!(
        objects.put(
            &scope,
            &mutation.path,
            b"{\"id\":\"a\",\"type\":\"task\",\"status\":\"open\",\"winner\":1}\n",
            "application/json",
            WriteCondition::Revision(rev.clone()),
            None
        ),
        objects.put(
            &scope,
            &mutation.path,
            b"{\"id\":\"a\",\"type\":\"task\",\"status\":\"open\",\"winner\":2}\n",
            "application/json",
            WriteCondition::Revision(rev.clone()),
            None
        )
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.as_ref().err().or(second.as_ref().err()),
        Some(StoreError::Conflict)
    ));
    for i in 0..24 {
        let path = format!("project/issues/card-{i:03}.json");
        let kind = if i < 4 { "bug" } else { "task" };
        let status = if i % 2 == 0 { "open" } else { "closed" };
        let bytes = format!("{{\"id\":\"{i}\",\"type\":\"{kind}\",\"status\":\"{status}\",\"unknown\":{{\"nested\":true}}}}\n");
        objects
            .put(
                &scope,
                &path,
                bytes.as_bytes(),
                "application/json",
                WriteCondition::Absent,
                None,
            )
            .await
            .unwrap();
    }
    let healthy = JournaledStore::new(objects.clone(), catalog.clone());
    assert_eq!(
        healthy
            .rebuild_catalog(&scope, "project/issues/")
            .await
            .unwrap(),
        25
    );
    let all = catalog
        .query(&scope, &CatalogQuery::default(), None, 1000)
        .await
        .unwrap();
    assert_eq!(all.cards.len(), 25);
    let query = CatalogQuery {
        issue_types: vec!["bug".into()],
        statuses: vec!["open".into()],
    };
    let targeted = indexed(&catalog, &scope, &query, 2).await;
    assert_eq!(targeted.requests, 1);
    assert_eq!(targeted.candidates, 2);
    assert!(targeted.candidates < all.candidates);
    let types = CatalogQuery {
        issue_types: vec!["bug".into()],
        statuses: vec![],
    };
    indexed(&catalog, &scope, &types, 4).await;
    let statuses = CatalogQuery {
        issue_types: vec![],
        statuses: vec!["open".into()],
    };
    indexed(&catalog, &scope, &statuses, 13).await;
    let typed_first = catalog.query(&scope, &types, None, 2).await.unwrap();
    let typed_second = catalog
        .query(&scope, &types, typed_first.next_cursor.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(typed_first.cards.len(), 2);
    assert_eq!(typed_second.cards.len(), 2);
    assert!(typed_second.next_cursor.is_none());
    assert!(typed_first.cards.last().unwrap().path < typed_second.cards.first().unwrap().path);
    let first = catalog
        .query(&scope, &CatalogQuery::default(), None, 3)
        .await
        .unwrap();
    let second = catalog
        .query(
            &scope,
            &CatalogQuery::default(),
            first.next_cursor.as_deref(),
            3,
        )
        .await
        .unwrap();
    assert!(first.cards.last().unwrap().path < second.cards.first().unwrap().path);
    let canonical_paths = objects.list(&scope, "", None, 100).await.unwrap().paths;
    let folder = tempfile::tempdir().unwrap();
    for path in &canonical_paths {
        let source_key = format!("{}/{path}", scope.namespace());
        s3.copy_object()
            .bucket(&bucket)
            .copy_source(format!("{bucket}/{source_key}"))
            .key(format!("{}/{path}", other_project.namespace()))
            .send()
            .await
            .unwrap();
        let raw = objects.get(&scope, path).await.unwrap().unwrap();
        assert_eq!(
            objects
                .get(&other_project, path)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            raw.bytes
        );
        let destination = folder.path().join(path);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::write(destination, &raw.bytes).unwrap();
    }
    assert_eq!(
        healthy
            .rebuild_catalog(&other_project, "project/issues/")
            .await
            .unwrap(),
        25
    );
    indexed(&catalog, &other_project, &query, 2).await;
    let authorized =
        AuthorizedProjects::new(&run, vec![scope.clone(), other_project.clone()]).unwrap();
    let board_all = healthy
        .query_authorized(
            &authorized,
            &ProjectSelection::All,
            &BoardQuery::default(),
            None,
            100,
        )
        .await
        .unwrap();
    assert_eq!(board_all.cards.len(), 50);
    assert_eq!((board_all.candidates, board_all.requests), (50, 2));
    let board_query = BoardQuery {
        candidates: query.clone(),
        predicates: vec![ScalarPredicate {
            field: "id".into(),
            condition: virtuus::SortCondition::Eq("0".into()),
        }],
        order: vec![QueryOrder {
            field: "id".into(),
            descending: true,
        }],
        max_candidates: 1000,
    };
    let board_first = healthy
        .query_authorized(&authorized, &ProjectSelection::All, &board_query, None, 1)
        .await
        .unwrap();
    let board_second = healthy
        .query_authorized(
            &authorized,
            &ProjectSelection::All,
            &board_query,
            board_first.next_cursor.as_deref(),
            1,
        )
        .await
        .unwrap();
    assert_eq!((board_first.candidates, board_first.requests), (4, 2));
    assert_eq!(board_first.cards.len(), 1);
    assert_eq!(board_second.cards.len(), 1);
    assert!(board_first.cards[0].scope < board_second.cards[0].scope);
    assert_eq!(board_first.cards[0].card.fields["id"], "0");
    assert!(board_second.next_cursor.is_none());
    let selected = ProjectSelection::Selected(vec!["project-a".into()]);
    let selected_page = healthy
        .query_authorized(&authorized, &selected, &board_query, None, 100)
        .await
        .unwrap();
    assert_eq!(
        (
            selected_page.cards.len(),
            selected_page.candidates,
            selected_page.requests
        ),
        (1, 2, 1)
    );
    assert!(matches!(
        healthy
            .query_authorized(
                &authorized,
                &selected,
                &board_query,
                board_first.next_cursor.as_deref(),
                1
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert!(matches!(
        healthy
            .query_authorized(
                &authorized,
                &ProjectSelection::Selected(vec!["unauthorized".into()]),
                &board_query,
                None,
                100
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
    println!("LIVE_BOARD all_projects=2 all_cards=50 all_candidates={} all_requests={} targeted_candidates={} targeted_requests={} residual_matches=2 deterministic_pages=true selected_candidates=2 selected_requests=1", board_all.candidates, board_all.requests, board_first.candidates, board_first.requests);
    let imported = Scope::new(&run, "folder-import").unwrap();
    for path in &canonical_paths {
        let bytes = std::fs::read(folder.path().join(path)).unwrap();
        objects
            .put(
                &imported,
                path,
                &bytes,
                "application/octet-stream",
                WriteCondition::Absent,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            objects.get(&imported, path).await.unwrap().unwrap().bytes,
            bytes
        );
    }
    assert_eq!(
        healthy
            .rebuild_catalog(&imported, "project/issues/")
            .await
            .unwrap(),
        25
    );
    assert_eq!(
        objects.list(&imported, "", None, 100).await.unwrap().paths,
        canonical_paths
    );
    let imported_current = objects
        .get(&imported, &mutation.path)
        .await
        .unwrap()
        .unwrap();
    let deletion = DeleteMutation {
        path: mutation.path.clone(),
        expected_revision: imported_current.revision,
        operation_id: "delete-a".into(),
        request_hash: "delete-request-a".into(),
        event_objects: vec![EventObject {
            path: "project/events/deleted-a.json".into(),
            bytes: b"{ \"event\": \"deleted\", \"unknown\": true }\n".to_vec(),
            content_type: "application/json".into(),
        }],
    };
    let delete_journal = JournaledStore::new(
        objects.clone(),
        Arc::new(FailCatalogOnce {
            inner: (*catalog).clone(),
            fail: AtomicBool::new(true),
        }),
    );
    assert!(
        delete_journal
            .delete(&imported, &deletion)
            .await
            .unwrap()
            .repair_pending
    );
    assert!(objects
        .get(&imported, &mutation.path)
        .await
        .unwrap()
        .is_none());
    assert!(
        !delete_journal
            .delete(&imported, &deletion)
            .await
            .unwrap()
            .repair_pending
    );
    assert!(catalog
        .get(&imported, &mutation.path)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        objects
            .get(&imported, "project/events/deleted-a.json")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        deletion.event_objects[0].bytes
    );
    assert!(
        !delete_journal
            .deletion_operation(&imported, "delete-a", "delete-request-a")
            .await
            .unwrap()
            .unwrap()
            .repair_pending
    );
    assert!(matches!(
        delete_journal
            .deletion_operation(&imported, "delete-a", "different")
            .await,
        Err(StoreError::OperationMismatch)
    ));
    assert!(matches!(
        objects
            .delete(&imported, &mutation.path, &deletion.expected_revision)
            .await,
        Err(StoreError::Conflict)
    ));
    for card in &all.cards {
        catalog
            .delete(&scope, &card.path, &card.revision)
            .await
            .unwrap();
    }
    assert!(catalog
        .query(&scope, &CatalogQuery::default(), None, 1000)
        .await
        .unwrap()
        .cards
        .is_empty());
    assert_eq!(
        healthy
            .rebuild_catalog(&scope, "project/issues/")
            .await
            .unwrap(),
        25
    );
    let current = objects.get(&scope, &mutation.path).await.unwrap().unwrap();
    assert!(matches!(
        objects.delete(&scope, &mutation.path, &rev).await,
        Err(StoreError::Conflict)
    ));
    objects
        .delete(&scope, &mutation.path, &current.revision)
        .await
        .unwrap();
    assert!(objects.get(&scope, &mutation.path).await.unwrap().is_none());
    healthy
        .rebuild_catalog(&scope, "project/issues/")
        .await
        .unwrap();
    assert!(catalog.get(&scope, &mutation.path).await.unwrap().is_none());
    println!("LIVE_AWS bucket={bucket} table={table} scope={} all_candidates={} all_requests={} targeted_candidates={} targeted_requests={} large_event_bytes={} single_cas_winner=true recovery=true rebuilt=25 final_cards=24 ordinary_prefix_copy=true folder_import=true", scope.namespace(), all.candidates, all.requests, targeted.candidates, targeted.requests, large_event.len());
}

#[tokio::test]
#[ignore = "requires explicitly authorized isolated real AWS fixture; infra/test-store/run.py"]
async fn long_scalar_catalog_remains_rebuildable() {
    let bucket = std::env::var("VIRTUUS_TEST_BUCKET").unwrap();
    let table = std::env::var("VIRTUUS_TEST_TABLE").unwrap();
    assert!(bucket.starts_with("virtuus-store-proof-20261014-aws01-"));
    assert!(table.starts_with("virtuus-store-proof-20261014-aws01-"));
    let region = std::env::var("VIRTUUS_TEST_REGION").unwrap();
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new(region.clone()))
            .credentials_provider(credentials())
            .build(),
    );
    let ddb = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_dynamodb::config::Region::new(region))
            .credentials_provider(credentials())
            .build(),
    );
    let scope = Scope::new(std::env::var("VIRTUUS_TEST_RUN").unwrap(), "long-scalar").unwrap();
    let objects = Arc::new(S3Store::new(s3, bucket));
    let catalog = Arc::new(DynamoCatalog::new(ddb.clone(), &table));
    let path = "project/issues/long.json";
    let value = "x".repeat(3000);
    let bytes = serde_json::to_vec(&serde_json::json!({"id":"long", "type":value, "status":value, "unknown": {"preserved":true}})).unwrap();
    objects
        .put(
            &scope,
            path,
            &bytes,
            "application/json",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    let journal = JournaledStore::new(objects.clone(), catalog.clone());
    assert_eq!(
        journal
            .rebuild_catalog(&scope, "project/issues/")
            .await
            .unwrap(),
        1
    );
    let card = catalog.get(&scope, path).await.unwrap().unwrap();
    assert_eq!(card.fields["type"], value);
    assert_eq!(card.fields["status"], value);
    assert_eq!(
        objects.get(&scope, path).await.unwrap().unwrap().bytes,
        bytes
    );
    assert_eq!(
        catalog
            .query(&scope, &CatalogQuery::default(), None, 100)
            .await
            .unwrap()
            .cards,
        [card]
    );
    let row = ddb
        .get_item()
        .table_name(table)
        .key(
            "scope",
            aws_sdk_dynamodb::types::AttributeValue::S(scope.namespace()),
        )
        .key(
            "path",
            aws_sdk_dynamodb::types::AttributeValue::S(path.into()),
        )
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    for attribute in ["type_scope", "status_scope", "type_status_scope"] {
        assert!(!row.contains_key(attribute));
    }
    println!("LIVE_LONG_SCALAR scope={} type_bytes=3000 status_bytes=3000 canonical_preserved=true rebuilt=1 direct_lookup=true all_query=true oversized_gsi_keys_omitted=true", scope.namespace());
}

#[tokio::test]
#[ignore = "requires explicitly authorized isolated real AWS fixture; infra/test-store/run.py"]
async fn bounded_import_and_exact_large_integer_queries() {
    let bucket = std::env::var("VIRTUUS_TEST_BUCKET").unwrap();
    let table = std::env::var("VIRTUUS_TEST_TABLE").unwrap();
    assert!(bucket.starts_with("virtuus-store-proof-20261014-aws01-"));
    assert!(table.starts_with("virtuus-store-proof-20261014-aws01-"));
    let region = std::env::var("VIRTUUS_TEST_REGION").unwrap();
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new(region.clone()))
            .credentials_provider(credentials())
            .build(),
    );
    let ddb = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_dynamodb::config::Region::new(region))
            .credentials_provider(credentials())
            .build(),
    );
    let run = std::env::var("VIRTUUS_TEST_RUN").unwrap();
    let scope = Scope::new(&run, "bounded-import").unwrap();
    let objects = Arc::new(S3Store::new(s3, bucket));
    let catalog = Arc::new(DynamoCatalog::new(ddb, table));
    let journal = JournaledStore::new(objects.clone(), catalog.clone());
    let path = "project/issues/giant-title.json";
    let raw = serde_json::to_vec(&serde_json::json!({"id":"giant", "title":"x".repeat(17_000), "type":"task", "status":"open", "unknown":{"preserved":true}})).unwrap();
    objects
        .put(
            &scope,
            path,
            &raw,
            "application/json",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        journal
            .rebuild_catalog(&scope, "project/issues/")
            .await
            .unwrap(),
        1
    );
    let card = catalog.get(&scope, path).await.unwrap().unwrap();
    assert!(card.omitted_fields.contains(&"title".to_owned()));
    assert!(!card.fields.contains_key("title"));
    assert_eq!(card.hash, virtuus_store::content_hash(&raw));
    assert_eq!(card.byte_length, raw.len() as u64);
    assert!(matches!(
        journal.get_bounded(&scope, path, 1024).await,
        Err(StoreError::TooLarge { limit: 1024 })
    ));
    assert_eq!(
        journal
            .get_bounded(&scope, path, raw.len())
            .await
            .unwrap()
            .unwrap()
            .bytes,
        raw
    );
    let event = vec![17_u8; 450_000];
    objects
        .put(
            &scope,
            "project/events/large.bin",
            &event,
            "application/octet-stream",
            WriteCondition::Absent,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        objects
            .get_bounded(&scope, "project/events/large.bin", 1024)
            .await,
        Err(StoreError::TooLarge { limit: 1024 })
    ));
    assert_eq!(
        objects
            .get_bounded(&scope, "project/events/large.bin", event.len())
            .await
            .unwrap()
            .unwrap()
            .bytes,
        event
    );
    let authorized = AuthorizedProjects::new(&run, vec![scope.clone()]).unwrap();
    let ordinary = journal
        .query_authorized(
            &authorized,
            &ProjectSelection::All,
            &BoardQuery::default(),
            None,
            100,
        )
        .await
        .unwrap();
    assert_eq!(ordinary.cards.len(), 1);
    let title_order = BoardQuery {
        order: vec![QueryOrder {
            field: "title".into(),
            descending: false,
        }],
        ..BoardQuery::default()
    };
    assert!(matches!(
        journal
            .query_authorized(&authorized, &ProjectSelection::All, &title_order, None, 100)
            .await,
        Err(StoreError::Invalid(_))
    ));
    let title_filter = BoardQuery {
        predicates: vec![ScalarPredicate {
            field: "title".into(),
            condition: virtuus::SortCondition::Contains("x".into()),
        }],
        ..BoardQuery::default()
    };
    assert!(matches!(
        journal
            .query_authorized(
                &authorized,
                &ProjectSelection::All,
                &title_filter,
                None,
                100
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
    let numbers = Scope::new(&run, "exact-numbers").unwrap();
    for (path, value) in [
        ("project/issues/a-high.json", 9_007_199_254_740_993_u64),
        ("project/issues/z-low.json", 9_007_199_254_740_992_u64),
    ] {
        let bytes = format!("{{\"id\":\"{value}\",\"type\":\"task\",\"priority\":{value}}}\n");
        objects
            .put(
                &numbers,
                path,
                bytes.as_bytes(),
                "application/json",
                WriteCondition::Absent,
                None,
            )
            .await
            .unwrap();
    }
    assert_eq!(
        journal
            .rebuild_catalog(&numbers, "project/issues/")
            .await
            .unwrap(),
        2
    );
    let authorized_numbers = AuthorizedProjects::new(&run, vec![numbers.clone()]).unwrap();
    let numeric_order = BoardQuery {
        order: vec![QueryOrder {
            field: "priority".into(),
            descending: false,
        }],
        ..BoardQuery::default()
    };
    let ordered = journal
        .query_authorized(
            &authorized_numbers,
            &ProjectSelection::All,
            &numeric_order,
            None,
            100,
        )
        .await
        .unwrap();
    assert_eq!(
        ordered
            .cards
            .iter()
            .map(|c| c.card.path.as_str())
            .collect::<Vec<_>>(),
        ["project/issues/z-low.json", "project/issues/a-high.json"]
    );
    assert_eq!((ordered.candidates, ordered.requests), (2, 1));
    for (condition, expected) in [
        (
            virtuus::SortCondition::Lt(serde_json::json!(9_007_199_254_740_993_u64)),
            "project/issues/z-low.json",
        ),
        (
            virtuus::SortCondition::Gt(serde_json::json!(9_007_199_254_740_992_u64)),
            "project/issues/a-high.json",
        ),
    ] {
        let filtered = BoardQuery {
            predicates: vec![ScalarPredicate {
                field: "priority".into(),
                condition,
            }],
            ..BoardQuery::default()
        };
        let result = journal
            .query_authorized(
                &authorized_numbers,
                &ProjectSelection::All,
                &filtered,
                None,
                100,
            )
            .await
            .unwrap();
        assert_eq!(result.cards.len(), 1);
        assert_eq!(result.cards[0].card.path, expected);
    }
    println!("LIVE_BOUNDED_EXACT scope={} giant_title_bytes=17000 canonical_preserved=true omitted_title_reported=true unprojected_query_rejected=true bounded_limit=1024 bounded_event_bytes=450000 full_budget_success=true adjacent_integers=9007199254740992,9007199254740993 exact_order_and_lt_gt=true numeric_candidates=2 numeric_requests=1", scope.namespace());
}
