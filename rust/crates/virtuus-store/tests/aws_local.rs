#![cfg(feature = "aws")]

use std::collections::BTreeMap;
use virtuus_store::aws::{DynamoCatalog, S3Store};
use virtuus_store::{Card, Catalog, CatalogQuery, ObjectStore, Scope, StoreError, WriteCondition};

#[tokio::test]
#[ignore = "requires local S3 and DynamoDB emulators; see AWS_LOCAL.md"]
async fn canonical_bytes_cas_and_scoped_catalog_pages() {
    let s3_endpoint = std::env::var("VIRTUUS_TEST_S3_ENDPOINT").expect("local S3 endpoint");
    let dynamo_endpoint =
        std::env::var("VIRTUUS_TEST_DYNAMO_ENDPOINT").expect("local Dynamo endpoint");
    assert!(s3_endpoint.starts_with("http://127.0.0.1:"));
    assert!(dynamo_endpoint.starts_with("http://127.0.0.1:"));
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "local",
                "local",
                None,
                None,
                "local-test",
            ))
            .endpoint_url(s3_endpoint)
            .force_path_style(true)
            .build(),
    );
    let dynamo = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "local",
                "local",
                None,
                None,
                "local-test",
            ))
            .endpoint_url(dynamo_endpoint)
            .build(),
    );
    let bucket = format!("virtuus-store-{}", std::process::id());
    s3.create_bucket().bucket(&bucket).send().await.unwrap();
    let store = S3Store::new(s3.clone(), bucket.clone());
    let scope = Scope::new("brand", "prospectus").unwrap();
    let other = Scope::new("another", "prospectus").unwrap();
    let path = "project/issues/prospect-1.json";
    let bytes = b"{ \"unknown\": [1, 2], \"type\": \"prospect\" }\n";
    let created = store
        .put(
            &scope,
            path,
            bytes,
            "application/json",
            WriteCondition::Absent,
            Some("create-1"),
        )
        .await
        .unwrap();
    assert_eq!(store.get(&scope, path).await.unwrap().unwrap().bytes, bytes);
    assert_eq!(
        store
            .get(&scope, path)
            .await
            .unwrap()
            .unwrap()
            .operation_id
            .as_deref(),
        Some("create-1")
    );
    assert!(store.get(&other, path).await.unwrap().is_none());
    assert!(matches!(
        store
            .put(
                &scope,
                path,
                b"bad",
                "application/json",
                WriteCondition::Absent,
                None
            )
            .await,
        Err(StoreError::Conflict)
    ));
    let updated = store
        .put(
            &scope,
            path,
            b"{}\n",
            "application/json",
            WriteCondition::Revision(created.revision.clone()),
            Some("update-1"),
        )
        .await
        .unwrap();
    assert_ne!(created.revision, updated.revision);
    assert!(matches!(
        store
            .put(
                &scope,
                path,
                b"stale",
                "application/json",
                WriteCondition::Revision(created.revision),
                None
            )
            .await,
        Err(StoreError::Conflict)
    ));
    store
        .put_control(
            &scope,
            "commands/create-1.json",
            b"{}",
            WriteCondition::Absent,
        )
        .await
        .unwrap();
    assert_eq!(
        store.list(&scope, "", None, 10).await.unwrap().paths,
        [path]
    );
    assert!(store
        .get_control(&other, "commands/create-1.json")
        .await
        .unwrap()
        .is_none());

    let table = format!("virtuus-store-{}", std::process::id());
    create_catalog_table(&dynamo, &table).await;
    let catalog = DynamoCatalog::new(dynamo.clone(), table.clone());
    for (path, kind, status) in [
        ("project/issues/a.json", "prospect", "new"),
        ("project/issues/b.json", "task", "open"),
        ("project/issues/c.json", "prospect", "customer"),
        ("project/issues/d.json", "prospect", "new"),
    ] {
        let card = Card {
            omitted_fields: Vec::new(),
            path: path.into(),
            revision: "r1".into(),
            hash: "raw-hash".into(),
            byte_length: 10,
            fields: BTreeMap::from([
                ("type".into(), kind.into()),
                ("status".into(), status.into()),
            ]),
        };
        catalog
            .put(&scope, &card, WriteCondition::Absent)
            .await
            .unwrap();
        assert_eq!(catalog.get(&scope, path).await.unwrap(), Some(card.clone()));
        assert!(catalog.get(&other, path).await.unwrap().is_none());
        assert!(matches!(
            catalog.put(&scope, &card, WriteCondition::Absent).await,
            Err(StoreError::Conflict)
        ));
    }
    let query = CatalogQuery {
        issue_types: vec!["prospect".into()],
        statuses: vec![],
    };
    let first = catalog.query(&scope, &query, None, 2).await.unwrap();
    assert_eq!(
        first
            .cards
            .iter()
            .map(|c| c.path.as_str())
            .collect::<Vec<_>>(),
        ["project/issues/a.json", "project/issues/c.json"]
    );
    let second = catalog
        .query(&scope, &query, first.next_cursor.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(
        second
            .cards
            .iter()
            .map(|c| c.path.as_str())
            .collect::<Vec<_>>(),
        ["project/issues/d.json"]
    );
    assert!(second.next_cursor.is_none());
    assert!(catalog
        .query(&other, &query, None, 2)
        .await
        .unwrap()
        .cards
        .is_empty());
    let statuses = CatalogQuery {
        issue_types: vec!["prospect".into()],
        statuses: vec!["new".into()],
    };
    assert_eq!(
        catalog
            .query(&scope, &statuses, None, 10)
            .await
            .unwrap()
            .cards
            .len(),
        2
    );
    dynamo
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();
    let keys = s3.list_objects_v2().bucket(&bucket).send().await.unwrap();
    for object in keys.contents() {
        s3.delete_object()
            .bucket(&bucket)
            .key(object.key().unwrap())
            .send()
            .await
            .unwrap();
    }
    s3.delete_bucket().bucket(&bucket).send().await.unwrap();
}

async fn create_catalog_table(client: &aws_sdk_dynamodb::Client, name: &str) {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType,
        Projection, ProjectionType, ScalarAttributeType,
    };
    let key = |name: &str, kind| {
        KeySchemaElement::builder()
            .attribute_name(name)
            .key_type(kind)
            .build()
            .unwrap()
    };
    let mut request = client
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key("scope", KeyType::Hash))
        .key_schema(key("path", KeyType::Range));
    for name in [
        "scope",
        "path",
        "type_scope",
        "status_scope",
        "type_status_scope",
    ] {
        request = request.attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(name)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        );
    }
    for (name, hash) in [
        ("by_type", "type_scope"),
        ("by_status", "status_scope"),
        ("by_type_status", "type_status_scope"),
    ] {
        request = request.global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name(name)
                .key_schema(key(hash, KeyType::Hash))
                .key_schema(key("path", KeyType::Range))
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        );
    }
    request.send().await.unwrap();
}
