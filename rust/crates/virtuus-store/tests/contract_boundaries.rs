use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use virtuus_store::{
    AuthorizedProjects, BoardQuery, Card, Catalog, CatalogQuery, FileStore, JournaledStore,
    MemoryCatalog, ObjectStore, ProjectSelection, QueryOrder, ScalarPredicate, Scope, StoreError,
    WriteCondition,
};

fn scope() -> Scope {
    Scope::new("brand", "board").unwrap()
}
fn card(index: usize) -> Card {
    Card {
        path: format!("project/issues/{index:04}.json"),
        revision: "r".into(),
        hash: "hash".into(),
        byte_length: 1,
        omitted_fields: vec![],
        fields: BTreeMap::from([
            ("type".into(), json!("task")),
            ("status".into(), json!("open")),
            ("score".into(), json!(index)),
            ("title".into(), json!("alpha")),
        ]),
    }
}
fn store(catalog: Arc<MemoryCatalog>) -> (tempfile::TempDir, JournaledStore) {
    let root = tempfile::tempdir().unwrap();
    let objects = Arc::new(FileStore::new(root.path()).unwrap());
    (root, JournaledStore::new(objects, catalog))
}

#[tokio::test]
async fn invalid_paths_limits_and_projection_metadata_fail_before_publication() {
    for path in ["", "/absolute", "a//b", "a/../b", "a/./b", "a\\b"] {
        assert!(virtuus_store::validate_path(path).is_err(), "{path}");
    }
    for limit in [0, 1001] {
        assert!(virtuus_store::validate_limit(limit).is_err());
    }
    let catalog = MemoryCatalog::default();
    for omitted in [
        vec![String::new()],
        vec!["x".repeat(129)],
        vec!["x".into(); 257],
    ] {
        let mut value = card(1);
        value.omitted_fields = omitted;
        assert!(matches!(
            catalog.put(&scope(), &value, WriteCondition::Absent).await,
            Err(StoreError::Invalid(_))
        ));
    }
    for fields in [
        BTreeMap::from([("nested".into(), json!({"value":1}))]),
        BTreeMap::from([("huge".into(), json!("x".repeat(17000)))]),
        (0..65).map(|i| (i.to_string(), Value::Null)).collect(),
    ] {
        assert!(virtuus_store::validate_fields(&fields).is_err());
    }
    assert!(catalog
        .get(&scope(), &card(1).path)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn selected_projects_and_query_budgets_validate_the_authorization_contract() {
    assert!(AuthorizedProjects::new("brand", vec![scope(); 33]).is_err());
    assert!(AuthorizedProjects::new("brand", vec![Scope::new("other", "board").unwrap()]).is_err());
    let (_root, store) = store(Arc::new(MemoryCatalog::default()));
    let authorized = AuthorizedProjects::new("brand", vec![scope()]).unwrap();
    for selected in [vec![], vec!["board".into(); 33]] {
        assert!(matches!(
            store
                .query_authorized(
                    &authorized,
                    &ProjectSelection::Selected(selected),
                    &BoardQuery::default(),
                    None,
                    1
                )
                .await,
            Err(StoreError::Invalid(_))
        ));
    }
    let mut queries = vec![];
    let query = BoardQuery {
        max_candidates: 0,
        ..Default::default()
    };
    queries.push(query);
    let mut query = BoardQuery::default();
    query.candidates.issue_types = (0..33).map(|i| i.to_string()).collect();
    queries.push(query);
    let query = BoardQuery {
        order: (0..9)
            .map(|_| QueryOrder {
                field: "score".into(),
                descending: false,
            })
            .collect(),
        ..Default::default()
    };
    queries.push(query);
    let query = BoardQuery {
        predicates: (0..33)
            .map(|_| ScalarPredicate {
                field: "score".into(),
                condition: virtuus::SortCondition::Eq(json!(1)),
            })
            .collect(),
        ..Default::default()
    };
    queries.push(query);
    let query = BoardQuery {
        order: vec![QueryOrder {
            field: String::new(),
            descending: false,
        }],
        ..Default::default()
    };
    queries.push(query);
    let query = BoardQuery {
        predicates: vec![ScalarPredicate {
            field: "x".repeat(129),
            condition: virtuus::SortCondition::Eq(json!(1)),
        }],
        ..Default::default()
    };
    queries.push(query);
    for query in queries {
        assert!(matches!(
            store
                .query_authorized(&authorized, &ProjectSelection::All, &query, None, 1)
                .await,
            Err(StoreError::Invalid(_))
        ));
    }
    for cursor in ["not-json".into(), "x".repeat(32769)] {
        assert!(matches!(
            store
                .query_authorized(
                    &authorized,
                    &ProjectSelection::All,
                    &BoardQuery::default(),
                    Some(&cursor),
                    1
                )
                .await,
            Err(StoreError::Invalid(_))
        ));
    }
}

#[tokio::test]
async fn scalar_conditions_and_descending_order_keep_exact_numeric_semantics() {
    let catalog = Arc::new(MemoryCatalog::default());
    for i in 0..3 {
        catalog
            .put(&scope(), &card(i), WriteCondition::Absent)
            .await
            .unwrap();
    }
    let (_root, store) = store(catalog);
    let authorized = AuthorizedProjects::new("brand", vec![scope()]).unwrap();
    use virtuus::SortCondition as C;
    for (condition, expected) in [
        (C::Eq(json!(1)), vec![1]),
        (C::Ne(json!(1)), vec![2, 0]),
        (C::Lte(json!(1)), vec![1, 0]),
        (C::Gte(json!(1)), vec![2, 1]),
        (C::Between(json!(1), json!(2)), vec![2, 1]),
    ] {
        let query = BoardQuery {
            predicates: vec![ScalarPredicate {
                field: "score".into(),
                condition,
            }],
            order: vec![QueryOrder {
                field: "score".into(),
                descending: true,
            }],
            ..Default::default()
        };
        let page = store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
            .await
            .unwrap();
        assert_eq!(
            page.cards
                .iter()
                .map(|entry| entry.card.fields["score"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }
    for condition in [C::BeginsWith("al".into()), C::Contains("ph".into())] {
        let query = BoardQuery {
            predicates: vec![ScalarPredicate {
                field: "title".into(),
                condition,
            }],
            ..Default::default()
        };
        assert_eq!(
            store
                .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
                .await
                .unwrap()
                .cards
                .len(),
            3
        );
    }
    let unsupported: Value = serde_json::from_str("1e9223372036854775808").unwrap();
    let query = BoardQuery {
        predicates: vec![ScalarPredicate {
            field: "score".into(),
            condition: C::Between(json!(0), unsupported),
        }],
        ..Default::default()
    };
    assert!(matches!(
        store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert_eq!(
        virtuus_store::compare_numbers(&json!(-1), &json!(1)),
        Some(std::cmp::Ordering::Less)
    );
    assert_eq!(
        virtuus_store::compare_numbers(&json!(1), &json!(-1)),
        Some(std::cmp::Ordering::Greater)
    );
}

#[tokio::test]
async fn catalog_paging_visits_every_card_and_rejects_an_exceeded_candidate_budget() {
    let catalog = Arc::new(MemoryCatalog::default());
    for i in 0..123 {
        catalog
            .put(&scope(), &card(i), WriteCondition::Absent)
            .await
            .unwrap();
    }
    let page = catalog
        .query(&scope(), &CatalogQuery::default(), None, 1)
        .await
        .unwrap();
    assert_eq!(
        catalog
            .query(
                &scope(),
                &CatalogQuery::default(),
                page.next_cursor.as_deref(),
                1
            )
            .await
            .unwrap()
            .cards[0]
            .path,
        card(1).path
    );
    let (_root, store) = store(catalog);
    let authorized = AuthorizedProjects::new("brand", vec![scope()]).unwrap();
    let page = store
        .query_authorized(
            &authorized,
            &ProjectSelection::All,
            &BoardQuery::default(),
            None,
            1000,
        )
        .await
        .unwrap();
    assert_eq!(page.cards.len(), 123);
    assert!(page.requests > 1);
    let query = BoardQuery {
        max_candidates: 1,
        ..Default::default()
    };
    assert!(matches!(
        store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 1)
            .await,
        Err(StoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn unsupported_projected_numbers_fail_closed_before_sorting_or_filtering() {
    let catalog = Arc::new(MemoryCatalog::default());
    let mut value = card(0);
    value.fields.insert(
        "score".into(),
        serde_json::from_str("1e9223372036854775808").unwrap(),
    );
    catalog
        .put(&scope(), &value, WriteCondition::Absent)
        .await
        .unwrap();
    let (_root, store) = store(catalog);
    let authorized = AuthorizedProjects::new("brand", vec![scope()]).unwrap();
    let mut query = BoardQuery {
        order: vec![QueryOrder {
            field: "score".into(),
            descending: false,
        }],
        ..Default::default()
    };
    assert!(matches!(
        store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 1)
            .await,
        Err(StoreError::Invalid(_))
    ));
    query.order.clear();
    query.predicates = vec![ScalarPredicate {
        field: "score".into(),
        condition: virtuus::SortCondition::Eq(json!(1)),
    }];
    assert!(matches!(
        store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 1)
            .await,
        Err(StoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn filesystem_bindings_regular_files_and_symlink_children_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let store = FileStore::for_project(root.path().join("board"), scope()).unwrap();
    assert!(matches!(
        store.get(&Scope::new("brand", "other").unwrap(), "a").await,
        Err(StoreError::Invalid(_))
    ));
    std::fs::create_dir_all(root.path().join("board/directory")).unwrap();
    assert!(matches!(
        store.get(&scope(), "directory").await,
        Err(StoreError::Invalid(_))
    ));
    std::os::unix::fs::symlink(root.path().join("outside"), root.path().join("board/link"))
        .unwrap();
    assert!(matches!(
        store.get(&scope(), "link").await,
        Err(StoreError::Invalid(_))
    ));
    assert!(store
        .list(&scope(), "", None, 100)
        .await
        .unwrap()
        .paths
        .is_empty());
    let empty = FileStore::new(root.path().join("empty")).unwrap();
    assert!(empty
        .list(&scope(), "", None, 100)
        .await
        .unwrap()
        .paths
        .is_empty());
}
