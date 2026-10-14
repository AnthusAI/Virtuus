use std::collections::BTreeMap;
use std::sync::Arc;
use virtuus_store::{
    AuthorizedProjects, BoardQuery, Card, Catalog, CatalogQuery, FileStore, JournaledStore,
    MemoryCatalog, ProjectSelection, QueryOrder, ScalarPredicate, Scope, WriteCondition,
};

#[tokio::test]
async fn adjacent_large_integers_order_and_filter_exactly() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Arc::new(MemoryCatalog::default());
    let store = JournaledStore::new(
        Arc::new(FileStore::new(root.path()).unwrap()),
        catalog.clone(),
    );
    let scope = Scope::new("brand", "app").unwrap();
    for (path, value) in [("a", 9007199254740993_u64), ("z", 9007199254740992_u64)] {
        catalog
            .put(
                &scope,
                &Card {
                    omitted_fields: Vec::new(),
                    path: format!("project/issues/{path}.json"),
                    revision: "r".into(),
                    hash: "h".into(),
                    byte_length: 1,
                    fields: BTreeMap::from([("custom.number".into(), value.into())]),
                },
                WriteCondition::Absent,
            )
            .await
            .unwrap();
    }
    let authorized = AuthorizedProjects::new("brand", vec![scope]).unwrap();
    let mut query = BoardQuery {
        candidates: CatalogQuery::default(),
        predicates: vec![],
        order: vec![QueryOrder {
            field: "custom.number".into(),
            descending: false,
        }],
        max_candidates: 100,
    };
    let page = store
        .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
        .await
        .unwrap();
    assert!(page.cards[0].card.path.ends_with("z.json"));
    query.predicates.push(ScalarPredicate {
        field: "custom.number".into(),
        condition: virtuus::SortCondition::Lt(9007199254740993_u64.into()),
    });
    assert_eq!(
        store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
            .await
            .unwrap()
            .cards
            .len(),
        1
    );
    query.predicates[0].condition = virtuus::SortCondition::Gt(9007199254740992_u64.into());
    let page = store
        .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
        .await
        .unwrap();
    assert_eq!(page.cards.len(), 1);
    assert!(page.cards[0].card.path.ends_with("a.json"));
    query.predicates[0].condition =
        virtuus::SortCondition::Lt(serde_json::from_str("1e9223372036854775808").unwrap());
    assert!(matches!(
        store
            .query_authorized(&authorized, &ProjectSelection::All, &query, None, 10)
            .await,
        Err(virtuus_store::StoreError::Invalid(_))
    ));
}

#[test]
fn exact_decimal_helper_normalizes_exponents_zero_sign_and_integer_float_equivalence() {
    use std::cmp::Ordering::*;
    use virtuus_store::compare_numbers;
    let value = |text: &str| serde_json::from_str::<serde_json::Value>(text).unwrap();
    for (a, b, expected) in [
        ("1", "1.0", Equal),
        ("-0.0", "0", Equal),
        ("-2e400", "-1e400", Less),
        ("1e-400", "0", Greater),
        ("1.25", "1.25000000000000001", Less),
        ("18446744073709551616", "18446744073709551617", Less),
        ("12e2", "1200", Equal),
    ] {
        assert_eq!(compare_numbers(&value(a), &value(b)), Some(expected));
        assert_eq!(
            compare_numbers(&value(b), &value(a)),
            Some(expected.reverse())
        );
    }
    assert_eq!(compare_numbers(&value("1"), &serde_json::json!("1")), None);
}
