use std::collections::BTreeMap;
use std::sync::Arc;
use virtuus_store::{
    AuthorizedProjects, BoardQuery, Card, Catalog, CatalogQuery, FileStore, JournaledStore,
    MemoryCatalog, ProjectSelection, QueryOrder, ScalarPredicate, Scope, StoreError,
    WriteCondition,
};

#[tokio::test]
async fn selected_and_all_authorized_projects_page_deterministically_with_residual_ordering() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Arc::new(MemoryCatalog::default());
    let store = JournaledStore::new(
        Arc::new(FileStore::new(root.path()).unwrap()),
        catalog.clone(),
    );
    let a = Scope::new("brand", "a").unwrap();
    let b = Scope::new("brand", "b").unwrap();
    let other = Scope::new("other", "a").unwrap();
    for (scope, score, region) in [
        (&a, 3, "west"),
        (&b, 1, "west"),
        (&a, 2, "east"),
        (&other, 0, "west"),
    ] {
        catalog
            .put(
                scope,
                &Card {
                    omitted_fields: Vec::new(),
                    path: format!("project/issues/{score}.json"),
                    revision: "r".into(),
                    hash: "h".into(),
                    byte_length: 3,
                    fields: BTreeMap::from([
                        ("type".into(), "prospect".into()),
                        ("status".into(), "new".into()),
                        ("custom.score".into(), score.into()),
                        ("custom.region".into(), region.into()),
                    ]),
                },
                WriteCondition::Absent,
            )
            .await
            .unwrap();
    }
    let authorized = AuthorizedProjects::new("brand", vec![b, a]).unwrap();
    let query = BoardQuery {
        candidates: CatalogQuery {
            issue_types: vec!["prospect".into()],
            statuses: vec!["new".into()],
        },
        predicates: vec![ScalarPredicate {
            field: "custom.region".into(),
            condition: virtuus::SortCondition::Eq("west".into()),
        }],
        order: vec![QueryOrder {
            field: "custom.score".into(),
            descending: false,
        }],
        max_candidates: 100,
    };
    let page = store
        .query_authorized(&authorized, &ProjectSelection::All, &query, None, 1)
        .await
        .unwrap();
    assert_eq!(page.cards[0].card.fields["custom.score"], 1);
    assert_eq!(page.cards[0].scope.project(), "b");
    assert_eq!(page.candidates, 3);
    assert_eq!(page.requests, 2);
    let next = store
        .query_authorized(
            &authorized,
            &ProjectSelection::All,
            &query,
            page.next_cursor.as_deref(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(next.cards[0].card.fields["custom.score"], 3);
    assert!(next.next_cursor.is_none());
    assert!(matches!(
        store
            .query_authorized(
                &authorized,
                &ProjectSelection::Selected(vec!["missing".into()]),
                &query,
                None,
                1
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert!(matches!(
        store
            .query_authorized(
                &authorized,
                &ProjectSelection::Selected(vec!["a".into()]),
                &query,
                page.next_cursor.as_deref(),
                1
            )
            .await,
        Err(StoreError::Invalid(_))
    ));
}
