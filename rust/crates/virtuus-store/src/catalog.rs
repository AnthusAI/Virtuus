use crate::{
    validate_fields, validate_limit, validate_path, Card, Catalog, CatalogPage, CatalogQuery,
    Result, Scope, StoreError, WriteCondition,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Rebuildable process-local catalog suitable for a filesystem-backed application.
#[derive(Default)]
pub struct MemoryCatalog {
    cards: Mutex<BTreeMap<(Scope, String), Card>>,
}

#[async_trait]
impl Catalog for MemoryCatalog {
    async fn get(&self, scope: &Scope, path: &str) -> Result<Option<Card>> {
        validate_path(path)?;
        Ok(self
            .cards
            .lock()
            .map_err(|_| StoreError::Unavailable("catalog lock poisoned".into()))?
            .get(&(scope.clone(), path.into()))
            .cloned())
    }

    async fn put(&self, scope: &Scope, card: &Card, condition: WriteCondition) -> Result<()> {
        validate_path(&card.path)?;
        validate_fields(&card.fields)?;
        crate::validate_omitted_fields(&card.omitted_fields)?;
        let mut cards = self
            .cards
            .lock()
            .map_err(|_| StoreError::Unavailable("catalog lock poisoned".into()))?;
        let key = (scope.clone(), card.path.clone());
        let matches = match condition {
            WriteCondition::Absent => !cards.contains_key(&key),
            WriteCondition::Revision(expected) => cards
                .get(&key)
                .is_some_and(|card| card.revision == expected),
        };
        if !matches {
            return Err(StoreError::Conflict);
        }
        cards.insert(key, card.clone());
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
        let cards = self
            .cards
            .lock()
            .map_err(|_| StoreError::Unavailable("catalog lock poisoned".into()))?;
        let mut candidates = 0;
        let mut selected: Vec<Card> = cards
            .range((scope.clone(), cursor.unwrap_or("").to_owned())..)
            .take_while(|((card_scope, _), _)| card_scope == scope)
            .filter(|((_, path), card)| {
                if cursor.is_some_and(|cursor| path.as_str() <= cursor) {
                    return false;
                }
                candidates += 1;
                (query.issue_types.is_empty()
                    || card
                        .fields
                        .get("type")
                        .and_then(|v| v.as_str())
                        .is_some_and(|kind| query.issue_types.iter().any(|v| v == kind)))
                    && (query.statuses.is_empty()
                        || card
                            .fields
                            .get("status")
                            .and_then(|v| v.as_str())
                            .is_some_and(|status| query.statuses.iter().any(|v| v == status)))
            })
            .map(|(_, card)| card.clone())
            .take(limit + 1)
            .collect();
        let next_cursor = if selected.len() > limit {
            selected.truncate(limit);
            selected.last().map(|card| card.path.clone())
        } else {
            None
        };
        Ok(CatalogPage {
            cards: selected,
            next_cursor,
            requests: 1,
            candidates,
        })
    }

    async fn delete(&self, scope: &Scope, path: &str, expected_revision: &str) -> Result<()> {
        validate_path(path)?;
        let mut cards = self
            .cards
            .lock()
            .map_err(|_| StoreError::Unavailable("catalog lock poisoned".into()))?;
        let key = (scope.clone(), path.to_owned());
        if !cards
            .get(&key)
            .is_some_and(|card| card.revision == expected_revision)
        {
            return Err(StoreError::Conflict);
        }
        cards.remove(&key);
        Ok(())
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn poisoned_catalog_never_returns_or_mutates_untrusted_state() {
        let catalog = Arc::new(MemoryCatalog::default());
        let other = catalog.clone();
        assert!(std::thread::spawn(move || {
            let _guard = other.cards.lock().unwrap();
            panic!("synthetic interrupted catalog writer");
        })
        .join()
        .is_err());
        let scope = Scope::new("brand", "board").unwrap();
        let card = Card {
            path: "project/issues/a.json".into(),
            revision: "r".into(),
            hash: "h".into(),
            byte_length: 1,
            fields: BTreeMap::new(),
            omitted_fields: vec![],
        };
        assert!(matches!(
            catalog.get(&scope, &card.path).await,
            Err(StoreError::Unavailable(_))
        ));
        assert!(matches!(
            catalog.put(&scope, &card, WriteCondition::Absent).await,
            Err(StoreError::Unavailable(_))
        ));
        assert!(matches!(
            catalog
                .query(&scope, &CatalogQuery::default(), None, 1)
                .await,
            Err(StoreError::Unavailable(_))
        ));
        assert!(matches!(
            catalog.delete(&scope, &card.path, "r").await,
            Err(StoreError::Unavailable(_))
        ));
    }
}
