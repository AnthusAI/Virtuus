use crate::{
    compare_numbers, content_hash, validate_limit, Card, CatalogQuery, JournaledStore, Result,
    Scope, StoreError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use virtuus::sort::OrderedValue;
use virtuus::SortCondition;

/// Explicit caller-authorized projects belonging to exactly one tenant.
/// This validates scope selection; callers must obtain grants from their auth layer.
#[derive(Debug, Clone)]
pub struct AuthorizedProjects {
    tenant: String,
    scopes: Vec<Scope>,
}

impl AuthorizedProjects {
    /// Construct a bounded, distinct authorization set; never infer grants by scanning storage.
    pub fn new(tenant: impl Into<String>, scopes: Vec<Scope>) -> Result<Self> {
        let tenant = tenant.into();
        Scope::new(&tenant, "validation")?;
        if scopes.len() > 32 || scopes.iter().any(|s| s.tenant() != tenant) {
            return Err(StoreError::Invalid(
                "authorization set must contain at most 32 projects in one tenant".into(),
            ));
        }
        let scopes = scopes
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(Self { tenant, scopes })
    }

    fn select(&self, selection: &ProjectSelection) -> Result<Vec<Scope>> {
        match selection {
            ProjectSelection::All => Ok(self.scopes.clone()),
            ProjectSelection::Selected(projects) => {
                if projects.is_empty() || projects.len() > 32 {
                    return Err(StoreError::Invalid(
                        "select 1 to 32 authorized projects".into(),
                    ));
                }
                projects
                    .iter()
                    .map(|project| {
                        let scope = Scope::new(&self.tenant, project)?;
                        if self.scopes.contains(&scope) {
                            Ok(scope)
                        } else {
                            Err(StoreError::Invalid("project is not authorized".into()))
                        }
                    })
                    .collect::<Result<BTreeSet<_>>>()
                    .map(|scopes| scopes.into_iter().collect())
            }
        }
    }
}

/// One project, several selected projects, or the complete caller authorization set.
#[derive(Debug, Clone)]
pub enum ProjectSelection {
    /// Include every project supplied by the authorization layer.
    All,
    /// Include only the named project identifiers within that same tenant.
    Selected(Vec<String>),
}

/// Generic scalar residual predicate using the existing Virtuus comparison semantics.
#[derive(Debug)]
pub struct ScalarPredicate {
    /// Configured dotted field path in the card catalog.
    pub field: String,
    /// Existing equality, range, prefix, or substring condition.
    pub condition: SortCondition,
}

/// Scalar order, with scope/path providing a deterministic final tie-breaker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOrder {
    /// Configured dotted field path in the card catalog.
    pub field: String,
    /// Reverse this scalar ordering when true.
    pub descending: bool,
}

/// A bounded board query over indexed candidates and residual scalar metadata.
#[derive(Debug)]
pub struct BoardQuery {
    /// Type/status index selections; empty vectors include all values.
    pub candidates: CatalogQuery,
    /// Predicates evaluated after the primary candidate indexes.
    pub predicates: Vec<ScalarPredicate>,
    /// Scalar ordering, followed by scope/path; empty means scope/path order.
    pub order: Vec<QueryOrder>,
    /// Accepted candidate count, including pagination lookahead.
    /// Queries reject above this threshold rather than returning incomplete ordering.
    /// A final bounded backend page may overshoot; this is not a hard read quota.
    pub max_candidates: usize,
}

impl Default for BoardQuery {
    fn default() -> Self {
        Self {
            candidates: CatalogQuery::default(),
            predicates: Vec::new(),
            order: Vec::new(),
            max_candidates: 10_000,
        }
    }
}

/// A card with its project scope, so matching issue IDs in different projects stay distinct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScopedCard {
    /// Tenant/project which owns this card.
    pub scope: Scope,
    /// Supplemental metadata only; canonical bytes remain in the object store.
    pub card: Card,
}

/// Deterministic merged board page with actual backend work counters.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedCatalogPage {
    /// Ordered and residual-filtered scoped cards.
    pub cards: Vec<ScopedCard>,
    /// Query-bound keyset cursor; this is not a snapshot across concurrent mutations.
    pub next_cursor: Option<String>,
    /// Catalog backend requests performed for this page.
    pub requests: usize,
    /// Candidate rows read before residual filtering.
    pub candidates: usize,
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    query_hash: String,
    last: ScopedCard,
}

fn compare(a: &ScopedCard, b: &ScopedCard, order: &[QueryOrder]) -> Ordering {
    for field in order {
        let lhs = OrderedValue(
            a.card
                .fields
                .get(&field.field)
                .cloned()
                .unwrap_or(Value::Null),
        );
        let rhs = OrderedValue(
            b.card
                .fields
                .get(&field.field)
                .cloned()
                .unwrap_or(Value::Null),
        );
        let result = compare_numbers(&lhs.0, &rhs.0).unwrap_or_else(|| lhs.cmp(&rhs));
        let result = if field.descending {
            result.reverse()
        } else {
            result
        };
        if result != Ordering::Equal {
            return result;
        }
    }
    a.scope
        .cmp(&b.scope)
        .then_with(|| a.card.path.cmp(&b.card.path))
}

fn scalar_matches(condition: &SortCondition, value: &Value) -> bool {
    let numeric = |other: &Value| compare_numbers(value, other);
    match condition {
        SortCondition::Eq(other) if value.is_number() && other.is_number() => {
            numeric(other).is_some_and(Ordering::is_eq)
        }
        SortCondition::Ne(other) if value.is_number() && other.is_number() => {
            numeric(other).is_some_and(|o| !o.is_eq())
        }
        SortCondition::Lt(other) if value.is_number() && other.is_number() => {
            numeric(other).is_some_and(Ordering::is_lt)
        }
        SortCondition::Lte(other) if value.is_number() && other.is_number() => {
            numeric(other).is_some_and(|o| !o.is_gt())
        }
        SortCondition::Gt(other) if value.is_number() && other.is_number() => {
            numeric(other).is_some_and(Ordering::is_gt)
        }
        SortCondition::Gte(other) if value.is_number() && other.is_number() => {
            numeric(other).is_some_and(|o| !o.is_lt())
        }
        SortCondition::Between(low, high)
            if value.is_number() && low.is_number() && high.is_number() =>
        {
            numeric(low).is_some_and(|o| !o.is_lt()) && numeric(high).is_some_and(|o| !o.is_gt())
        }
        _ => condition.evaluate(value),
    }
}

impl JournaledStore {
    /// Merge bounded indexed candidates across caller-authorized projects, then filter/order.
    /// Arbitrary residual ordering reads all selected candidate summaries up to the explicit
    /// budget; it never downloads canonical objects or silently truncates a board.
    pub async fn query_authorized(
        &self,
        authorized: &AuthorizedProjects,
        selection: &ProjectSelection,
        query: &BoardQuery,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ScopedCatalogPage> {
        validate_limit(limit)?;
        let scopes = authorized.select(selection)?;
        let types = query.candidates.issue_types.iter().collect::<BTreeSet<_>>();
        let statuses = query.candidates.statuses.iter().collect::<BTreeSet<_>>();
        if scopes
            .len()
            .saturating_mul(types.len().max(1))
            .saturating_mul(statuses.len().max(1))
            > 32
            || !(1..=10_000).contains(&query.max_candidates)
            || query.predicates.len() > 32
            || query.order.len() > 8
        {
            return Err(StoreError::Invalid(
                "query exceeds project/index fanout, predicate, order, or candidate budget limits"
                    .into(),
            ));
        }
        if query
            .predicates
            .iter()
            .any(|p| p.field.is_empty() || p.field.len() > 128)
            || query
                .order
                .iter()
                .any(|o| o.field.is_empty() || o.field.len() > 128)
        {
            return Err(StoreError::Invalid("invalid query field".into()));
        }
        let number_supported =
            |value: &Value| !value.is_number() || compare_numbers(value, value).is_some();
        if query
            .predicates
            .iter()
            .any(|predicate| match &predicate.condition {
                SortCondition::Eq(value)
                | SortCondition::Ne(value)
                | SortCondition::Lt(value)
                | SortCondition::Lte(value)
                | SortCondition::Gt(value)
                | SortCondition::Gte(value) => !number_supported(value),
                SortCondition::Between(low, high) => {
                    !number_supported(low) || !number_supported(high)
                }
                SortCondition::BeginsWith(_) | SortCondition::Contains(_) => false,
            })
        {
            return Err(StoreError::Invalid(
                "query number exponent cannot be normalized safely".into(),
            ));
        }
        let query_hash = content_hash(&serde_json::to_vec(&(
            &scopes,
            &query.candidates,
            format!("{:?}", query.predicates),
            &query.order,
            query.max_candidates,
        ))?);
        let cursor = cursor
            .map(|cursor| {
                if cursor.len() > 32_768 {
                    return Err(StoreError::Invalid("cursor too large".into()));
                }
                let cursor: Cursor = serde_json::from_str(cursor)
                    .map_err(|_| StoreError::Invalid("invalid query cursor".into()))?;
                if cursor.query_hash != query_hash || !scopes.contains(&cursor.last.scope) {
                    return Err(StoreError::Invalid(
                        "cursor belongs to a different query or authorization set".into(),
                    ));
                }
                Ok(cursor)
            })
            .transpose()?;
        let mut requests = 0;
        let mut candidates = 0;
        let mut cards = Vec::new();
        for scope in scopes {
            let mut catalog_cursor = None;
            loop {
                let streams = types.len().max(1).saturating_mul(statuses.len().max(1));
                let remaining = query.max_candidates.saturating_sub(candidates);
                let page_limit = (remaining / streams).saturating_sub(1).clamp(1, 100);
                let page = self
                    .query(
                        &scope,
                        &query.candidates,
                        catalog_cursor.as_deref(),
                        page_limit,
                    )
                    .await?;
                requests += page.requests;
                candidates += page.candidates;
                if candidates > query.max_candidates {
                    return Err(StoreError::Invalid(
                        "candidate budget exceeded; narrow the indexed type/status selection"
                            .into(),
                    ));
                }
                for card in page.cards {
                    if query
                        .predicates
                        .iter()
                        .any(|p| card.omitted_fields.contains(&p.field))
                        || query
                            .order
                            .iter()
                            .any(|o| card.omitted_fields.contains(&o.field))
                    {
                        return Err(StoreError::Invalid("query requires scalar metadata omitted by the catalog budget; hydrate canonical data within a read budget".into()));
                    }
                    if query.predicates.iter().any(|p| {
                        card.fields
                            .get(&p.field)
                            .is_some_and(|v| v.is_number() && compare_numbers(v, v).is_none())
                    }) || query.order.iter().any(|o| {
                        card.fields
                            .get(&o.field)
                            .is_some_and(|v| v.is_number() && compare_numbers(v, v).is_none())
                    }) {
                        return Err(StoreError::Invalid(
                            "query number exponent cannot be normalized safely".into(),
                        ));
                    }
                    if query.predicates.iter().all(|p| {
                        scalar_matches(
                            &p.condition,
                            card.fields.get(&p.field).unwrap_or(&Value::Null),
                        )
                    }) {
                        cards.push(ScopedCard {
                            scope: scope.clone(),
                            card,
                        });
                    }
                }
                catalog_cursor = page.next_cursor;
                if catalog_cursor.is_none() {
                    break;
                }
            }
        }
        cards.sort_by(|a, b| compare(a, b, &query.order));
        if let Some(cursor) = cursor {
            cards.retain(|card| compare(card, &cursor.last, &query.order) == Ordering::Greater);
        }
        let has_more = cards.len() > limit;
        cards.truncate(limit);
        let next_cursor = if has_more {
            cards
                .last()
                .map(|last| {
                    serde_json::to_string(&Cursor {
                        query_hash,
                        last: last.clone(),
                    })
                })
                .transpose()?
        } else {
            None
        };
        Ok(ScopedCatalogPage {
            cards,
            next_cursor,
            requests,
            candidates,
        })
    }
}
