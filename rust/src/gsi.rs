//! Global Secondary Index implementation.

use std::collections::HashMap;

use serde_json::Value;

use crate::sort::{OrderedValue, SortCondition};

#[derive(Debug, Clone, PartialEq)]
struct GsiEntry {
    pk: String,
    sort_value: Option<Value>,
}

/// Global Secondary Index with hash partition and optional range key.
#[derive(Debug, Clone)]
pub struct Gsi {
    name: String,
    partition_key: String,
    sort_key: Option<String>,
    buckets: HashMap<String, Vec<GsiEntry>>,
}

impl Gsi {
    /// Create a new GSI definition.
    ///
    /// :param name: Index name.
    /// :param partition_key: Partition key field.
    /// :param sort_key: Optional sort key field.
    pub fn new(name: &str, partition_key: &str, sort_key: Option<&str>) -> Self {
        Self {
            name: name.to_string(),
            partition_key: partition_key.to_string(),
            sort_key: sort_key.map(|s| s.to_string()),
            buckets: HashMap::new(),
        }
    }

    /// Return the index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the partition key field name.
    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    /// Return the sort key field name, if present.
    pub fn sort_key(&self) -> Option<&str> {
        self.sort_key.as_deref()
    }

    /// Insert a record into the index.
    pub fn put(&mut self, pk: &str, record: &Value) {
        let sort_value = self.extract_sort_value(record);
        if self.sort_key.is_some() && sort_value.is_none() {
            return;
        }
        for partition_value in select_values(record, &self.partition_key) {
            let key = partition_key(&partition_value);
            let bucket = self.buckets.entry(key).or_default();
            bucket.push(GsiEntry {
                pk: pk.to_string(),
                sort_value: sort_value.clone(),
            });
        }
    }

    /// Remove a record from the index.
    pub fn remove(&mut self, pk: &str, record: &Value) {
        let sort_value = self.extract_sort_value(record);
        if self.sort_key.is_some() && sort_value.is_none() {
            return;
        }
        for partition_value in select_values(record, &self.partition_key) {
            let key = partition_key(&partition_value);
            let remove_bucket = if let Some(bucket) = self.buckets.get_mut(&key) {
                bucket.retain(|entry| !(entry.pk == pk && entry.sort_value == sort_value));
                bucket.is_empty()
            } else {
                false
            };
            if remove_bucket {
                self.buckets.remove(&key);
            }
        }
    }

    /// Update an indexed record.
    pub fn update(&mut self, pk: &str, old_record: &Value, new_record: &Value) {
        self.remove(pk, old_record);
        self.put(pk, new_record);
    }

    /// Query by partition key with optional sort condition and direction.
    pub fn query(
        &self,
        partition_value: &Value,
        sort_condition: Option<&SortCondition>,
        descending: bool,
    ) -> Vec<String> {
        let key = partition_key(partition_value);
        let mut bucket = match self.buckets.get(&key) {
            Some(entries) => entries.clone(),
            None => return Vec::new(),
        };
        if let Some(condition) = sort_condition {
            bucket.retain(|entry| match entry.sort_value.as_ref() {
                Some(value) => condition.evaluate(value),
                None => false,
            });
        }
        if self.sort_key.is_some() {
            bucket.sort_by(|a, b| {
                OrderedValue(a.sort_value.clone().unwrap_or(Value::Null))
                    .cmp(&OrderedValue(b.sort_value.clone().unwrap_or(Value::Null)))
            });
        }
        if descending {
            bucket.reverse();
        }
        bucket.into_iter().map(|entry| entry.pk).collect()
    }

    /// Return entries for a partition key (pk + sort value).
    pub fn entries(&self, partition_value: &Value) -> Vec<(String, Option<Value>)> {
        let key = partition_key(partition_value);
        self.buckets
            .get(&key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|entry| (entry.pk, entry.sort_value))
            .collect()
    }

    fn extract_sort_value(&self, record: &Value) -> Option<Value> {
        let sort_key = self.sort_key.as_ref()?;
        select_values(record, sort_key).into_iter().next()
    }
}

/// Select scalar values with dot paths, array wildcards, and array filters.
/// Examples: `labels[*]` and `dependencies[dependency_type=blocked-by].target`.
fn select_values(record: &Value, selector: &str) -> Vec<Value> {
    let mut current = vec![record.clone()];
    for segment in selector.split('.') {
        let (field, filter) = match segment.split_once('[') {
            Some((field, rest)) => (field, Some(rest.trim_end_matches(']'))),
            None => (segment, None),
        };
        let mut next = Vec::new();
        for value in current {
            let Some(field_value) = value.get(field) else {
                continue;
            };
            match filter {
                None => next.push(field_value.clone()),
                Some("*") => {
                    if let Value::Array(items) = field_value {
                        next.extend(items.clone());
                    }
                }
                Some(condition) => {
                    let Some((key, expected)) = condition.split_once('=') else {
                        continue;
                    };
                    if let Value::Array(items) = field_value {
                        next.extend(
                            items
                                .iter()
                                .filter(|item| {
                                    item.get(key).and_then(Value::as_str) == Some(expected)
                                })
                                .cloned(),
                        );
                    }
                }
            }
        }
        current = next;
    }
    current
}

fn get_field(record: &Value, key: &str) -> Option<Value> {
    select_values(record, key).into_iter().next()
}

fn partition_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{}\"", value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_hash_only_gsi() {
        let gsi = Gsi::new("by_status", "status", None);
        assert_eq!(gsi.name(), "by_status");
        assert_eq!(gsi.partition_key(), "status");
        assert!(gsi.sort_key().is_none());
    }

    #[test]
    fn create_hash_range_gsi() {
        let gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        assert_eq!(gsi.sort_key(), Some("created_at"));
    }

    #[test]
    fn put_and_query_hash_only() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.put("user-1", &json!({"status": "active"}));
        let result = gsi.query(&json!("active"), None, false);
        assert_eq!(result, vec!["user-1".to_string()]);
    }

    #[test]
    fn indexes_array_and_filtered_nested_values() {
        let record = json!({"labels":["a","b"],"dependencies":[{"dependency_type":"blocked-by","target":"one"},{"dependency_type":"blocks","target":"two"}]});
        let mut labels = Gsi::new("labels", "labels[*]", None);
        labels.put("issue", &record);
        assert_eq!(labels.query(&json!("b"), None, false), vec!["issue"]);
        let mut blocked = Gsi::new(
            "blocked",
            "dependencies[dependency_type=blocked-by].target",
            None,
        );
        blocked.put("issue", &record);
        assert_eq!(blocked.query(&json!("one"), None, false), vec!["issue"]);
        assert!(blocked.query(&json!("two"), None, false).is_empty());
    }

    #[test]
    fn missing_partition_key_is_skipped() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.put("user-1", &json!({"name": "Alice"}));
        let result = gsi.query(&json!("active"), None, false);
        assert!(result.is_empty());
    }

    #[test]
    fn missing_sort_key_is_skipped() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.put("user-1", &json!({"org_id": "org-a"}));
        let result = gsi.query(&json!("org-a"), None, false);
        assert!(result.is_empty());
    }

    #[test]
    fn remove_missing_partition_key_is_ignored() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.remove("user-1", &json!({"name": "Alice"}));
        let result = gsi.query(&json!("active"), None, false);
        assert!(result.is_empty());
    }

    #[test]
    fn remove_missing_sort_key_is_ignored() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.remove("user-1", &json!({"org_id": "org-a"}));
        let result = gsi.query(&json!("org-a"), None, false);
        assert!(result.is_empty());
    }

    #[test]
    fn remove_from_missing_bucket_is_ignored() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.remove("user-1", &json!({"status": "active"}));
        let result = gsi.query(&json!("active"), None, false);
        assert!(result.is_empty());
    }

    #[test]
    fn sort_condition_skips_missing_sort_value() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.put("user-1", &json!({"status": "active"}));
        let condition = SortCondition::Eq(json!("active"));
        let result = gsi.query(&json!("active"), Some(&condition), false);
        assert!(result.is_empty());
    }

    #[test]
    fn non_object_record_is_ignored() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.put("user-1", &json!(["status", "active"]));
        let result = gsi.query(&json!("active"), None, false);
        assert!(result.is_empty());
    }

    #[test]
    fn remove_entry() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.put("user-1", &json!({"status": "active"}));
        gsi.remove("user-1", &json!({"status": "active"}));
        let result = gsi.query(&json!("active"), None, false);
        assert!(result.is_empty());
    }

    #[test]
    fn update_reindexes_partition() {
        let mut gsi = Gsi::new("by_status", "status", None);
        gsi.put("user-1", &json!({"status": "active"}));
        gsi.update(
            "user-1",
            &json!({"status": "active"}),
            &json!({"status": "inactive"}),
        );
        let active = gsi.query(&json!("active"), None, false);
        let inactive = gsi.query(&json!("inactive"), None, false);
        assert!(active.is_empty());
        assert_eq!(inactive, vec!["user-1".to_string()]);
    }

    #[test]
    fn hash_range_sorted_query() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.put(
            "user-1",
            &json!({"org_id": "org-a", "created_at": "2025-03-01"}),
        );
        gsi.put(
            "user-2",
            &json!({"org_id": "org-a", "created_at": "2025-01-15"}),
        );
        gsi.put(
            "user-3",
            &json!({"org_id": "org-a", "created_at": "2025-06-20"}),
        );
        let result = gsi.query(&json!("org-a"), None, false);
        assert_eq!(
            result,
            vec![
                "user-2".to_string(),
                "user-1".to_string(),
                "user-3".to_string()
            ]
        );
    }

    #[test]
    fn hash_range_descending_query() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.put(
            "user-1",
            &json!({"org_id": "org-a", "created_at": "2025-01-01"}),
        );
        gsi.put(
            "user-2",
            &json!({"org_id": "org-a", "created_at": "2025-06-01"}),
        );
        gsi.put(
            "user-3",
            &json!({"org_id": "org-a", "created_at": "2025-12-01"}),
        );
        let result = gsi.query(&json!("org-a"), None, true);
        assert_eq!(
            result,
            vec![
                "user-3".to_string(),
                "user-2".to_string(),
                "user-1".to_string()
            ]
        );
    }

    #[test]
    fn hash_range_sort_condition() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.put(
            "user-1",
            &json!({"org_id": "org-a", "created_at": "2025-01-01"}),
        );
        gsi.put(
            "user-2",
            &json!({"org_id": "org-a", "created_at": "2025-06-01"}),
        );
        gsi.put(
            "user-3",
            &json!({"org_id": "org-a", "created_at": "2025-12-01"}),
        );
        let condition = SortCondition::Gte(json!("2025-06-01"));
        let result = gsi.query(&json!("org-a"), Some(&condition), false);
        assert_eq!(result, vec!["user-2".to_string(), "user-3".to_string()]);
    }

    #[test]
    fn hash_range_between_condition() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.put(
            "user-1",
            &json!({"org_id": "org-a", "created_at": "2025-01-01"}),
        );
        gsi.put(
            "user-2",
            &json!({"org_id": "org-a", "created_at": "2025-06-01"}),
        );
        gsi.put(
            "user-3",
            &json!({"org_id": "org-a", "created_at": "2025-12-01"}),
        );
        let condition = SortCondition::Between(json!("2025-03-01"), json!("2025-09-01"));
        let result = gsi.query(&json!("org-a"), Some(&condition), false);
        assert_eq!(result, vec!["user-2".to_string()]);
    }

    #[test]
    fn update_reindexes_sort_key() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.put(
            "user-1",
            &json!({"org_id": "org-a", "created_at": "2025-01-15"}),
        );
        gsi.update(
            "user-1",
            &json!({"org_id": "org-a", "created_at": "2025-01-15"}),
            &json!({"org_id": "org-a", "created_at": "2025-06-01"}),
        );
        let old_condition = SortCondition::Eq(json!("2025-01-15"));
        let new_condition = SortCondition::Eq(json!("2025-06-01"));
        let old_result = gsi.query(&json!("org-a"), Some(&old_condition), false);
        let new_result = gsi.query(&json!("org-a"), Some(&new_condition), false);
        assert!(old_result.is_empty());
        assert_eq!(new_result, vec!["user-1".to_string()]);
    }

    #[test]
    fn entries_return_pk_and_sort() {
        let mut gsi = Gsi::new("by_org", "org_id", Some("created_at"));
        gsi.put(
            "user-1",
            &json!({"org_id": "org-a", "created_at": "2025-01-15"}),
        );
        let entries = gsi.entries(&json!("org-a"));
        assert_eq!(
            entries,
            vec![("user-1".to_string(), Some(json!("2025-01-15")))]
        );
    }

    #[test]
    fn get_field_non_object_returns_none() {
        assert!(get_field(&json!(["array"]), "foo").is_none());
    }
}
