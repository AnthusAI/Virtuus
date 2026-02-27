//! Keyword search index for searchable fields.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// In-memory keyword index for a table's searchable fields.
#[derive(Debug, Clone, Default)]
pub struct SearchIndex {
    fields: Vec<String>,
    tokens: HashMap<String, Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedIndex {
    fields: Vec<String>,
    tokens: HashMap<String, Vec<String>>,
}

impl SearchIndex {
    /// Create a new search index for the provided fields.
    pub fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            tokens: HashMap::new(),
        }
    }

    /// Return the configured searchable fields.
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Insert tokens from a record into the index.
    pub fn index_record(&mut self, pk: &str, record: &Value) {
        let fields = self.fields.clone();
        for field in fields {
            if let Some(value) = record.get(&field) {
                self.index_value(pk, value);
            }
        }
    }

    /// Remove tokens for a record from the index.
    pub fn remove_record(&mut self, pk: &str, record: &Value) {
        let mut to_remove = Vec::new();
        for field in &self.fields {
            if let Some(value) = record.get(field) {
                for token in tokens_from_value(value) {
                    if let Some(postings) = self.tokens.get_mut(&token) {
                        postings.retain(|entry| entry != pk);
                        if postings.is_empty() {
                            to_remove.push(token.clone());
                        }
                    }
                }
            }
        }
        for token in to_remove {
            self.tokens.remove(&token);
        }
    }

    /// Search the index for a query string.
    pub fn search(&self, query: &str) -> Vec<String> {
        let mut tokens = tokenize(query);
        if tokens.is_empty() {
            return Vec::new();
        }
        tokens.sort();
        tokens.dedup();
        let mut postings_sets: Vec<Vec<String>> = tokens
            .iter()
            .filter_map(|token| self.tokens.get(token).cloned())
            .collect();
        if postings_sets.len() != tokens.len() {
            return Vec::new();
        }
        postings_sets.sort_by_key(|set| set.len());
        let mut result: Vec<String> = postings_sets.first().cloned().unwrap_or_default();
        for postings in postings_sets.iter().skip(1) {
            let lookup: std::collections::HashMap<&str, ()> =
                postings.iter().map(|pk| (pk.as_str(), ())).collect();
            result.retain(|pk| lookup.contains_key(pk.as_str()));
            if result.is_empty() {
                break;
            }
        }
        result.sort();
        result
    }

    /// Load a persisted search index from disk.
    pub fn load(path: &Path) -> Option<Self> {
        let data = std::fs::read_to_string(path).ok()?;
        let persisted: PersistedIndex = serde_json::from_str(&data).ok()?;
        Some(Self {
            fields: persisted.fields,
            tokens: persisted.tokens,
        })
    }

    /// Persist the search index to disk.
    pub fn persist(&self, path: &Path) -> Result<(), String> {
        let payload = PersistedIndex {
            fields: self.fields.clone(),
            tokens: self.tokens.clone(),
        };
        let data = serde_json::to_string(&payload).map_err(|err| err.to_string())?;
        std::fs::write(path, data).map_err(|err| err.to_string())
    }

    fn index_value(&mut self, pk: &str, value: &Value) {
        for token in tokens_from_value(value) {
            let postings = self.tokens.entry(token).or_default();
            if !postings.iter().any(|entry| entry == pk) {
                postings.push(pk.to_string());
            }
        }
    }
}

fn tokens_from_value(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => tokenize(text),
        Value::Array(items) => items
            .iter()
            .flat_map(tokens_from_value)
            .collect::<Vec<String>>(),
        _ => Vec::new(),
    }
}

fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            for lower in ch.to_lowercase() {
                current.push(lower);
            }
        } else if !current.is_empty() {
            tokens.push(current.clone());
            current.clear();
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tokenize_splits_on_non_alnum() {
        let tokens = tokenize("Hello, world! 123");
        assert_eq!(tokens, vec!["hello", "world", "123"]);
    }

    #[test]
    fn index_and_search() {
        let mut index = SearchIndex::new(vec!["title".to_string()]);
        index.index_record("a", &json!({"title": "Alpha Beta"}));
        index.index_record("b", &json!({"title": "Beta Gamma"}));
        let results = index.search("alpha beta");
        assert_eq!(results, vec!["a".to_string()]);
    }

    #[test]
    fn fields_remove_and_tokens_from_values() {
        let mut index = SearchIndex::new(vec![
            "title".to_string(),
            "tags".to_string(),
            "count".to_string(),
        ]);
        assert_eq!(
            index.fields(),
            &vec!["title".to_string(), "tags".to_string(), "count".to_string()]
        );
        let record = json!({"title": "Alpha", "tags": ["Beta", "Gamma"], "count": 5});
        index.index_record("a", &record);
        index.remove_record("a", &record);
        assert!(index.search("alpha").is_empty());
    }

    #[test]
    fn search_handles_empty_missing_and_no_overlap() {
        let mut index = SearchIndex::new(vec!["title".to_string()]);
        index.index_record("a", &json!({"title": "Alpha"}));
        index.index_record("b", &json!({"title": "Beta"}));
        assert!(index.search("").is_empty());
        assert!(index.search("gamma").is_empty());
        assert!(index.search("alpha beta").is_empty());
    }

    #[test]
    fn persist_and_load_index() {
        let dir = std::env::temp_dir().join("virtuus_search_index");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("index.json");
        let mut index = SearchIndex::new(vec!["title".to_string()]);
        index.index_record("a", &json!({"title": "Alpha"}));
        index.persist(&path).unwrap();
        let loaded = SearchIndex::load(&path).unwrap();
        assert_eq!(loaded.fields(), &vec!["title".to_string()]);
        assert_eq!(loaded.search("alpha"), vec!["a".to_string()]);
    }

    #[test]
    fn persist_reports_write_errors() {
        let index = SearchIndex::new(vec!["title".to_string()]);
        let dir = std::env::temp_dir().join("virtuus_search_missing_dir");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("missing").join("index.json");
        assert!(index.persist(&path).is_err());
    }
}
