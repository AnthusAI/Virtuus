//! Sort condition predicates for DynamoDB-style range key filtering.

use serde_json::Value;

/// A newtype wrapper around [`serde_json::Value`] that implements [`Ord`].
///
/// JSON values are ordered as: Null < Bool(false) < Bool(true) < numbers (by numeric value)
/// < strings (lexicographic) < arrays < objects.  This ordering is used by
/// sorted GSI buckets and sort condition comparisons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedValue(pub Value);

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        value_rank(&self.0)
            .cmp(&value_rank(&other.0))
            .then_with(|| compare_same_kind(&self.0, &other.0))
    }
}

/// Returns a numeric rank for the variant of a JSON value (for cross-type ordering).
fn value_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// Compares two values of the same JSON kind.
fn compare_same_kind(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => {
            let xf = x.as_f64().unwrap_or(f64::NEG_INFINITY);
            let yf = y.as_f64().unwrap_or(f64::NEG_INFINITY);
            xf.partial_cmp(&yf).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Coerce a JSON [`Value`] to a number if it is a string that parses as one,
/// otherwise return it unchanged.
fn coerce(v: &Value) -> Value {
    if let Value::String(s) = v {
        if let Ok(n) = s.parse::<i64>() {
            return Value::Number(n.into());
        }
        if let Ok(f) = s.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Value::Number(n);
            }
        }
    }
    v.clone()
}

/// A sort condition predicate that can be evaluated against a JSON value.
#[derive(Debug)]
pub enum SortCondition {
    /// Matches values equal to the given value.
    Eq(Value),
    /// Matches values not equal to the given value.
    Ne(Value),
    /// Matches values strictly less than the given value.
    Lt(Value),
    /// Matches values less than or equal to the given value.
    Lte(Value),
    /// Matches values strictly greater than the given value.
    Gt(Value),
    /// Matches values greater than or equal to the given value.
    Gte(Value),
    /// Matches values in the inclusive range [low, high].
    Between(Value, Value),
    /// Matches string values that begin with the given prefix.
    BeginsWith(String),
    /// Matches string values that contain the given substring.
    Contains(String),
}

impl SortCondition {
    /// Evaluate this condition against a JSON value.
    ///
    /// Returns `false` for `Null` inputs for all operators.
    ///
    /// :param input: The value to test.
    /// :return: `true` if the condition is satisfied.
    pub fn evaluate(&self, input: &Value) -> bool {
        if input.is_null() {
            return false;
        }
        let coerced_input = coerce(input);
        match self {
            SortCondition::Eq(v) => OrderedValue(coerced_input) == OrderedValue(coerce(v)),
            SortCondition::Ne(v) => OrderedValue(coerced_input) != OrderedValue(coerce(v)),
            SortCondition::Lt(v) => OrderedValue(coerced_input) < OrderedValue(coerce(v)),
            SortCondition::Lte(v) => OrderedValue(coerced_input) <= OrderedValue(coerce(v)),
            SortCondition::Gt(v) => OrderedValue(coerced_input) > OrderedValue(coerce(v)),
            SortCondition::Gte(v) => OrderedValue(coerced_input) >= OrderedValue(coerce(v)),
            SortCondition::Between(low, high) => {
                let ov = OrderedValue(coerced_input);
                ov >= OrderedValue(coerce(low)) && ov <= OrderedValue(coerce(high))
            }
            SortCondition::BeginsWith(prefix) => match input {
                Value::String(s) => s.starts_with(prefix.as_str()),
                _ => false,
            },
            SortCondition::Contains(substr) => match input {
                Value::String(s) => s.contains(substr.as_str()),
                _ => false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- OrderedValue ordering ---

    #[test]
    fn ordered_null_null_equal() {
        // Use cmp() directly to exercise the Null-Null branch in compare_same_kind.
        assert_eq!(
            OrderedValue(Value::Null).cmp(&OrderedValue(Value::Null)),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn ordered_null_lt_bool() {
        assert!(OrderedValue(Value::Null) < OrderedValue(json!(false)));
    }

    #[test]
    fn ordered_bool_false_lt_true() {
        assert!(OrderedValue(json!(false)) < OrderedValue(json!(true)));
    }

    #[test]
    fn ordered_bool_equal() {
        assert_eq!(OrderedValue(json!(true)), OrderedValue(json!(true)));
    }

    #[test]
    fn ordered_numbers() {
        assert!(OrderedValue(json!(1)) < OrderedValue(json!(2)));
        assert_eq!(OrderedValue(json!(5)), OrderedValue(json!(5)));
        assert!(OrderedValue(json!(10)) > OrderedValue(json!(9)));
    }

    #[test]
    fn ordered_strings() {
        assert!(OrderedValue(json!("apple")) < OrderedValue(json!("banana")));
        assert_eq!(OrderedValue(json!("same")), OrderedValue(json!("same")));
    }

    #[test]
    fn ordered_number_lt_string() {
        assert!(OrderedValue(json!(42)) < OrderedValue(json!("hello")));
    }

    #[test]
    fn ordered_array_gt_string() {
        assert!(OrderedValue(json!([])) > OrderedValue(json!("z")));
    }

    #[test]
    fn ordered_object_gt_array() {
        assert!(OrderedValue(json!({})) > OrderedValue(json!([])));
    }

    #[test]
    fn ordered_mixed_kinds_equal_within_catch_all() {
        // Arrays of different contents — both rank 4, catch-all returns Equal
        assert_eq!(
            OrderedValue(json!([1])).cmp(&OrderedValue(json!([2]))),
            std::cmp::Ordering::Equal
        );
    }

    // --- coerce ---

    #[test]
    fn coerce_integer_string() {
        assert_eq!(coerce(&json!("42")), json!(42));
    }

    #[test]
    fn coerce_float_string() {
        // "1.5" parses as f64
        let result = coerce(&json!("1.5"));
        assert!(result.is_number());
    }

    #[test]
    fn coerce_non_numeric_string() {
        assert_eq!(coerce(&json!("hello")), json!("hello"));
    }

    #[test]
    fn coerce_non_string_unchanged() {
        assert_eq!(coerce(&json!(99)), json!(99));
    }

    #[test]
    fn coerce_null_unchanged() {
        assert_eq!(coerce(&Value::Null), Value::Null);
    }

    // --- SortCondition::evaluate ---

    #[test]
    fn eq_matches_equal_string() {
        let cond = SortCondition::Eq(json!("alice"));
        assert!(cond.evaluate(&json!("alice")));
        assert!(!cond.evaluate(&json!("bob")));
    }

    #[test]
    fn eq_matches_equal_number() {
        let cond = SortCondition::Eq(json!(42));
        assert!(cond.evaluate(&json!(42)));
        assert!(!cond.evaluate(&json!(43)));
    }

    #[test]
    fn eq_null_returns_false() {
        assert!(!SortCondition::Eq(json!("x")).evaluate(&Value::Null));
    }

    #[test]
    fn ne_matches() {
        let cond = SortCondition::Ne(json!("alice"));
        assert!(!cond.evaluate(&json!("alice")));
        assert!(cond.evaluate(&json!("bob")));
    }

    #[test]
    fn ne_null_returns_false() {
        assert!(!SortCondition::Ne(json!("x")).evaluate(&Value::Null));
    }

    #[test]
    fn lt_matches() {
        let cond = SortCondition::Lt(json!(10));
        assert!(cond.evaluate(&json!(5)));
        assert!(!cond.evaluate(&json!(10)));
        assert!(!cond.evaluate(&json!(15)));
    }

    #[test]
    fn lt_null_returns_false() {
        assert!(!SortCondition::Lt(json!(10)).evaluate(&Value::Null));
    }

    #[test]
    fn lte_matches() {
        let cond = SortCondition::Lte(json!(10));
        assert!(cond.evaluate(&json!(5)));
        assert!(cond.evaluate(&json!(10)));
        assert!(!cond.evaluate(&json!(15)));
    }

    #[test]
    fn lte_null_returns_false() {
        assert!(!SortCondition::Lte(json!(10)).evaluate(&Value::Null));
    }

    #[test]
    fn gt_matches() {
        let cond = SortCondition::Gt(json!(10));
        assert!(cond.evaluate(&json!(15)));
        assert!(!cond.evaluate(&json!(10)));
        assert!(!cond.evaluate(&json!(5)));
    }

    #[test]
    fn gt_null_returns_false() {
        assert!(!SortCondition::Gt(json!(10)).evaluate(&Value::Null));
    }

    #[test]
    fn gte_matches() {
        let cond = SortCondition::Gte(json!(10));
        assert!(cond.evaluate(&json!(15)));
        assert!(cond.evaluate(&json!(10)));
        assert!(!cond.evaluate(&json!(5)));
    }

    #[test]
    fn gte_null_returns_false() {
        assert!(!SortCondition::Gte(json!(10)).evaluate(&Value::Null));
    }

    #[test]
    fn between_inclusive_bounds() {
        let cond = SortCondition::Between(json!(5), json!(15));
        assert!(cond.evaluate(&json!(5)));
        assert!(cond.evaluate(&json!(10)));
        assert!(cond.evaluate(&json!(15)));
        assert!(!cond.evaluate(&json!(4)));
        assert!(!cond.evaluate(&json!(16)));
    }

    #[test]
    fn between_null_returns_false() {
        assert!(!SortCondition::Between(json!("a"), json!("z")).evaluate(&Value::Null));
    }

    #[test]
    fn begins_with_matches() {
        let cond = SortCondition::BeginsWith("user-".to_string());
        assert!(cond.evaluate(&json!("user-abc")));
        assert!(!cond.evaluate(&json!("admin-abc")));
    }

    #[test]
    fn begins_with_null_returns_false() {
        assert!(!SortCondition::BeginsWith("x".to_string()).evaluate(&Value::Null));
    }

    #[test]
    fn begins_with_non_string_returns_false() {
        assert!(!SortCondition::BeginsWith("x".to_string()).evaluate(&json!(42)));
    }

    #[test]
    fn contains_matches() {
        let cond = SortCondition::Contains("error".to_string());
        assert!(cond.evaluate(&json!("server error")));
        assert!(!cond.evaluate(&json!("all good")));
    }

    #[test]
    fn contains_null_returns_false() {
        assert!(!SortCondition::Contains("x".to_string()).evaluate(&Value::Null));
    }

    #[test]
    fn contains_non_string_returns_false() {
        assert!(!SortCondition::Contains("x".to_string()).evaluate(&json!(42)));
    }
}
