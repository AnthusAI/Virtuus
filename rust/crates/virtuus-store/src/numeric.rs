use serde_json::Value;

/// Compare JSON numbers by exact decimal digits, without conversion to binary floats.
/// Returns `None` for non-numbers or an exponent/scale outside signed 64-bit bounds.
pub fn compare_numbers(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    fn normalized(value: &Value) -> Option<(bool, i64, Vec<u8>)> {
        let text = value.as_number()?.to_string();
        let negative = text.starts_with('-');
        let unsigned = text.trim_start_matches('-');
        let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
            Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
            None => (unsigned, 0),
        };
        let integer_length = mantissa.find('.').unwrap_or(mantissa.len()) as i64;
        let digits: Vec<u8> = mantissa.bytes().filter(|byte| *byte != b'.').collect();
        let leading = digits
            .iter()
            .position(|byte| *byte != b'0')
            .unwrap_or(digits.len());
        if leading == digits.len() {
            return Some((false, 0, vec![b'0']));
        }
        let end = digits.iter().rposition(|byte| *byte != b'0')? + 1;
        Some((
            negative,
            integer_length
                .checked_add(exponent)?
                .checked_sub(leading as i64)?,
            digits[leading..end].to_vec(),
        ))
    }
    let (left_negative, left_scale, left_digits) = normalized(left)?;
    let (right_negative, right_scale, right_digits) = normalized(right)?;
    if left_negative != right_negative {
        return Some(right_negative.cmp(&left_negative));
    }
    let ordering = if left_digits == *b"0" && right_digits != *b"0" {
        std::cmp::Ordering::Less
    } else if right_digits == *b"0" && left_digits != *b"0" {
        std::cmp::Ordering::Greater
    } else {
        left_scale.cmp(&right_scale).then_with(|| {
            (0..left_digits.len().max(right_digits.len()))
                .map(|index| {
                    left_digits
                        .get(index)
                        .copied()
                        .unwrap_or(b'0')
                        .cmp(&right_digits.get(index).copied().unwrap_or(b'0'))
                })
                .find(|order| !order.is_eq())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    };
    Some(if left_negative {
        ordering.reverse()
    } else {
        ordering
    })
}
