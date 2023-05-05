use dozer_types::json_types::JsonValue;
use regex::Regex;

/// compare sizes of json elements
/// The method expects to get a number on the right side and array or string or object on the left
/// where the number of characters, elements or fields will be compared respectively.
pub fn size(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if let Some(JsonValue::Number(n)) = right.get(0) {
        for el in left.iter() {
            match el {
                JsonValue::String(v) if v.len() == n.0 as usize => true,
                JsonValue::Array(elems) if elems.len() == n.0 as usize => true,
                JsonValue::Object(fields) if fields.len() == n.0 as usize => true,
                _ => return false,
            };
        }
        return true;
    }
    false
}

/// ensure the array on the left side is a subset of the array on the right side.
pub fn sub_set_of(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.is_empty() {
        return true;
    }
    if right.is_empty() {
        return false;
    }

    if let Some(elems) = left.first().and_then(|e| (*e).as_array()) {
        if let Some(JsonValue::Array(right_elems)) = right.get(0) {
            if right_elems.is_empty() {
                return false;
            }

            for el in elems {
                let mut res = false;

                for r in right_elems.iter() {
                    if el.eq(r) {
                        res = true
                    }
                }
                if !res {
                    return false;
                }
            }
            return true;
        }
    }
    false
}

/// ensure at least one element in the array  on the left side belongs to the array on the right side.
pub fn any_of(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.is_empty() {
        return true;
    }
    if right.is_empty() {
        return false;
    }

    if let Some(JsonValue::Array(elems)) = right.get(0) {
        if elems.is_empty() {
            return false;
        }

        for el in left.iter() {
            if let Some(left_elems) = el.as_array() {
                for l in left_elems.iter() {
                    for r in elems.iter() {
                        if l.eq(r) {
                            return true;
                        }
                    }
                }
            } else {
                for r in elems.iter() {
                    if el.eq(&r) {
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// ensure that the element on the left sides mathes the regex on the right side
pub fn regex(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.is_empty() || right.is_empty() {
        return false;
    }

    match right.get(0) {
        Some(JsonValue::String(str)) => {
            if let Ok(regex) = Regex::new(str) {
                for el in left.iter() {
                    if let Some(v) = el.as_str() {
                        if regex.is_match(v) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// ensure that the element on the left side belongs to the array on the right side.
pub fn inside(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.is_empty() {
        return false;
    }

    match right.get(0) {
        Some(JsonValue::Array(elems)) => {
            for el in left.iter() {
                if elems.contains(el) {
                    return true;
                }
            }
            false
        }
        Some(JsonValue::Object(elems)) => {
            for el in left.iter() {
                for r in elems.values() {
                    if el.eq(&r) {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// ensure the number on the left side is less the number on the right side
pub fn less(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.len() == 1 && right.len() == 1 {
        match (left.get(0), right.get(0)) {
            (Some(JsonValue::Number(l)), Some(JsonValue::Number(r))) => l.0 < r.0,
            _ => false,
        }
    } else {
        false
    }
}

/// compare elements
pub fn eq(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.len() != right.len() {
        false
    } else {
        left.iter().zip(right).map(|(a, b)| a.eq(&b)).all(|a| a)
    }
}
