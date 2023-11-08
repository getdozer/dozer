use dozer_types::json_types::{DestructuredJsonRef, JsonValue};
use regex::Regex;

/// compare sizes of json elements
/// The method expects to get a number on the right side and array or string or object on the left
/// where the number of characters, elements or fields will be compared respectively.
pub fn size(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    let Some(n) = right.get(0).and_then(|v| v.to_usize()) else {
        return false;
    };
    left.iter().all(|el| match el.destructure_ref() {
        DestructuredJsonRef::String(v) => v.len() == n,
        DestructuredJsonRef::Array(elems) => elems.len() == n,
        DestructuredJsonRef::Object(fields) => fields.len() == n,
        _ => false,
    })
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
        if let Some(right_elems) = right.get(0).and_then(|v| v.as_array()) {
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

    let Some(elems) = right.get(0).and_then(|v| v.as_array()) else {
        return false;
    };
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
    false
}

/// ensure that the element on the left sides mathes the regex on the right side
pub fn regex(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.is_empty() || right.is_empty() {
        return false;
    }

    let Some(str) = right.get(0).and_then(|v| v.as_string()) else {
        return false;
    };
    if let Ok(regex) = Regex::new(str) {
        for el in left.iter() {
            if let Some(v) = el.as_string() {
                if regex.is_match(v) {
                    return true;
                }
            }
        }
    }
    false
}

/// ensure that the element on the left side belongs to the array on the right side.
pub fn inside(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.is_empty() {
        return false;
    }

    let Some(first) = right.get(0) else {
        return false;
    };
    if let Some(elems) = first.as_array() {
        for el in left.iter() {
            if elems.contains(el) {
                return true;
            }
        }
    } else if let Some(elems) = first.as_object() {
        for el in left.iter() {
            for r in elems.values() {
                if el.eq(&r) {
                    return true;
                }
            }
        }
    }
    false
}

/// ensure the number on the left side is less the number on the right side
pub fn less(left: Vec<&JsonValue>, right: Vec<&JsonValue>) -> bool {
    if left.len() == 1 && right.len() == 1 {
        let left_no = left.get(0).and_then(|v| v.as_number());
        let right_no = right.get(0).and_then(|v| v.as_number());
        match (left_no, right_no) {
            (Some(l), Some(r)) => l < r,
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
