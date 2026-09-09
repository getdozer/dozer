export function addOne(value: i64): i64 {
  return value + 1;
}

export function add(left: f64, right: f64): f64 {
  return left + right;
}

export function nonnegative(value: i64): i64 {
  assert(value >= 0, "value must be non-negative");
  return value;
}
