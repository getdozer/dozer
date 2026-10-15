export function my_function(field1: i64, field2: i64): i64 {
  return field1 + field2;
}

export function scale(value: f64, factor: f64): f64 {
  return value * factor;
}

export function checked_divide(value: i32, divisor: i32): i32 {
  assert(divisor != 0, "division by zero");
  return value / divisor;
}

let counter: i64 = 0;
export function isolated_counter(value: i64): i64 {
  counter += 1;
  return value + counter;
}
