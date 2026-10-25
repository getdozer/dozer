// Each exported name is registered separately in Dozer, using the same module.
export function keep_integer(value: i64): i64 {
  return value;
}

export function half(value: f64): f64 {
  return value / 2;
}

export function single(value: f32): f32 {
  return value;
}

export function invert(value: bool): bool {
  return !value;
}
