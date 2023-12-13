export function addi(a: i64, b: i64): i64 {
  return a + b;
}

export function addf(a: f64, b: f64): f64 {
  return a + b;
}

export function fib(n: i64): i64 {
  var a = 0, b = 1
  if (n > 0) {
    while (--n) {
      let t = a + b
      a = b
      b = t
    }
    return b
  }
  return a
}
