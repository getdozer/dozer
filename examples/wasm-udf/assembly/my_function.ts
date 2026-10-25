// A numeric scalar export. No host I/O or managed objects cross the ABI.
export function my_function(left: i32, right: i32): i32 {
  return left + right;
}
