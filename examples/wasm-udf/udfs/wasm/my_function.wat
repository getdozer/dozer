(module
 (type $0 (func (param i64 i64) (result i64)))
 (type $1 (func (param f64 f64) (result f64)))
 (type $2 (func (param i32 i32 i32 i32)))
 (type $3 (func (param i32 i32) (result i32)))
 (type $4 (func (param i64) (result i64)))
 (import "env" "abort" (func $~lib/builtins/abort (param i32 i32 i32 i32)))
 (global $assembly/index/counter (mut i64) (i64.const 0))
 (memory $0 1)
 (data $0 (i32.const 1036) "<")
 (data $0.1 (i32.const 1048) "\02\00\00\00 \00\00\00d\00i\00v\00i\00s\00i\00o\00n\00 \00b\00y\00 \00z\00e\00r\00o")
 (data $1 (i32.const 1100) "<")
 (data $1.1 (i32.const 1112) "\02\00\00\00\"\00\00\00a\00s\00s\00e\00m\00b\00l\00y\00/\00i\00n\00d\00e\00x\00.\00t\00s")
 (export "my_function" (func $assembly/index/my_function))
 (export "scale" (func $assembly/index/scale))
 (export "checked_divide" (func $assembly/index/checked_divide))
 (export "isolated_counter" (func $assembly/index/isolated_counter))
 (export "memory" (memory $0))
 (func $assembly/index/my_function (param $0 i64) (param $1 i64) (result i64)
  local.get $0
  local.get $1
  i64.add
 )
 (func $assembly/index/scale (param $0 f64) (param $1 f64) (result f64)
  local.get $0
  local.get $1
  f64.mul
 )
 (func $assembly/index/checked_divide (param $0 i32) (param $1 i32) (result i32)
  local.get $1
  i32.eqz
  if
   i32.const 1056
   i32.const 1120
   i32.const 10
   i32.const 3
   call $~lib/builtins/abort
   unreachable
  end
  local.get $0
  local.get $1
  i32.div_s
 )
 (func $assembly/index/isolated_counter (param $0 i64) (result i64)
  global.get $assembly/index/counter
  i64.const 1
  i64.add
  global.set $assembly/index/counter
  local.get $0
  global.get $assembly/index/counter
  i64.add
 )
)
