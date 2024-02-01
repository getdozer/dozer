#![allow(clippy::all)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[macro_export]
macro_rules! as_exp_build {
    ($func:ident $args:tt ) => {{
        let mut v = Vec::new();
        $crate::as_exp_build_inner!(v, $func $args);
        $crate::as_exp_compile(v.as_mut_ptr(), v.len() as u32)
    }}
}

#[macro_export]
macro_rules! as_exp_build_inner {
    ($v:expr, as_exp_bin_int($bin_name:expr $(,)?)) => {{
        let bin_name: *const i8 = $bin_name;
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_BIN,
            count: 3,
            sz: 0,
            prev_va_args: 0,
            v: std::mem::zeroed(),
        });
        $crate::as_exp_build_inner!($v, as_exp_int($crate::as_exp_type_AS_EXP_TYPE_INT as i64));
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_VAL_RAWSTR,
            v: $crate::as_exp_entry__bindgen_ty_1 { str_val: bin_name },
            count: 0,
            sz: 0,
            prev_va_args: 0,
        });
    }};
    ($v:expr, as_exp_int($val:expr)) => {
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_VAL_INT,
            v: $crate::as_exp_entry__bindgen_ty_1 { int_val: $val },
            count: 0,
            sz: 0,
            prev_va_args: 0,
        })
    };
    ($v:expr, as_exp_uint($val:expr)) => {
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_VAL_UINT,
            v: $crate::as_exp_entry__bindgen_ty_1 { uint_val: $val },
            count: 0,
            sz: 0,
            prev_va_args: 0,
        })
    };
    ($v:expr, as_exp_cmp_eq($left_name:ident $left_args:tt, $right_name:ident $right_args:tt $(,)?)) => {{
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_CMP_EQ,
            count: 3,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
        $crate::as_exp_build_inner!($v, $left_name $left_args);
        $crate::as_exp_build_inner!($v, $right_name $right_args);
    }};
    ($v:expr, as_exp_cmp_gt($left_name:ident $left_args:tt, $right_name:ident $right_args:tt $(,)?)) => {{
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_CMP_GT,
            count: 3,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
        $crate::as_exp_build_inner!($v, $left_name $left_args);
        $crate::as_exp_build_inner!($v, $right_name $right_args);
    }};
    ($v:expr, as_exp_cmp_ge($left_name:ident $left_args:tt, $right_name:ident $right_args:tt $(,)?)) => {{
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_CMP_GE,
            count: 3,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
        $crate::as_exp_build_inner!($v, $left);
        $crate::as_exp_build_inner!($v, $right);
    }};
    ($v:expr, as_exp_cmp_lt($left_name:ident $left_args:tt, $right_name:ident $right_args:tt $(,)?)) => {{
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_CMP_LT,
            count: 3,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
        $crate::as_exp_build_inner!($v, $left_name $left_args);
        $crate::as_exp_build_inner!($v, $right_name $right_args);
    }};
    ($v:expr, as_exp_cmp_le($left_name:ident $left_args:tt, $right_name:ident $right_args:tt $(,)?)) => {{
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_CMP_LE,
            count: 3,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
        $crate::as_exp_build_inner!($v, $left_name $left_args);
        $crate::as_exp_build_inner!($v, $right_name $right_args);
    }};
    ($v:expr, as_exp_and($($arg_name:ident $arg_args:tt),*)) => {{
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_AND,
            count: 0,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
        $($crate::as_exp_build_inner!($v, $arg_name $arg_args));*;
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_END_OF_VA_ARGS,
            count: 0,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
    }};
($v:expr, as_exp_or($($arg_name:ident $arg_args:tt),*)) => {{
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_OR,
            count: 0,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
        $($crate::as_exp_build_inner!($v, $arg_name $arg_args));*;
        $v.push($crate::as_exp_entry {
            op: $crate::as_exp_ops__AS_EXP_CODE_END_OF_VA_ARGS,
            count: 0,
            v: std::mem::zeroed(),
            sz: 0,
            prev_va_args: 0,
        });
    }};
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;

    #[test]
    fn test_as_exp_build() {
        // Tested that this results in the same compiled expression as when
        // using the macros from the C library
        let bin_name = CString::new("bin_name").unwrap();
        unsafe {
            let exp = as_exp_build! {
                as_exp_and(
                as_exp_cmp_gt(
                    as_exp_bin_int(bin_name.as_ptr()),
                    as_exp_int(3)
                ),
                as_exp_cmp_lt(
                    as_exp_bin_int(bin_name.as_ptr()),
                    as_exp_int(8)
                )
                )
            };
            assert!(!exp.is_null());
            as_exp_destroy(exp);
        }
    }
}
