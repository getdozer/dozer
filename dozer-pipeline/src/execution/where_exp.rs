#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_must_use)]

use std::ops::Deref;
use crate::{ExecutionContext, Field, MemoryExecutionContext, Operation, Processor, Record};
use async_trait::async_trait;
use crate::Field::empty;


pub enum Operand {
    const_value(Field),
    field_value(usize) //Index
}

pub enum Operator {
    eq(Operand, Operand),
    ne(Operand, Operand),
    lt(Operand, Operand),
    gt(Operand, Operand),
    lte(Operand, Operand),
    gte(Operand, Operand),
    and(Box<Operator>, Box<Operator>),
    or(Box<Operator>, Box<Operator>),
    not(Box<Operator>)
}

enum Cmp {
    eq, ne, gt, lt, gte, lte
}

fn eval_cmp<T: std::cmp::PartialEq + std::cmp::PartialOrd>(v_left: T, v_right: T, cmp: Cmp) -> bool {
    match cmp {
        Cmp::eq => { return v_left == v_right; }
        Cmp::ne => { return v_left != v_right; }
        Cmp::gt => { return v_left > v_right; }
        Cmp::lt => { return v_left < v_right; }
        Cmp::gte => { return v_left >= v_right; }
        Cmp::lte => { return v_left <= v_right; }
    }
}


fn eval_operand<'a>(op: &'a Operand, record: &'a Record, ctx: &dyn ExecutionContext) -> &'a Field {
    match op {
        Operand::const_value(value) => { value }
        Operand::field_value(index) => {
            let o = record.values.get(*index);
            if (o.is_none()) {&Field::empty} else {o.unwrap()}
        }
        _ => {&Field::empty}
    }
}

fn eval_cmp_operator(record: &Record, ctx: &dyn ExecutionContext, left: &Operand, right: &Operand, cmp: Cmp) -> bool {

    let c_left = eval_operand(left, record, ctx);
    let c_right = eval_operand(right, record, ctx);
    match c_left {
        Field::empty => {
            match c_right {
                Field::empty => { return true; }
                _ => { return false; }
            }
        }
        Field::int_field(v_left) => {
            match c_right {
                Field::int_field(v_right) => { return eval_cmp(v_left, v_right, cmp); }
                _ => { return false; }
            }
        }
        Field::bool_field(v_left) => {
            match c_right {
                Field::bool_field(v_right) => { return eval_cmp(v_left, v_right, cmp); }
                _ => { return false; }
            }
        }
        Field::binary_field(v_left) => {
            match c_right {
                Field::binary_field(v_right) => { return eval_cmp(v_left, v_right, cmp); }
                _ => { return false; }
            }
        }
        Field::float_field(v_left) => {
            match c_right {
                Field::float_field(v_right) => { return eval_cmp(v_left, v_right, cmp); }
                _ => { return false; }
            }
        }
        Field::string_field(v_left) => {
            match c_right {
                Field::string_field(v_right) => { return eval_cmp(v_left, v_right, cmp); }
                _ => { return false; }
            }
        }
        Field::timestamp_field(v_left) => {
            match c_right {
                Field::timestamp_field(v_right) => { return eval_cmp(v_left, v_right, cmp); }
                _ => { return false; }
            }

        }
    }
}


pub fn eval_operator(op: &Box<Operator>, record: &Record, ctx: &dyn ExecutionContext) -> bool {
    match op.deref() {
        Operator::eq (left, right) => {
            return eval_cmp_operator(record, ctx, left, right, Cmp::eq);
        }
        Operator::ne (left, right) => {
            return eval_cmp_operator(record, ctx, left, right, Cmp::ne);
        }
        Operator::gt (left, right) => {
            return eval_cmp_operator(record, ctx, left, right, Cmp::gt);
        }
        Operator::lt (left, right) => {
            return eval_cmp_operator(record, ctx, left, right, Cmp::lt);
        }
        Operator::lte (left, right) => {
            return eval_cmp_operator(record, ctx, left, right, Cmp::lte);
        }
        Operator::gte (left, right) => {
            return eval_cmp_operator(record, ctx, left, right, Cmp::gte);
        }
        Operator::and(o1, o2) => {
            let c_o1 = eval_operator(o1, record, ctx);
            let c_o2 = eval_operator(o2, record, ctx);
            return c_o1 && c_o2;
        }
        Operator::or(o1, o2) => {
            let c_o1 = eval_operator(o1, record, ctx);
            let c_o2 = eval_operator(o2, record, ctx);
            return c_o1 || c_o2;
        }
        Operator::not(o) => {
            let c_o = eval_operator(o, record, ctx);
            return !c_o;
        }
    }
}

mod tests {
    use crate::{Field, MemoryExecutionContext, Record};
    use crate::execution::where_exp::{eval_operator, Operand, Operator};

    #[test]
    fn test_eq_operator_int() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::int_field(10);
        let r = Record::new(0, vec![Field::int_field(10)]);
        let op = Box::new(Operator::eq(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_eq_operator_float() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::float_field(10.1);
        let r = Record::new(0, vec![Field::float_field(10.1)]);
        let op = Box::new(Operator::eq(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_eq_operator_bool() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::bool_field(false);
        let r = Record::new(0, vec![Field::bool_field(false)]);
        let op = Box::new(Operator::eq(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_eq_operator_str() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::string_field("test".to_string());
        let r = Record::new(0, vec![Field::string_field("test".to_string())]);
        let op = Box::new(Operator::eq(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_eq_operator_binary() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::binary_field(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let r = Record::new(0, vec![Field::binary_field(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]);
        let op = Box::new(Operator::eq(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_eq_operator_ts() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::timestamp_field(1000);
        let r = Record::new(0, vec![Field::timestamp_field(1000)]);
        let op = Box::new(Operator::eq(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }


    #[test]
    fn test_ne_operator() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::int_field(50);
        let r = Record::new(0, vec![Field::int_field(10)]);
        let op = Box::new(Operator::ne(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_gt_operator() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::int_field(50);
        let r = Record::new(0, vec![Field::int_field(100)]);
        let op = Box::new(Operator::ne(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_lt_operator() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::int_field(100);
        let r = Record::new(0, vec![Field::int_field(50)]);
        let op = Box::new(Operator::ne(Operand::const_value(f), Operand::field_value(0)));
        assert!(eval_operator(&op, &r, &ctx));
    }

    #[test]
    fn test_and_operator() {
        let ctx = MemoryExecutionContext::new();

        let f0 = Field::int_field(10);
        let f1 = Field::int_field(20);
        let r = Record::new(0, vec![Field::int_field(10), Field::int_field(20)]);

        let op0 = Box::new(Operator::eq(Operand::const_value(f0), Operand::field_value(0)));
        let op1 = Box::new(Operator::eq(Operand::const_value(f1), Operand::field_value(1)));

        let and = Box::new(Operator::and(op0, op1));

        assert!(eval_operator(&and, &r, &ctx));
    }

    #[test]
    fn test_or_operator() {
        let ctx = MemoryExecutionContext::new();

        let f0 = Field::int_field(10);
        let f1 = Field::int_field(30);
        let r = Record::new(0, vec![Field::int_field(10), Field::int_field(20)]);

        let op0 = Box::new(Operator::eq(Operand::const_value(f0), Operand::field_value(0)));
        let op1 = Box::new(Operator::eq(Operand::const_value(f1), Operand::field_value(1)));

        let and = Box::new(Operator::or(op0, op1));

        assert!(eval_operator(&and, &r, &ctx));
    }

    #[test]
    fn test_not_operator() {
        let ctx = MemoryExecutionContext::new();
        let f = Field::int_field(10);
        let r = Record::new(0, vec![Field::int_field(20)]);
        let op = Box::new(Operator::eq(Operand::const_value(f), Operand::field_value(0)));
        let not_op = Box::new(Operator::not(op));
        assert!(eval_operator(&not_op, &r, &ctx));
    }
}
