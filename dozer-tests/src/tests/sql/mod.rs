pub mod agg;
pub mod nested;
pub mod simple;

pub mod join;

pub mod helper;
pub mod python_udf;

#[derive(Clone, Debug)]
pub enum TestInstruction {
    FromCsv(&'static str, Vec<&'static str>),
    List(Vec<(&'static str, String)>),
}
