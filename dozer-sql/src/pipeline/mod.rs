mod aggregation;
pub mod builder;
pub mod errors;
mod expression;
mod pipeline_builder;
mod planner;
mod product;
mod projection;
mod selection;
mod table_operator;
mod utils;
mod window;

#[cfg(test)]
mod tests;

#[cfg(feature = "onnx")]
#[derive(Clone, Debug)]
pub struct DozerSession(pub std::sync::Arc<ort::Session>);

#[cfg(feature = "onnx")]
impl PartialEq for DozerSession {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self as *const _, other as *const _)
    }
}
