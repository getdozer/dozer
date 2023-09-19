pub mod error;
pub mod udf;
pub mod utils;

#[derive(Clone, Debug)]
pub struct DozerSession(pub std::sync::Arc<ort::Session>);

impl PartialEq for DozerSession {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self as *const _, other as *const _)
    }
}
