use std::borrow::Cow;

pub trait Name {
    fn name(&self) -> Cow<str>;
}
