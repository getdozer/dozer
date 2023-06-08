use crate::types::{IndexDefinition, Record, SchemaWithIndex};

pub trait IntoOwned<Owned> {
    fn into_owned(self) -> Owned;
}

impl<'a> IntoOwned<String> for &'a str {
    fn into_owned(self) -> String {
        self.to_string()
    }
}

impl<'a, T: Clone> IntoOwned<Vec<T>> for &'a [T] {
    fn into_owned(self) -> Vec<T> {
        self.to_vec()
    }
}

pub trait Borrow: Sized {
    type Borrowed<'a>
    where
        Self: 'a;

    fn borrow(&self) -> Self::Borrowed<'_>;

    fn upcast<'b, 'a: 'b>(borrow: Self::Borrowed<'a>) -> Self::Borrowed<'b>;
}

impl Borrow for String {
    type Borrowed<'a> = &'a str;

    fn borrow(&self) -> Self::Borrowed<'_> {
        self
    }

    fn upcast<'b, 'a: 'b>(borrow: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
        borrow
    }
}

impl<T: Clone> Borrow for Vec<T> {
    type Borrowed<'a> = &'a [T] where T: 'a;

    fn borrow(&self) -> Self::Borrowed<'_> {
        self
    }

    fn upcast<'b, 'a: 'b>(borrow: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
        borrow
    }
}

#[macro_export]
macro_rules! impl_borrow_for_clone_type {
    ($($t:ty),*) => {
        $(
            impl<'a> IntoOwned<$t> for &'a $t {
                fn into_owned(self) -> $t {
                    self.clone()
                }
            }

            impl Borrow for $t {
                type Borrowed<'a> = &'a $t;

                fn borrow(&self) -> Self::Borrowed<'_> {
                    self
                }

                fn upcast<'b, 'a: 'b>(borrow: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
                    borrow
                }
            }
        )*
    };
}

impl_borrow_for_clone_type!(bool, u8, u32, u64, Record, IndexDefinition, SchemaWithIndex);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cow<'a, B: Borrow + 'a> {
    Borrowed(B::Borrowed<'a>),
    Owned(B),
}

impl<'a, B: Borrow + 'a> IntoOwned<B> for Cow<'a, B>
where
    B::Borrowed<'a>: IntoOwned<B>,
{
    fn into_owned(self) -> B {
        match self {
            Cow::Borrowed(borrowed) => borrowed.into_owned(),
            Cow::Owned(owned) => owned,
        }
    }
}

impl<'cow, B: Borrow + 'cow> Borrow for Cow<'cow, B>
where
    for<'a> B::Borrowed<'a>: Copy,
{
    type Borrowed<'a> = B::Borrowed<'a> where Self: 'a;

    fn borrow(&self) -> B::Borrowed<'_> {
        match self {
            Cow::Borrowed(borrowed) => B::upcast(*borrowed),
            Cow::Owned(owned) => owned.borrow(),
        }
    }

    fn upcast<'b, 'a: 'b>(borrow: B::Borrowed<'a>) -> B::Borrowed<'b> {
        B::upcast(borrow)
    }
}
