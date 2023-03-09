use crate::types::{IndexDefinition, Record, Schema};

pub trait ToOwned<Owned>: Copy {
    fn to_owned(self) -> Owned;
}

impl<'a> ToOwned<String> for &'a str {
    fn to_owned(self) -> String {
        self.to_string()
    }
}

impl<'a, T: Clone> ToOwned<Vec<T>> for &'a [T] {
    fn to_owned(self) -> Vec<T> {
        self.to_vec()
    }
}

pub trait Borrow: Sized {
    type Borrowed<'a>: ToOwned<Self>
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
            impl<'a> ToOwned<$t> for &'a $t {
                fn to_owned(self) -> $t {
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

impl_borrow_for_clone_type!(u8, u32, u64, Record, (Schema, Vec<IndexDefinition>));

pub enum Cow<'a, B: Borrow + 'a> {
    Borrowed(B::Borrowed<'a>),
    Owned(B),
}

impl<'a, B: Borrow + 'a> Cow<'a, B> {
    pub fn into_owned(self) -> B {
        match self {
            Cow::Borrowed(borrowed) => borrowed.to_owned(),
            Cow::Owned(owned) => owned,
        }
    }

    pub fn borrow(&self) -> B::Borrowed<'_> {
        match self {
            Cow::Borrowed(borrowed) => B::upcast(*borrowed),
            Cow::Owned(owned) => owned.borrow(),
        }
    }

    pub fn to_mut(&mut self) -> &mut B {
        match self {
            Cow::Borrowed(borrowed) => {
                *self = Cow::Owned(borrowed.to_owned());
                match self {
                    Cow::Owned(owned) => owned,
                    _ => unreachable!(),
                }
            }
            Cow::Owned(owned) => owned,
        }
    }
}
