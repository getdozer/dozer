mod dump;
mod iterator;
mod lmdb_val;
mod raw_iterator;

pub use dump::{assert_database_equal, dump, restore, DumpItem, RestoreError};
pub use iterator::{Iterator, KeyIterator, ValueIterator};
pub use lmdb_val::{BorrowEncode, Decode, Encode, Encoded, LmdbKey, LmdbKeyType, LmdbVal};

#[macro_export]
macro_rules! yield_return_if_err {
    ($context:ident, $expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                $context.yield_(Err(e.into())).await;
                return Err(());
            }
        }
    };
}
