pub mod aggregate;
mod arg_utils;
pub mod builder;
mod case;
mod cast;
mod comparison;
mod conditional;
mod datetime;
pub mod error;
pub mod execution;
mod geo;
mod in_list;
mod json_functions;
mod logical;
mod mathematical;
pub mod operator;
pub mod scalar;

mod javascript;
#[cfg(feature = "onnx")]
mod onnx;
#[cfg(feature = "python")]
mod python_udf;
#[cfg(feature = "wasm")]
mod wasm;

pub use num_traits;
pub use sqlparser;

#[cfg(test)]
mod tests {
    use dozer_types::{
        chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike},
        rust_decimal::Decimal,
    };
    use proptest::{
        prelude::Arbitrary,
        strategy::{BoxedStrategy, Strategy},
    };

    #[derive(Debug)]
    pub struct ArbitraryDecimal(pub Decimal);

    impl Arbitrary for ArbitraryDecimal {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (i64::MIN..i64::MAX, u32::MIN..29u32)
                .prop_map(|(num, scale)| ArbitraryDecimal(Decimal::new(num, scale)))
                .boxed()
        }
    }

    #[derive(Debug)]
    pub struct ArbitraryDateTime(pub DateTime<FixedOffset>);

    impl Arbitrary for ArbitraryDateTime {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (
                NaiveDateTime::MIN.year()..NaiveDateTime::MAX.year(),
                1..13u32,
                1..32u32,
                0..NaiveDateTime::MAX.second(),
                0..NaiveDateTime::MAX.nanosecond(),
            )
                .prop_map(|(year, month, day, secs, nano)| {
                    let timezone_east = FixedOffset::east_opt(8 * 60 * 60).unwrap();
                    let date = NaiveDate::from_ymd_opt(year, month, day);
                    // Some dates are not able to created caused by leap in February with day larger than 28 or 29
                    if date.is_none() {
                        return ArbitraryDateTime(DateTime::default());
                    }
                    let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).unwrap();
                    let datetime = DateTime::<FixedOffset>::from_naive_utc_and_offset(
                        NaiveDateTime::new(date.unwrap(), time),
                        timezone_east,
                    );
                    ArbitraryDateTime(datetime)
                })
                .boxed()
        }
    }
}
