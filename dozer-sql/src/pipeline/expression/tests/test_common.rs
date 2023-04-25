use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::{projection::factory::ProjectionProcessorFactory, tests::utils::get_select};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::node::ProcessorFactory;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::chrono::{
    DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike,
};
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, Operation, Record, Schema};
use proptest::prelude::*;
use std::collections::HashMap;

struct TestChannelForwarder {
    operations: Vec<Operation>,
}

impl ProcessorChannelForwarder for TestChannelForwarder {
    fn send(
        &mut self,
        op: dozer_types::types::Operation,
        _port: dozer_core::node::PortHandle,
    ) -> Result<(), dozer_core::errors::ExecutionError> {
        self.operations.push(op);
        Ok(())
    }
}

pub(crate) fn run_fct(sql: &str, schema: Schema, input: Vec<Field>) -> Field {
    let select = get_select(sql).unwrap();
    let processor_factory = ProjectionProcessorFactory::_new(select.projection);
    processor_factory
        .get_output_schema(
            &DEFAULT_PORT_HANDLE,
            &[(
                DEFAULT_PORT_HANDLE,
                (schema.clone(), SchemaSQLContext::default()),
            )]
            .into_iter()
            .collect(),
        )
        .unwrap();

    let mut processor = processor_factory
        .build(
            HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
            HashMap::new(),
        )
        .unwrap();

    let mut fw = TestChannelForwarder { operations: vec![] };

    let op = Operation::Insert {
        new: Record::new(None, input),
    };

    processor.process(DEFAULT_PORT_HANDLE, op, &mut fw).unwrap();

    match &fw.operations[0] {
        Operation::Insert { new } => new.values[0].clone(),
        _ => panic!("Unable to find result value"),
    }
}

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
                let datetime = DateTime::<FixedOffset>::from_local(
                    NaiveDateTime::new(date.unwrap(), time),
                    timezone_east,
                );
                ArbitraryDateTime(datetime)
            })
            .boxed()
    }
}
