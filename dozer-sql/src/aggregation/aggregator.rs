#![allow(clippy::enum_variant_names)]

use crate::aggregation::avg::AvgAggregator;
use crate::aggregation::count::CountAggregator;
use crate::aggregation::max::MaxAggregator;
use crate::aggregation::min::MinAggregator;
use crate::aggregation::sum::SumAggregator;
use crate::calculate_err;
use crate::errors::PipelineError;
use dozer_types::chrono::{DateTime, FixedOffset, NaiveDate};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;

use enum_dispatch::enum_dispatch;
use std::collections::BTreeMap;

use dozer_sql_expression::aggregate::AggregateFunctionType;
use dozer_sql_expression::execution::Expression;

use crate::aggregation::max_append_only::MaxAppendOnlyAggregator;
use crate::aggregation::max_value::MaxValueAggregator;
use crate::aggregation::min_append_only::MinAppendOnlyAggregator;
use crate::aggregation::min_value::MinValueAggregator;
use crate::errors::PipelineError::{InvalidFunctionArgument, InvalidValue};
use dozer_sql_expression::aggregate::AggregateFunctionType::MaxValue;
use dozer_types::types::{DozerDuration, Field, FieldType, Schema};
use std::fmt::{Debug, Display, Formatter};

#[enum_dispatch]
pub trait Aggregator: Send + Sync + bincode::Encode + bincode::Decode {
    fn init(&mut self, return_type: FieldType);
    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError>;
    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError>;
    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError>;
}

#[enum_dispatch(Aggregator)]
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub enum AggregatorEnum {
    AvgAggregator,
    MinAggregator,
    MinAppendOnlyAggregator,
    MinValueAggregator,
    MaxAggregator,
    MaxAppendOnlyAggregator,
    MaxValueAggregator,
    SumAggregator,
    CountAggregator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregatorType {
    Avg,
    Count,
    Max,
    MaxAppendOnly,
    MaxValue,
    Min,
    MinAppendOnly,
    MinValue,
    Sum,
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub(crate) struct OrderedAggregatorState {
    function_type: AggregateFunctionType,
    inner: OrderedAggregatorStateInner,
}

#[derive(Debug, bincode::Encode, bincode::Decode)]
enum OrderedAggregatorStateInner {
    UInt(BTreeMap<u64, u64>),
    U128(BTreeMap<u128, u64>),
    Int(BTreeMap<i64, u64>),
    I128(BTreeMap<i128, u64>),
    Float(#[bincode(with_serde)] BTreeMap<OrderedFloat<f64>, u64>),
    Decimal(#[bincode(with_serde)] BTreeMap<Decimal, u64>),
    Timestamp(#[bincode(with_serde)] BTreeMap<DateTime<FixedOffset>, u64>),
    Date(#[bincode(with_serde)] BTreeMap<NaiveDate, u64>),
    Duration(BTreeMap<DozerDuration, u64>),
}

impl OrderedAggregatorState {
    fn max_in_map<T: Ord + Clone>(map: &BTreeMap<T, u64>) -> Option<T> {
        let (value, _count) = map.last_key_value()?;
        Some(value.clone())
    }
    fn min_in_map<T: Ord + Clone>(map: &BTreeMap<T, u64>) -> Option<T> {
        Some(map.first_key_value()?.0.clone())
    }

    fn update_for_map<T: Ord + Clone + Debug>(
        map: &mut BTreeMap<T, u64>,
        for_value: T,
        incr: bool,
    ) {
        let amount = map.entry(for_value.clone()).or_insert(0);
        if incr {
            *amount += 1;
        } else {
            *amount -= 1;
        }
        if *amount == 0 {
            map.remove(&for_value);
        }
    }

    fn update(&mut self, for_value: &Field, incr: bool) -> Result<(), PipelineError> {
        if for_value == &Field::Null {
            return Ok(());
        }
        match &mut self.inner {
            OrderedAggregatorStateInner::UInt(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_uint(), self.function_type),
                incr,
            ),
            OrderedAggregatorStateInner::U128(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_u128(), self.function_type),
                incr,
            ),

            OrderedAggregatorStateInner::Int(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_int(), self.function_type),
                incr,
            ),

            OrderedAggregatorStateInner::I128(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_i128(), self.function_type),
                incr,
            ),

            OrderedAggregatorStateInner::Float(map) => Self::update_for_map(
                map,
                OrderedFloat(calculate_err!(for_value.as_float(), self.function_type)),
                incr,
            ),

            OrderedAggregatorStateInner::Decimal(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_decimal(), self.function_type),
                incr,
            ),

            OrderedAggregatorStateInner::Timestamp(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_timestamp(), self.function_type),
                incr,
            ),

            OrderedAggregatorStateInner::Date(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_date(), self.function_type),
                incr,
            ),

            OrderedAggregatorStateInner::Duration(map) => Self::update_for_map(
                map,
                calculate_err!(for_value.as_duration(), self.function_type),
                incr,
            ),
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn incr(&mut self, value: &Field) -> Result<(), PipelineError> {
        self.update(value, true)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn decr(&mut self, value: &Field) -> Result<(), PipelineError> {
        self.update(value, false)?;
        Ok(())
    }

    pub(crate) fn new(function_type: AggregateFunctionType, field_type: FieldType) -> Option<Self> {
        let inner = match field_type {
            FieldType::UInt => OrderedAggregatorStateInner::UInt(Default::default()),
            FieldType::U128 => OrderedAggregatorStateInner::U128(Default::default()),
            FieldType::Int => OrderedAggregatorStateInner::Int(Default::default()),
            FieldType::I128 => OrderedAggregatorStateInner::I128(Default::default()),
            FieldType::Float => OrderedAggregatorStateInner::Float(Default::default()),
            FieldType::Decimal => OrderedAggregatorStateInner::Decimal(Default::default()),
            FieldType::Timestamp => OrderedAggregatorStateInner::Timestamp(Default::default()),
            FieldType::Date => OrderedAggregatorStateInner::Date(Default::default()),
            FieldType::Duration => OrderedAggregatorStateInner::Duration(Default::default()),
            _ => return None,
        };
        Some(Self {
            function_type,
            inner,
        })
    }

    fn get_min_opt(&self) -> Option<Field> {
        let field = match &self.inner {
            OrderedAggregatorStateInner::UInt(map) => Field::UInt(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::U128(map) => Field::U128(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::Int(map) => Field::Int(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::I128(map) => Field::I128(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::Float(map) => Field::Float(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::Decimal(map) => Field::Decimal(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::Timestamp(map) => Field::Timestamp(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::Date(map) => Field::Date(Self::min_in_map(map)?),
            OrderedAggregatorStateInner::Duration(map) => Field::Duration(Self::min_in_map(map)?),
        };
        Some(field)
    }

    #[inline]
    pub(crate) fn get_min(&self) -> Field {
        self.get_min_opt().unwrap_or(Field::Null)
    }

    fn get_max_opt(&self) -> Option<Field> {
        let field = match &self.inner {
            OrderedAggregatorStateInner::UInt(map) => Field::UInt(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::U128(map) => Field::U128(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::Int(map) => Field::Int(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::I128(map) => Field::I128(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::Float(map) => Field::Float(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::Decimal(map) => Field::Decimal(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::Timestamp(map) => Field::Timestamp(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::Date(map) => Field::Date(Self::max_in_map(map)?),
            OrderedAggregatorStateInner::Duration(map) => Field::Duration(Self::max_in_map(map)?),
        };
        Some(field)
    }

    #[inline]
    pub(crate) fn get_max(&self) -> Field {
        self.get_max_opt().unwrap_or(Field::Null)
    }
}

impl Display for AggregatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregatorType::Avg => f.write_str("avg"),
            AggregatorType::Count => f.write_str("count"),
            AggregatorType::Max => f.write_str("max"),
            AggregatorType::MaxAppendOnly => f.write_str("max_append_only"),
            AggregatorType::MaxValue => f.write_str("max_value"),
            AggregatorType::Min => f.write_str("min"),
            AggregatorType::MinAppendOnly => f.write_str("min_append_only"),
            AggregatorType::MinValue => f.write_str("min_value"),
            AggregatorType::Sum => f.write_str("sum"),
        }
    }
}

pub fn get_aggregator_from_aggregator_type(typ: AggregatorType) -> AggregatorEnum {
    match typ {
        AggregatorType::Avg => AvgAggregator::new().into(),
        AggregatorType::Count => CountAggregator::new().into(),
        AggregatorType::Max => MaxAggregator::new().into(),
        AggregatorType::MaxAppendOnly => MaxAppendOnlyAggregator::new().into(),
        AggregatorType::MaxValue => MaxValueAggregator::new().into(),
        AggregatorType::Min => MinAggregator::new().into(),
        AggregatorType::MinAppendOnly => MinAppendOnlyAggregator::new().into(),
        AggregatorType::MinValue => MinValueAggregator::new().into(),
        AggregatorType::Sum => SumAggregator::new().into(),
    }
}

pub fn get_aggregator_type_from_aggregation_expression(
    e: &Expression,
    schema: &Schema,
) -> Result<(Vec<Expression>, AggregatorType), PipelineError> {
    match e {
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Sum,
            args,
        } => Ok((
            vec![args.first()
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Sum.to_string())
                })?
                .clone()],
            AggregatorType::Sum,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Min,
            args,
        } => Ok((
            vec![args.first()
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Min.to_string())
                })?
                .clone()],
            AggregatorType::Min,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::MinAppendOnly,
            args,
        } => Ok((
            vec![args.first()
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(
                        AggregateFunctionType::MinAppendOnly.to_string(),
                    )
                })?
                .clone()],
            AggregatorType::MinAppendOnly,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Max,
            args,
        } => Ok((
            vec![args.first()
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Max.to_string())
                })?
                .clone()],
            AggregatorType::Max,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::MaxAppendOnly,
            args,
        } => Ok((
            vec![args.first()
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(
                        AggregateFunctionType::MaxAppendOnly.to_string(),
                    )
                })?
                .clone()],
            AggregatorType::MaxAppendOnly,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::MaxValue,
            args,
        } => Ok((
            vec![
                args.first()
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MaxValue.to_string(),
                        )
                    })?
                    .clone(),
                args.get(1)
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MaxValue.to_string(),
                        )
                    })?
                    .clone(),
            ],
            AggregatorType::MaxValue,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::MinValue,
            args,
        } => Ok((
            vec![
                args.first()
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MinValue.to_string(),
                        )
                    })?
                    .clone(),
                args.get(1)
                    .ok_or_else(|| {
                        PipelineError::NotEnoughArguments(
                            AggregateFunctionType::MinValue.to_string(),
                        )
                    })?
                    .clone(),
            ],
            AggregatorType::MinValue,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Avg,
            args,
        } => Ok((
            vec![args.first()
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Avg.to_string())
                })?
                .clone()],
            AggregatorType::Avg,
        )),
        Expression::AggregateFunction {
            fun: AggregateFunctionType::Count,
            args,
        } => Ok((
            vec![args.first()
                .ok_or_else(|| {
                    PipelineError::NotEnoughArguments(AggregateFunctionType::Count.to_string())
                })?
                .clone()],
            AggregatorType::Count,
        )),
        _ => Err(PipelineError::InvalidFunction(e.to_string(schema))),
    }
}

pub fn update_val_map(
    fields: &[Field],
    val_delta: u64,
    decr: bool,
    field_map: &mut BTreeMap<Field, u64>,
    return_map: &mut BTreeMap<Field, Vec<Field>>,
) -> Result<(), PipelineError> {
    let field = match fields.first() {
        Some(v) => v,
        None => {
            return Err(InvalidFunctionArgument(
                MaxValue.to_string(),
                Field::Null,
                0,
            ))
        }
    };
    let return_field = match fields.get(1) {
        Some(v) => v.clone(),
        None => {
            return Err(InvalidFunctionArgument(
                MaxValue.to_string(),
                Field::Null,
                0,
            ))
        }
    };

    if field == &Field::Null {
        return Ok(());
    }

    let get_prev_count = field_map.get(field);
    let prev_count = match get_prev_count {
        Some(v) => *v,
        None => 0_u64,
    };
    let mut new_count = prev_count;
    if decr {
        new_count = new_count.wrapping_sub(val_delta);
    } else {
        new_count = new_count.wrapping_add(val_delta);
    }
    if new_count < 1 {
        field_map.remove(field);
    } else if field_map.contains_key(field) {
        if let Some(val) = field_map.get_mut(field) {
            *val = new_count;
        }
    } else {
        field_map.insert(field.clone(), new_count);
    }

    if !decr {
        if return_map.contains_key(field) {
            if let Some(val) = return_map.get_mut(field) {
                val.insert(0, return_field);
            }
        } else {
            return_map.insert(field.clone(), vec![return_field]);
        }
    } else if return_map.contains_key(field) {
        if let Some(val) = return_map.get_mut(field) {
            let idx = match val.iter().position(|r| r.clone() == return_field) {
                Some(id) => id,
                None => return Err(InvalidValue(format!("{:?}", val))),
            };
            val.remove(idx);
            if val.is_empty() {
                return_map.remove(field);
            }
        }
    }

    let _res = format!("{:?}", field_map);
    let _res_return = format!("{:?}", return_map);

    Ok(())
}

#[macro_export]
macro_rules! deserialize_u8 {
    ($stmt:expr) => {
        match $stmt {
            Some(v) => u8::from_be_bytes(deserialize!(v)),
            None => 0_u8,
        }
    };
}

#[macro_export]
macro_rules! check_nan_f64 {
    ($stmt:expr) => {
        if $stmt.is_nan() {
            0_f64
        } else {
            $stmt
        }
    };
}

#[macro_export]
macro_rules! check_nan_decimal {
    ($stmt:expr) => {
        if $stmt.is_nan() {
            dozer_types::rust_decimal::Decimal::zero()
        } else {
            $stmt
        }
    };
}

#[macro_export]
macro_rules! try_unwrap {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

#[macro_export]
macro_rules! calculate_err {
    ($stmt:expr, $aggr:expr) => {
        $stmt.ok_or(PipelineError::InvalidReturnType(format!(
            "Failed to calculate {}",
            $aggr
        )))?
    };
}

#[macro_export]
macro_rules! calculate_err_field {
    ($stmt:expr, $aggr:expr, $field:expr) => {
        $stmt.ok_or(PipelineError::InvalidReturnType(format!(
            "Failed to calculate {} while parsing {}",
            $aggr, $field
        )))?
    };
}

#[macro_export]
macro_rules! calculate_err_type {
    ($stmt:expr, $aggr:expr, $return_type:expr) => {
        $stmt.ok_or(PipelineError::InvalidReturnType(format!(
            "Failed to calculate {} while casting {}",
            $aggr, $return_type
        )))?
    };
}
