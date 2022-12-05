#![allow(clippy::too_many_arguments)]
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::ExpressionExecutor;
use crate::pipeline::{aggregation::aggregator::Aggregator, expression::execution::Expression};
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_core::dag::errors::ExecutionError::InvalidPortHandle;
use dozer_core::dag::executor_local::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Processor, ProcessorFactory,
};
use dozer_types::internal_err;
use dozer_types::types::{Field, FieldDefinition, Operation, Record, Schema};

use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Database, Environment, RwTransaction};
use dozer_core::storage::errors::StorageError::InvalidDatabase;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use sqlparser::ast::{Expr as SqlExpr, SelectItem};
use std::{collections::HashMap, mem::size_of_val};

use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::builder::ExpressionType;

pub enum FieldRule {
    /// Represents a dimension field, generally used in the GROUP BY clause
    Dimension(
        /// Field to be used as a dimension in the source schema
        String,
        /// Expression for this dimension
        Box<Expression>,
        /// true of this field should be included in the list of values of the
        /// output schema, otherwise false. Generally, this value is true if the field appears
        /// in the output results in addition to being in the list of the GROUP BY fields
        bool,
        /// Name of the field, if renaming is required. If `None` the original name is retained
        Option<String>,
    ),
    /// Represents an aggregated field that will be calculated using the appropriate aggregator
    Measure(
        /// Field to be aggregated in the source schema
        String,
        /// Aggregator implementation for this measure
        Aggregator,
        /// true if this field should be included in the list of values of the
        /// output schema, otherwise false. Generally this value is true if the field appears
        /// in the output results in addition of being a condition for the HAVING condition
        bool,
        /// Name of the field, if renaming is required. If `None` the original name is retained
        Option<String>,
    ),
}

const COUNTER_KEY: u8 = 1_u8;

pub(crate) struct AggregationData<'a> {
    pub value: Field,
    pub state: Option<&'a [u8]>,
    pub prefix: u32,
}

impl<'a> AggregationData<'a> {
    pub fn new(value: Field, state: Option<&'a [u8]>, prefix: u32) -> Self {
        Self {
            value,
            state,
            prefix,
        }
    }
}

pub struct AggregationProcessorFactory {
    select: Vec<SelectItem>,
    groupby: Vec<SqlExpr>,
}

impl AggregationProcessorFactory {
    /// Creates a new [`AggregationProcessorFactory`].
    pub fn new(select: Vec<SelectItem>, groupby: Vec<SqlExpr>) -> Self {
        Self { select, groupby }
    }
}

impl ProcessorFactory for AggregationProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortDefOptions::default(),
        )]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(AggregationProcessor::new(
            self.select.clone(),
            self.groupby.clone(),
        ))
    }
}

pub struct AggregationProcessor {
    select: Vec<SelectItem>,
    groupby: Vec<SqlExpr>,
    output_field_rules: Vec<FieldRule>,
    out_dimensions: Vec<(usize, Box<Expression>, usize)>,
    out_measures: Vec<(usize, Box<Aggregator>, usize)>,
    builder: ExpressionBuilder,
    pub db: Option<Database>,
    meta_db: Option<Database>,
    aggregators_db: Option<Database>,
}

enum AggregatorOperation {
    Insert,
    Delete,
    Update,
}

const AGG_VALUES_DATASET_ID: u16 = 0x0000_u16;
const AGG_COUNT_DATASET_ID: u16 = 0x0001_u16;

const AGG_DEFAULT_DIMENSION_ID: u8 = 0xFF_u8;

impl AggregationProcessor {
    pub fn new(select: Vec<SelectItem>, groupby: Vec<SqlExpr>) -> Self {
        Self {
            select,
            groupby,
            output_field_rules: vec![],
            out_dimensions: vec![],
            out_measures: vec![],
            builder: ExpressionBuilder {},
            db: None,
            meta_db: None,
            aggregators_db: None,
        }
    }

    fn build(
        &self,
        select: &[SelectItem],
        groupby: &[SqlExpr],
        schema: &Schema,
    ) -> Result<Vec<FieldRule>, PipelineError> {
        let mut groupby_rules = groupby
            .iter()
            .map(|expr| self.parse_sql_groupby_item(expr, schema))
            .collect::<Result<Vec<FieldRule>, PipelineError>>()?;

        let mut select_rules = select
            .iter()
            .map(|item| self.parse_sql_aggregate_item(item, schema))
            .filter(|e| e.is_ok())
            .collect::<Result<Vec<FieldRule>, PipelineError>>()?;

        groupby_rules.append(&mut select_rules);

        Ok(groupby_rules)
    }

    pub fn parse_sql_groupby_item(
        &self,
        sql_expression: &SqlExpr,
        schema: &Schema,
    ) -> Result<FieldRule, PipelineError> {
        let expression =
            self.builder
                .build(&ExpressionType::FullExpression, sql_expression, schema)?;

        Ok(FieldRule::Dimension(
            sql_expression.to_string(),
            expression,
            true,
            None,
        ))
    }

    pub fn parse_sql_aggregate_item(
        &self,
        item: &SelectItem,
        schema: &Schema,
    ) -> Result<FieldRule, PipelineError> {
        match item {
            SelectItem::UnnamedExpr(sql_expr) => {
                match self.builder.parse_sql_expression(
                    &ExpressionType::Aggregation,
                    sql_expr,
                    schema,
                ) {
                    Ok(expr) => Ok(FieldRule::Measure(
                        sql_expr.to_string(),
                        self.get_aggregator(expr.0, schema)?,
                        true,
                        Some(item.to_string()),
                    )),
                    Err(error) => Err(error),
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => Err(PipelineError::InvalidExpression(
                format!("Unsupported Expression {}:{}", expr, alias),
            )),
            SelectItem::Wildcard => Err(PipelineError::InvalidExpression(
                "Wildcard Operator is not supported".to_string(),
            )),
            SelectItem::QualifiedWildcard(ref _object_name) => {
                Err(PipelineError::InvalidExpression(
                    "Qualified Wildcard Operator is not supported".to_string(),
                ))
            }
        }
    }

    fn get_aggregator(
        &self,
        expression: Box<Expression>,
        schema: &Schema,
    ) -> Result<Aggregator, PipelineError> {
        match *expression {
            Expression::AggregateFunction { fun, args } => {
                let arg_type = args[0].get_type(schema);
                match (&fun, arg_type) {
                    (AggregateFunctionType::Avg, _) => Ok(Aggregator::Avg),
                    (AggregateFunctionType::Count, _) => Ok(Aggregator::Count),
                    (AggregateFunctionType::Max, _) => Ok(Aggregator::Max),
                    (AggregateFunctionType::Min, _) => Ok(Aggregator::Min),
                    (AggregateFunctionType::Sum, _) => Ok(Aggregator::Sum),
                    _ => Err(PipelineError::InvalidExpression(format!(
                        "Not implemented Aggregation function: {:?}",
                        fun
                    ))),
                }
            }
            _ => Err(PipelineError::InvalidExpression(format!(
                "Not an Aggregation function: {:?}",
                expression
            ))),
        }
    }

    fn populate_rules(&mut self, schema: &Schema) -> Result<(), PipelineError> {
        let mut out_measures: Vec<(usize, Box<Aggregator>, usize)> = Vec::new();
        let mut out_dimensions: Vec<(usize, Box<Expression>, usize)> = Vec::new();

        for rule in self.output_field_rules.iter().enumerate() {
            match rule.1 {
                FieldRule::Measure(idx, aggr, _nullable, _name) => {
                    out_measures.push((
                        schema.get_field_index(idx.as_str())?.0,
                        Box::new(aggr.clone()),
                        rule.0,
                    ));
                }
                FieldRule::Dimension(idx, expression, _nullable, _name) => {
                    out_dimensions.push((
                        schema.get_field_index(idx.as_str())?.0,
                        expression.clone(),
                        rule.0,
                    ));
                }
            }
        }

        self.out_measures = out_measures;
        self.out_dimensions = out_dimensions;

        Ok(())
    }

    fn build_output_schema(&self, input_schema: &Schema) -> Result<Schema, ExecutionError> {
        let mut output_schema = Schema::empty();

        for e in self.output_field_rules.iter().enumerate() {
            match e.1 {
                FieldRule::Dimension(idx, expression, is_value, name) => {
                    let src_fld = input_schema.get_field_index(idx.as_str())?;
                    output_schema.fields.push(FieldDefinition::new(
                        match name {
                            Some(n) => n.clone(),
                            _ => src_fld.1.name.clone(),
                        },
                        expression.get_type(input_schema),
                        false,
                    ));
                    if *is_value {
                        output_schema.values.push(e.0);
                    }
                    output_schema.primary_index.push(e.0);
                }

                FieldRule::Measure(idx, aggr, is_value, name) => {
                    let src_fld = input_schema.get_field_index(idx)?;
                    output_schema.fields.push(FieldDefinition::new(
                        match name {
                            Some(n) => n.clone(),
                            _ => src_fld.1.name.clone(),
                        },
                        aggr.get_return_type(src_fld.1.typ),
                        false,
                    ));
                    if *is_value {
                        output_schema.values.push(e.0);
                    }
                }
            }
        }
        Ok(output_schema)
    }

    fn init_store(&mut self, txn: &mut dyn Environment) -> Result<(), PipelineError> {
        self.db = Some(txn.open_database("aggr", false)?);
        self.aggregators_db = Some(txn.open_database("aggr_data", false)?);
        self.meta_db = Some(txn.open_database("meta", false)?);
        Ok(())
    }

    fn fill_dimensions(&self, in_rec: &Record, out_rec: &mut Record) -> Result<(), PipelineError> {
        for v in &self.out_dimensions {
            out_rec.set_value(v.2, in_rec.get_value(v.0)?.clone());
        }
        Ok(())
    }

    fn get_record_key(&self, hash: &Vec<u8>, database_id: u16) -> Result<Vec<u8>, PipelineError> {
        let mut vec = Vec::with_capacity(hash.len() + size_of_val(&database_id));
        vec.extend_from_slice(&database_id.to_le_bytes());
        vec.extend(hash);
        Ok(vec)
    }

    fn get_counter(&self, txn: &mut dyn RwTransaction) -> Result<u32, PipelineError> {
        let meta_db = self
            .meta_db
            .as_ref()
            .ok_or(PipelineError::InternalStorageError(InvalidDatabase))?;
        let curr_ctr = match txn.get(meta_db, &COUNTER_KEY.to_be_bytes())? {
            Some(v) => u32::from_be_bytes(v.try_into().unwrap()),
            None => 1_u32,
        };
        txn.put(
            meta_db,
            &COUNTER_KEY.to_be_bytes(),
            &(curr_ctr + 1).to_be_bytes(),
        )?;
        Ok(curr_ctr + 1)
    }

    pub(crate) fn decode_buffer(buf: &[u8]) -> Result<(usize, AggregationData), PipelineError> {
        let prefix = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let mut offset: usize = 4;

        let val_len = u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let val: Field = internal_err!(bincode::deserialize(
            &buf[offset..offset + val_len as usize]
        ))?;
        offset += val_len as usize;
        let state_len = u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let state: Option<&[u8]> = if state_len > 0 {
            Some(&buf[offset..offset + state_len as usize])
        } else {
            None
        };
        offset += state_len as usize;

        let r = AggregationData::new(val, state, prefix);
        Ok((offset, r))
    }

    pub(crate) fn encode_buffer(
        prefix: u32,
        value: &Field,
        state: &Option<Vec<u8>>,
    ) -> Result<(usize, Vec<u8>), PipelineError> {
        let mut r = Vec::with_capacity(512);
        r.extend(prefix.to_be_bytes());

        let sz_val = internal_err!(bincode::serialize(&value))?;
        r.extend((sz_val.len() as u16).to_be_bytes());
        r.extend(&sz_val);

        let len = if let Some(state) = state.as_ref() {
            r.extend((state.len() as u16).to_be_bytes());
            r.extend(state);
            state.len()
        } else {
            r.extend(0_u16.to_be_bytes());
            0_usize
        };

        Ok((5 + sz_val.len() as usize + len, r))
    }

    fn calc_and_fill_measures(
        &self,
        txn: &mut dyn RwTransaction,
        cur_state: &Option<Vec<u8>>,
        deleted_record: Option<&Record>,
        inserted_record: Option<&Record>,
        out_rec_delete: &mut Record,
        out_rec_insert: &mut Record,
        op: AggregatorOperation,
    ) -> Result<Vec<u8>, PipelineError> {
        // array holding the list of states for all measures
        let mut next_state = Vec::<u8>::new();
        let mut offset: usize = 0;

        for measure in &self.out_measures {
            let curr_agg_data = match cur_state {
                Some(ref e) => {
                    let (len, res) = Self::decode_buffer(&e[offset..])?;
                    offset += len;
                    Some(res)
                }
                None => None,
            };

            let (prefix, next_state_slice) = match op {
                AggregatorOperation::Insert => {
                    let inserted_field = inserted_record.unwrap().get_value(measure.0)?;
                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.set_value(measure.2, curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.insert(
                            curr.state,
                            inserted_field,
                            inserted_field.get_type()?,
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.insert(
                            None,
                            inserted_field,
                            inserted_field.get_type()?,
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (prefix, r)
                    }
                }
                AggregatorOperation::Delete => {
                    let deleted_field = deleted_record.unwrap().get_value(measure.0)?;
                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.set_value(measure.2, curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.delete(
                            curr.state,
                            deleted_field,
                            deleted_field.get_type()?,
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.delete(
                            None,
                            deleted_field,
                            deleted_field.get_type()?,
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (prefix, r)
                    }
                }
                AggregatorOperation::Update => {
                    let deleted_field = deleted_record.unwrap().get_value(measure.0)?;
                    let updated_field = inserted_record.unwrap().get_value(measure.0)?;

                    if let Some(curr) = curr_agg_data {
                        out_rec_delete.set_value(measure.2, curr.value);
                        let mut p_tx = PrefixTransaction::new(txn, curr.prefix);
                        let r = measure.1.update(
                            curr.state,
                            deleted_field,
                            updated_field,
                            deleted_field.get_type()?,
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (curr.prefix, r)
                    } else {
                        let prefix = self.get_counter(txn)?;
                        let mut p_tx = PrefixTransaction::new(txn, prefix);
                        let r = measure.1.update(
                            None,
                            deleted_field,
                            updated_field,
                            deleted_field.get_type()?,
                            &mut p_tx,
                            self.aggregators_db.as_ref().unwrap(),
                        )?;
                        (prefix, r)
                    }
                }
            };

            next_state.extend(
                &Self::encode_buffer(prefix, &next_state_slice.value, &next_state_slice.state)?.1,
            );
            out_rec_insert.set_value(measure.2, next_state_slice.value);
        }

        Ok(next_state)
    }

    fn update_segment_count(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        key: Vec<u8>,
        delta: u64,
        decr: bool,
    ) -> Result<u64, PipelineError> {
        let bytes = txn.get(db, key.as_slice())?;

        let curr_count = match bytes {
            Some(b) => u64::from_le_bytes(b.try_into().unwrap()),
            None => 0_u64,
        };

        txn.put(
            db,
            key.as_slice(),
            (if decr {
                curr_count - delta
            } else {
                curr_count + delta
            })
            .to_le_bytes()
            .as_slice(),
        )?;
        Ok(curr_count)
    }

    fn agg_delete(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        old: &Record,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());

        let record_hash = if !self.out_dimensions.is_empty() {
            old.get_key(&self.out_dimensions.iter().map(|i| i.0).collect())?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        let prev_count = self.update_segment_count(txn, db, record_count_key, 1, true)?;

        let cur_state = txn.get(db, record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            txn,
            &cur_state,
            Some(old),
            None,
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Delete,
        )?;

        let res = if prev_count == 1 {
            self.fill_dimensions(old, &mut out_rec_delete)?;
            Operation::Delete {
                old: out_rec_delete,
            }
        } else {
            self.fill_dimensions(old, &mut out_rec_insert)?;
            self.fill_dimensions(old, &mut out_rec_delete)?;
            Operation::Update {
                new: out_rec_insert,
                old: out_rec_delete,
            }
        };

        if prev_count > 0 {
            txn.put(db, record_key.as_slice(), new_state.as_slice())?;
        } else {
            let _ = txn.del(db, record_key.as_slice(), None)?;
        }
        Ok(res)
    }

    fn agg_insert(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        new: &Record,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());

        let record_hash = if !self.out_dimensions.is_empty() {
            new.get_key(&self.out_dimensions.iter().map(|i| i.0).collect())?
        } else {
            vec![AGG_DEFAULT_DIMENSION_ID]
        };

        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let record_count_key = self.get_record_key(&record_hash, AGG_COUNT_DATASET_ID)?;
        self.update_segment_count(txn, db, record_count_key, 1, false)?;

        let cur_state = txn.get(db, record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            txn,
            &cur_state,
            None,
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Insert,
        )?;

        let res = if cur_state.is_none() {
            self.fill_dimensions(new, &mut out_rec_insert)?;
            Operation::Insert {
                new: out_rec_insert,
            }
        } else {
            self.fill_dimensions(new, &mut out_rec_insert)?;
            self.fill_dimensions(new, &mut out_rec_delete)?;
            Operation::Update {
                new: out_rec_insert,
                old: out_rec_delete,
            }
        };

        txn.put(db, record_key.as_slice(), new_state.as_slice())?;

        Ok(res)
    }

    fn agg_update(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        old: &Record,
        new: &Record,
        record_hash: Vec<u8>,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_insert = Record::nulls(None, self.output_field_rules.len());
        let mut out_rec_delete = Record::nulls(None, self.output_field_rules.len());
        let record_key = self.get_record_key(&record_hash, AGG_VALUES_DATASET_ID)?;

        let cur_state = txn.get(db, record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            txn,
            &cur_state,
            Some(old),
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Update,
        )?;

        self.fill_dimensions(new, &mut out_rec_insert)?;
        self.fill_dimensions(old, &mut out_rec_delete)?;

        let res = Operation::Update {
            new: out_rec_insert,
            old: out_rec_delete,
        };

        txn.put(db, record_key.as_slice(), new_state.as_slice())?;

        Ok(res)
    }

    pub fn aggregate(
        &self,
        txn: &mut dyn RwTransaction,
        db: &Database,
        op: Operation,
    ) -> Result<Vec<Operation>, PipelineError> {
        match op {
            Operation::Insert { ref new } => Ok(vec![self.agg_insert(txn, db, new)?]),
            Operation::Delete { ref old } => Ok(vec![self.agg_delete(txn, db, old)?]),
            Operation::Update { ref old, ref new } => {
                let (old_record_hash, new_record_hash) = if self.out_dimensions.is_empty() {
                    (
                        vec![AGG_DEFAULT_DIMENSION_ID],
                        vec![AGG_DEFAULT_DIMENSION_ID],
                    )
                } else {
                    let record_keys: Vec<usize> = self.out_dimensions.iter().map(|i| i.0).collect();
                    (old.get_key(&record_keys)?, new.get_key(&record_keys)?)
                };

                if old_record_hash == new_record_hash {
                    Ok(vec![self.agg_update(txn, db, old, new, old_record_hash)?])
                } else {
                    Ok(vec![
                        self.agg_insert(txn, db, new)?,
                        self.agg_delete(txn, db, old)?,
                    ])
                }
            }
        }
    }
}

impl Processor for AggregationProcessor {
    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError> {
        internal_err!(self.init_store(state))
    }

    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(InvalidPortHandle(output_port))?;

        let field_rules = internal_err!(self.build(&self.select, &self.groupby, input_schema))?;

        self.output_field_rules = field_rules;

        self.populate_rules(input_schema)
            .map_err(|e| InternalError(Box::new(e)))?;

        self.build_output_schema(input_schema)
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        match &self.db {
            Some(d) => {
                let ops = internal_err!(self.aggregate(txn, d, op))?;
                for op in ops {
                    fw.send(op, DEFAULT_PORT_HANDLE)?;
                }
                Ok(())
            }
            _ => Err(ExecutionError::InvalidDatabase),
        }
    }
}
