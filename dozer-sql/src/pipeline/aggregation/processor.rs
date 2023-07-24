#![allow(clippy::too_many_arguments)]

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::ExpressionExecutor;
use crate::pipeline::{aggregation::aggregator::Aggregator, expression::execution::Expression};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::ref_types::ProcessorRecordRef;
use dozer_types::types::{Field, FieldType, ProcessorOperation, ProcessorRecord, Schema};
use std::hash::{Hash, Hasher};

use crate::pipeline::aggregation::aggregator::{
    get_aggregator_from_aggregator_type, get_aggregator_type_from_aggregation_expression,
    AggregatorEnum, AggregatorType,
};
use ahash::AHasher;
use dozer_core::epoch::Epoch;
use hashbrown::HashMap;

const DEFAULT_SEGMENT_KEY: &str = "DOZER_DEFAULT_SEGMENT_KEY";

#[derive(Debug)]
struct AggregationState {
    count: usize,
    states: Vec<AggregatorEnum>,
    values: Option<Vec<Field>>,
}

impl AggregationState {
    pub fn new(types: &[AggregatorType], ret_types: &[FieldType]) -> Self {
        let mut states: Vec<AggregatorEnum> = Vec::new();
        for (idx, typ) in types.iter().enumerate() {
            let mut aggr = get_aggregator_from_aggregator_type(*typ);
            aggr.init(ret_types[idx]);
            states.push(aggr);
        }

        Self {
            count: 0,
            states,
            values: None,
        }
    }
}

#[derive(Debug)]
pub struct AggregationProcessor {
    _id: String,
    dimensions: Vec<Expression>,
    measures: Vec<Vec<Expression>>,
    measures_types: Vec<AggregatorType>,
    measures_return_types: Vec<FieldType>,
    projections: Vec<Expression>,
    having: Option<Expression>,
    input_schema: Schema,
    aggregation_schema: Schema,
    states: HashMap<u64, AggregationState>,
    default_segment_key: u64,
    having_eval_schema: Schema,
}

enum AggregatorOperation {
    Insert,
    Delete,
    Update,
}

impl AggregationProcessor {
    pub fn new(
        id: String,
        dimensions: Vec<Expression>,
        measures: Vec<Expression>,
        projections: Vec<Expression>,
        having: Option<Expression>,
        input_schema: Schema,
        aggregation_schema: Schema,
    ) -> Result<Self, PipelineError> {
        let mut aggr_types = Vec::new();
        let mut aggr_measures = Vec::new();
        let mut aggr_measures_ret_types = Vec::new();

        for measure in measures {
            let (aggr_measure, aggr_type) =
                get_aggregator_type_from_aggregation_expression(&measure, &input_schema)?;
            aggr_measures.push(aggr_measure);
            aggr_types.push(aggr_type);
            aggr_measures_ret_types.push(measure.get_type(&input_schema)?.return_type)
        }

        let mut hasher = AHasher::default();
        DEFAULT_SEGMENT_KEY.hash(&mut hasher);

        let mut having_eval_schema_fields = input_schema.fields.clone();
        having_eval_schema_fields.extend(aggregation_schema.fields.clone());

        Ok(Self {
            _id: id,
            dimensions,
            projections,
            input_schema,
            aggregation_schema,
            states: HashMap::new(),
            measures: aggr_measures,
            having,
            measures_types: aggr_types,
            measures_return_types: aggr_measures_ret_types,
            default_segment_key: hasher.finish(),
            having_eval_schema: Schema {
                fields: having_eval_schema_fields,
                primary_index: vec![],
            },
        })
    }

    fn calc_and_fill_measures(
        curr_state: &mut AggregationState,
        deleted_record: Option<&ProcessorRecordRef>,
        inserted_record: Option<&ProcessorRecordRef>,
        out_rec_delete: &mut Vec<Field>,
        out_rec_insert: &mut Vec<Field>,
        op: AggregatorOperation,
        measures: &Vec<Vec<Expression>>,
        input_schema: &Schema,
    ) -> Result<Vec<Field>, PipelineError> {
        let mut new_fields: Vec<Field> = Vec::with_capacity(measures.len());

        for (idx, measure) in measures.iter().enumerate() {
            let curr_aggr = &mut curr_state.states[idx];
            let curr_val_opt: Option<&Field> = curr_state.values.as_ref().map(|e| &e[idx]);

            let new_val = match op {
                AggregatorOperation::Insert => {
                    let mut inserted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        inserted_fields
                            .push(m.evaluate(inserted_record.unwrap().get_record(), input_schema)?);
                    }
                    if let Some(curr_val) = curr_val_opt {
                        out_rec_delete.push(curr_val.clone());
                    }
                    curr_aggr.insert(&inserted_fields)?
                }
                AggregatorOperation::Delete => {
                    let mut deleted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        deleted_fields
                            .push(m.evaluate(deleted_record.unwrap().get_record(), input_schema)?);
                    }
                    if let Some(curr_val) = curr_val_opt {
                        out_rec_delete.push(curr_val.clone());
                    }
                    curr_aggr.delete(&deleted_fields)?
                }
                AggregatorOperation::Update => {
                    let mut deleted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        deleted_fields
                            .push(m.evaluate(deleted_record.unwrap().get_record(), input_schema)?);
                    }
                    let mut inserted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        inserted_fields
                            .push(m.evaluate(inserted_record.unwrap().get_record(), input_schema)?);
                    }
                    if let Some(curr_val) = curr_val_opt {
                        out_rec_delete.push(curr_val.clone());
                    }
                    curr_aggr.update(&deleted_fields, &inserted_fields)?
                }
            };
            out_rec_insert.push(new_val.clone());
            new_fields.push(new_val);
        }
        Ok(new_fields)
    }

    fn agg_delete(
        &mut self,
        old: &mut ProcessorRecordRef,
    ) -> Result<Vec<ProcessorOperation>, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let key = if !self.dimensions.is_empty() {
            get_key(&self.input_schema, old, &self.dimensions)?
        } else {
            self.default_segment_key
        };

        let curr_state_opt = self.states.get_mut(&key);
        assert!(
            curr_state_opt.is_some(),
            "Unable to find aggregator state during DELETE operation"
        );
        let curr_state = curr_state_opt.unwrap();

        let new_values = Self::calc_and_fill_measures(
            curr_state,
            Some(old),
            None,
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Delete,
            &self.measures,
            &self.input_schema,
        )?;

        let (out_rec_delete_having_satisfied, out_rec_insert_having_satisfied) = match &self.having
        {
            None => (true, true),
            Some(having) => (
                Self::having_is_satisfied(
                    &self.having_eval_schema,
                    old,
                    having,
                    &mut out_rec_delete,
                )?,
                Self::having_is_satisfied(
                    &self.having_eval_schema,
                    old,
                    having,
                    &mut out_rec_insert,
                )?,
            ),
        };

        let res = if curr_state.count == 1 {
            self.states.remove(&key);
            if out_rec_delete_having_satisfied {
                vec![ProcessorOperation::Delete {
                    old: Self::build_projection(
                        old,
                        out_rec_delete,
                        &self.projections,
                        &self.aggregation_schema,
                    )?,
                }]
            } else {
                vec![]
            }
        } else {
            curr_state.count -= 1;
            curr_state.values = Some(new_values);

            Self::generate_op_for_existing_segment(
                out_rec_delete_having_satisfied,
                out_rec_insert_having_satisfied,
                out_rec_delete,
                out_rec_insert,
                old,
                &self.projections,
                &self.aggregation_schema,
            )?
        };

        Ok(res)
    }

    fn agg_insert(
        &mut self,
        new: &mut ProcessorRecordRef,
    ) -> Result<Vec<ProcessorOperation>, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let key = if !self.dimensions.is_empty() {
            get_key(&self.input_schema, new, &self.dimensions)?
        } else {
            self.default_segment_key
        };

        let curr_state = self.states.entry(key).or_insert(AggregationState::new(
            &self.measures_types,
            &self.measures_return_types,
        ));

        let new_values = Self::calc_and_fill_measures(
            curr_state,
            None,
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Insert,
            &self.measures,
            &self.input_schema,
        )?;

        let (out_rec_delete_having_satisfied, out_rec_insert_having_satisfied) = match &self.having
        {
            None => (true, true),
            Some(having) => (
                Self::having_is_satisfied(
                    &self.having_eval_schema,
                    new,
                    having,
                    &mut out_rec_delete,
                )?,
                Self::having_is_satisfied(
                    &self.having_eval_schema,
                    new,
                    having,
                    &mut out_rec_insert,
                )?,
            ),
        };

        let res = if curr_state.count == 0 {
            if out_rec_insert_having_satisfied {
                vec![ProcessorOperation::Insert {
                    new: Self::build_projection(
                        new,
                        out_rec_insert,
                        &self.projections,
                        &self.aggregation_schema,
                    )?,
                }]
            } else {
                vec![]
            }
        } else {
            Self::generate_op_for_existing_segment(
                out_rec_delete_having_satisfied,
                out_rec_insert_having_satisfied,
                out_rec_delete,
                out_rec_insert,
                new,
                &self.projections,
                &self.aggregation_schema,
            )?
        };

        curr_state.count += 1;
        curr_state.values = Some(new_values);

        Ok(res)
    }

    fn generate_op_for_existing_segment(
        out_rec_delete_having_satisfied: bool,
        out_rec_insert_having_satisfied: bool,
        out_rec_delete: Vec<Field>,
        out_rec_insert: Vec<Field>,
        rec: &mut ProcessorRecordRef,
        projections: &Vec<Expression>,
        aggregation_schema: &Schema,
    ) -> Result<Vec<ProcessorOperation>, PipelineError> {
        Ok(
            match (
                out_rec_delete_having_satisfied,
                out_rec_insert_having_satisfied,
            ) {
                (false, true) => vec![ProcessorOperation::Insert {
                    new: Self::build_projection(
                        rec,
                        out_rec_insert,
                        projections,
                        aggregation_schema,
                    )?,
                }],
                (true, false) => vec![ProcessorOperation::Delete {
                    old: Self::build_projection(
                        rec,
                        out_rec_delete,
                        projections,
                        aggregation_schema,
                    )?,
                }],
                (true, true) => vec![ProcessorOperation::Update {
                    new: Self::build_projection(
                        rec,
                        out_rec_insert,
                        projections,
                        aggregation_schema,
                    )?,
                    old: Self::build_projection(
                        rec,
                        out_rec_delete,
                        projections,
                        aggregation_schema,
                    )?,
                }],
                (false, false) => vec![],
            },
        )
    }

    fn having_is_satisfied(
        having_eval_schema: &Schema,
        original_record: &mut ProcessorRecordRef,
        having: &Expression,
        out_rec: &mut Vec<Field>,
    ) -> Result<bool, PipelineError> {
        let mut original_record = ProcessorRecord::from_referenced_record(original_record.clone());
        Ok(match out_rec.len() {
            0 => false,
            _ => {
                for f in out_rec.iter() {
                    original_record.extend_direct_field(f.clone());
                }

                let r = having
                    .evaluate(&original_record, having_eval_schema)?
                    .as_boolean()
                    .unwrap_or(false);

                r
            }
        })
    }

    fn agg_update(
        &mut self,
        old: &mut ProcessorRecordRef,
        new: &mut ProcessorRecordRef,
        key: u64,
    ) -> Result<Vec<ProcessorOperation>, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let curr_state_opt = self.states.get_mut(&key);
        assert!(
            curr_state_opt.is_some(),
            "Unable to find aggregator state during UPDATE operation"
        );
        let curr_state = curr_state_opt.unwrap();

        let new_values = Self::calc_and_fill_measures(
            curr_state,
            Some(old),
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Update,
            &self.measures,
            &self.input_schema,
        )?;

        let (out_rec_delete_having_satisfied, out_rec_insert_having_satisfied) = match &self.having
        {
            None => (true, true),
            Some(having) => (
                Self::having_is_satisfied(
                    &self.having_eval_schema,
                    old,
                    having,
                    &mut out_rec_delete,
                )?,
                Self::having_is_satisfied(
                    &self.having_eval_schema,
                    new,
                    having,
                    &mut out_rec_insert,
                )?,
            ),
        };

        let res = match (
            out_rec_delete_having_satisfied,
            out_rec_insert_having_satisfied,
        ) {
            (false, true) => vec![ProcessorOperation::Insert {
                new: Self::build_projection(
                    new,
                    out_rec_insert,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
            }],
            (true, false) => vec![ProcessorOperation::Delete {
                old: Self::build_projection(
                    old,
                    out_rec_delete,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
            }],
            (true, true) => vec![ProcessorOperation::Update {
                new: Self::build_projection(
                    new,
                    out_rec_insert,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
                old: Self::build_projection(
                    old,
                    out_rec_delete,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
            }],
            (false, false) => vec![],
        };

        curr_state.values = Some(new_values);
        Ok(res)
    }

    pub fn build_projection(
        original: &mut ProcessorRecordRef,
        measures: Vec<Field>,
        projections: &Vec<Expression>,
        aggregation_schema: &Schema,
    ) -> Result<ProcessorRecordRef, PipelineError> {
        let mut original = ProcessorRecord::from_referenced_record(original.clone());
        for f in measures {
            original.extend_direct_field(f);
        }

        let mut output_record = ProcessorRecord::new();
        for exp in projections {
            output_record.extend_direct_field(exp.evaluate(&original, aggregation_schema)?);
        }

        output_record.set_lifetime(original.get_lifetime());

        Ok(ProcessorRecordRef::new(output_record))
    }

    pub fn aggregate(
        &mut self,
        mut op: ProcessorOperation,
    ) -> Result<Vec<ProcessorOperation>, PipelineError> {
        match op {
            ProcessorOperation::Insert { ref mut new } => Ok(self.agg_insert(new)?),
            ProcessorOperation::Delete { ref mut old } => Ok(self.agg_delete(old)?),
            ProcessorOperation::Update {
                ref mut old,
                ref mut new,
            } => {
                let (old_record_hash, new_record_hash) = if self.dimensions.is_empty() {
                    (self.default_segment_key, self.default_segment_key)
                } else {
                    (
                        get_key(&self.input_schema, old, &self.dimensions)?,
                        get_key(&self.input_schema, new, &self.dimensions)?,
                    )
                };

                if old_record_hash == new_record_hash {
                    Ok(self.agg_update(old, new, old_record_hash)?)
                } else {
                    let mut r = Vec::with_capacity(2);
                    r.extend(self.agg_delete(old)?);
                    r.extend(self.agg_insert(new)?);
                    Ok(r)
                }
            }
        }
    }
}

fn get_key(
    schema: &Schema,
    record: &ProcessorRecordRef,
    dimensions: &[Expression],
) -> Result<u64, PipelineError> {
    let mut key = Vec::<Field>::with_capacity(dimensions.len());
    for dimension in dimensions.iter() {
        key.push(dimension.evaluate(record.get_record(), schema)?);
    }
    let mut hasher = AHasher::default();
    key.hash(&mut hasher);
    let v = hasher.finish();
    Ok(v)
}

impl Processor for AggregationProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let ops = self.aggregate(op)?;
        for fop in ops {
            fw.send(fop, DEFAULT_PORT_HANDLE);
        }
        Ok(())
    }
}
