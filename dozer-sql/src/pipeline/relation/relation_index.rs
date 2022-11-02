// use crate::pipeline::expression::execution::Expression;
// use dozer_core::state::indexer::StateStoreIndexer;
// use dozer_types::core::node::PortHandle;
// use dozer_types::errors::pipeline::PipelineError;
// use dozer_types::types::{Field, FieldDefinition, Operation, Record};
//
// pub struct RelationIndex {
//     id: PortHandle,
//     indexer: StateStoreIndexer,
//     pk_indexes: Vec<usize>,
//     fk_indexes: Vec<(u16, Expression)>,
//     vals_indexes: Option<Vec<usize>>,
// }
//
// impl RelationIndex {
//     pub fn new(
//         id: PortHandle,
//         indexer: StateStoreIndexer,
//         pk_indexes: Vec<usize>,
//         fk_indexes: Vec<(u16, Expression)>,
//         vals_indexes: Option<Vec<usize>>,
//     ) -> Self {
//         Self {
//             id,
//             indexer,
//             pk_indexes,
//             fk_indexes,
//             vals_indexes,
//         }
//     }
//
//     fn extract_fields(&self, in_rec: Record) -> Result<Record, PipelineError> {
//         Ok(match &self.vals_indexes {
//             Some(ls) => {
//                 let mut out_rec: Vec<Field> = Vec::with_capacity(self.vals_indexes.len());
//                 for i in ls {
//                     r.push(in_rec.get_value(*i)?);
//                 }
//                 Record::new(None, out_rec)
//             }
//             _ => r,
//         })
//     }
//
//     pub fn handle(&self, op: Operation) -> Result<(), PipelineError> {
//         match op {
//             Operation::Update { old, new } => {
//                 out_rec = self.extract_fields(new);
//             }
//             Operation::Delete { old } => {}
//             Operation::Insert { new } => {}
//         }
//
//         Ok(())
//     }
// }
