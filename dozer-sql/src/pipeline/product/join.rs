use sqlparser::ast::TableWithJoins;

use crate::pipeline::errors::PipelineError;

use crate::pipeline::product::join::PipelineError::InvalidExpression;

// pub struct JoinOperator {
//     left: Record,
//     operator: JoinOperatorType,
//     right: Schema,
//     contraint: Expression,
// }

// pub enum JoinOperatorType {
//     NaturalJoin,
// }

// impl JoinOperatorType {
//     pub fn execute(
//         &self,
//         left: &[Record],
//         right: &[Record],
//         contraint: &Expression,
//     ) -> Result<Record, PipelineError> {
//         let (record, schema) = match self {
//             JoinOperatorType::NaturalJoin => execute_natural_join(left, right),
//         };
//         execute_selection(expression, record)
//     }
// }

pub struct TableName {
    name: String,
    alias: String,
}

pub fn get_from_clause_table_names(
    from_clause: &[TableWithJoins],
) -> Result<Vec<TableName>, PipelineError> {
    // from_clause
    //     .into_iter()
    //     .map(|item| get_table_names(item))
    //     .flat_map(|result| match result {
    //         Ok(vec) => vec.into_iter().map(Ok).collect(),
    //         Err(err) => vec![PipelineError::InvalidExpression(err.to_string())],
    //     })
    //     .collect::<Result<Vec<TableName>, PipelineError>>()
    todo!()
}

fn get_table_names(item: &TableWithJoins) -> Result<Vec<TableName>, PipelineError> {
    Err(InvalidExpression("ERR".to_string()))
}
