use std::sync::Arc;

use dozer_schema::registry::{SchemaRegistryClient};


use super::{
    super::models::{api_endpoint::ApiEndpoint, source::Source},
    executor::Executor,
};
use crate::Orchestrator;

pub struct SimpleOrchestrator {
    pub sources: Vec<Source>,
    pub api_endpoint: Option<ApiEndpoint>,
    pub schema_client: Arc<SchemaRegistryClient>,
}

impl Orchestrator for SimpleOrchestrator {
    fn add_sources(&mut self, sources: Vec<Source>) -> &mut Self {
        for source in sources.iter() {
            self.sources.push(source.to_owned());
        }
        self
    }

    fn add_endpoint(&mut self, endpoint: ApiEndpoint) -> &mut Self {
        self.api_endpoint = Some(endpoint);
        self
    }

    fn run(&mut self) -> anyhow::Result<()> {
        Executor::run(&self)
    }
}

impl SimpleOrchestrator {
    pub fn new(schema_client: Arc<SchemaRegistryClient>) -> Self {
        Self {
            sources: vec![],
            api_endpoint: None,
            schema_client,
        }
    }
}

// #[test]
// mod tests {
//     let sql = "SELECT Country, COUNT(Spending), ROUND(SUM(ROUND(Spending))) \
//                             FROM Customers \
//                             WHERE Spending >= 1000 \
//                             GROUP BY Country \
//                             HAVING COUNT(CustomerID) > 1;";

//         let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

//         let ast = Parser::parse_sql(&dialect, sql).unwrap();
//         println!("AST: {:?}", ast);

//         let statement: &Statement = &ast[0];

//         let schema = Schema {
//             fields: vec![
//                 FieldDefinition {
//                     name: String::from("CustomerID"),
//                     typ: FieldType::Int,
//                     nullable: false,
//                 },
//                 FieldDefinition {
//                     name: String::from("Country"),
//                     typ: FieldType::String,
//                     nullable: false,
//                 },
//                 FieldDefinition {
//                     name: String::from("Spending"),
//                     typ: FieldType::Int,
//                     nullable: false,
//                 },
//             ],
//             values: vec![0],
//             primary_index: vec![],
//             secondary_indexes: vec![],
//             identifier: None,
//         };

// }
