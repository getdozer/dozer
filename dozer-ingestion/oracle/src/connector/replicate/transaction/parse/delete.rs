use dozer_ingestion_connector::dozer_types::log::warn;
use regex::Regex;

use crate::connector::Error;

use super::{row, ParsedRow};

#[derive(Debug, Clone)]
pub struct Parser {
    regex: Regex,
    row_parser: row::Parser,
}

impl Parser {
    pub fn new() -> Self {
        let regex =
            Regex::new(r#"^delete from "((?:C##)?\w+)"\."(\w+)"\n *where\n(?s)(.+)$"#).unwrap();
        Self {
            regex,
            row_parser: row::Parser::new(" and", ";"),
        }
    }

    pub fn parse(&self, sql_redo: &str, table_pair: &(String, String)) -> Result<ParsedRow, Error> {
        let captures = self
            .regex
            .captures(sql_redo)
            .ok_or_else(|| Error::DeleteFailedToMatch(sql_redo.to_string()))?;
        let owner = captures.get(1).unwrap().as_str();
        let table_name = captures.get(2).unwrap().as_str();
        if owner != table_pair.0 || table_name != table_pair.1 {
            warn!(
                "Table name {}.{} doesn't match {}.{} in log content",
                owner, table_name, table_pair.0, table_pair.1
            );
        }

        self.row_parser.parse(captures.get(3).unwrap().as_str())
    }
}

#[test]
fn test_parse() {
    let parser = Parser::new();
    let sql_redo = r#"delete from "HR"."EMPLOYEES"
    where
       "EMPLOYEE_ID" = 306 and
       "FIRST_NAME" = 'Nandini' and
       "LAST_NAME" = 'Shastry' and
       "EMAIL" = 'NSHASTRY' and
       "PHONE_NUMBER" = '1234567890' and
       "JOB_ID" = 'HR_REP' and
       "SALARY" = 120000 and
       "COMMISSION_PCT" = .05 and
       "MANAGER_ID" = 105 and
       "DEPARTMENT_ID" = 10;
    "#;
    let parsed = parser
        .parse(sql_redo, &("HR".to_string(), "EMPLOYEES".to_string()))
        .unwrap();
    assert_eq!(parsed.len(), 10);
}
