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
            Regex::new(r#"^insert into "((?:C##)?\w+)"\."(\w+)"\n *values\n(?s)(.+)$"#).unwrap();
        Self {
            regex,
            row_parser: row::Parser::new(",", ";"),
        }
    }

    pub fn parse(&self, sql_redo: &str, table_pair: &(String, String)) -> Result<ParsedRow, Error> {
        let captures = self
            .regex
            .captures(sql_redo)
            .ok_or_else(|| Error::InsertFailedToMatch(sql_redo.to_string()))?;
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
    let sql_redo = r#"insert into "HR"."EMPLOYEES"
    values
      "EMPLOYEE_ID" = 306,
      "FIRST_NAME" = 'Nandini',
      "LAST_NAME" = 'Shastry',
      "EMAIL" = 'NSHASTRY',
      "PHONE_NUMBER" = '1234567890',
      "JOB_ID" = 'HR_REP',
      "SALARY" = 120000,
      "COMMISSION_PCT" = .05,
      "MANAGER_ID" = 105,
      "NULL_FIELD" IS NULL,
      "DEPARTMENT_ID" = 10;
    "#;
    let parsed = parser
        .parse(sql_redo, &("HR".to_string(), "EMPLOYEES".to_string()))
        .unwrap();
    assert_eq!(parsed.len(), 11);
}
