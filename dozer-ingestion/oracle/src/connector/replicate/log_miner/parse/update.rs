use dozer_ingestion_connector::dozer_types::log::warn;
use regex::Regex;

use crate::connector::Error;

use super::{row, ParsedRow};

#[derive(Debug, Clone)]
pub struct Parser {
    regex: Regex,
    new_row_parser: row::Parser,
    old_row_parser: row::Parser,
}

impl Parser {
    pub fn new() -> Self {
        let regex =
            Regex::new(r#"^update "(\w+)"\."(\w+)"\n *(?s)(.+) *where\n(?s)(.+)$"#).unwrap();
        Self {
            regex,
            new_row_parser: row::Parser::new(",", "\n"),
            old_row_parser: row::Parser::new(" and", ";"),
        }
    }

    pub fn parse(
        &self,
        sql_redo: &str,
        table_pair: &(String, String),
    ) -> Result<(ParsedRow, ParsedRow), Error> {
        let captures = self
            .regex
            .captures(sql_redo)
            .ok_or_else(|| Error::UpdateFailedToMatch(sql_redo.to_string()))?;
        let owner = captures.get(1).unwrap().as_str();
        let table_name = captures.get(2).unwrap().as_str();
        if owner != table_pair.0 || table_name != table_pair.1 {
            warn!(
                "Table name {}.{} doesn't match {}.{} in log content",
                owner, table_name, table_pair.0, table_pair.1
            );
        }

        let mut new_row = self
            .new_row_parser
            .parse(captures.get(3).unwrap().as_str())?;
        let old_row = self
            .old_row_parser
            .parse(captures.get(4).unwrap().as_str())?;
        for (column, old_value) in old_row.iter() {
            if !new_row.contains_key(column) {
                new_row.insert(column.clone(), old_value.clone());
            }
        }
        Ok((old_row, new_row))
    }
}

#[test]
fn test_parse() {
    use super::ParsedValue;

    let parser = Parser::new();
    let sql_redo = r#"update "DOZER"."TRANSACTIONS"
        "TYPE" = 'REBATE'
    where
        "TRANSACTION_ID" = 12001 and
        "CUSTOMER_ID" = 63147 and
        "TYPE" = 'Withdrawal' and
        "AMOUNT" = 9691.34 and
        "CURRENCY" = 'USD' and
        "TRANSACTION_DATE" = '28-JAN-24' and
        "STATUS" = 'Completed' and
        "DESCRIPTION" = 'Yeah become language inside purpose.';
    "#;
    let (old, new) = parser
        .parse(sql_redo, &("HR".to_string(), "EMPLOYEES".to_string()))
        .unwrap();
    assert_eq!(old.len(), 8);
    assert_eq!(new.len(), 8);
    assert_eq!(
        old.get("TRANSACTION_ID").unwrap(),
        &ParsedValue::Number("12001".parse().unwrap())
    );
    assert_eq!(
        new.get("TRANSACTION_ID").unwrap(),
        &ParsedValue::Number("12001".parse().unwrap())
    );
    assert_eq!(
        old.get("TYPE").unwrap(),
        &ParsedValue::String("Withdrawal".to_string())
    );
    assert_eq!(
        new.get("TYPE").unwrap(),
        &ParsedValue::String("REBATE".to_string())
    );
}
