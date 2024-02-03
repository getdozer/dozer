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
            Regex::new(r#"^update "(\w+)"\."(\w+)"\n *set\n(?s)(.+) *where\n(?s)(.+)$"#).unwrap();
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
    let sql_redo = r#"update "OE"."PRODUCT_INFORMATION"
    set
      "WARRANTY_PERIOD" = 'TO_YMINTERVAL('+05-00')'
    where
      "PRODUCT_ID" = '1799' and
      "WARRANTY_PERIOD" = 'TO_YMINTERVAL('+01-00')';
    "#;
    let (old, new) = parser
        .parse(sql_redo, &("HR".to_string(), "EMPLOYEES".to_string()))
        .unwrap();
    assert_eq!(old.len(), 2);
    assert_eq!(new.len(), 2);
    assert_eq!(
        old.get("PRODUCT_ID").unwrap(),
        &ParsedValue::String("1799".to_string())
    );
    assert_eq!(
        new.get("PRODUCT_ID").unwrap(),
        &ParsedValue::String("1799".to_string())
    );
    assert_eq!(
        old.get("WARRANTY_PERIOD").unwrap(),
        &ParsedValue::String("TO_YMINTERVAL('+01-00')".to_string())
    );
    assert_eq!(
        new.get("WARRANTY_PERIOD").unwrap(),
        &ParsedValue::String("TO_YMINTERVAL('+05-00')".to_string())
    );
}
