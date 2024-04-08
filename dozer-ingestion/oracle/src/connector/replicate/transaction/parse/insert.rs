use std::borrow::Cow;

use fxhash::FxHashMap;
use memchr::{memchr, memchr3_iter, memmem};

use super::ParsedRow;

const INSERT_INTO: &[u8] = "insert into ".as_bytes();
const UPDATE: &[u8] = "update ".as_bytes();
const DELETE_FROM: &[u8] = "delete from ".as_bytes();

const VALUES: &[u8] = " values ".as_bytes();
const SET: &[u8] = " set ".as_bytes();
const WHERE: &[u8] = " where ".as_bytes();
const UNSUPPORTED_TYPE: &[u8] = "Unsupported Type".as_bytes();
const UNSUPPORTED: &[u8] = "Unsupported".as_bytes();
const NULL: &[u8] = "NULL".as_bytes();
const IS_NULL: &[u8] = "IS NULL".as_bytes();
const AND: &[u8] = "and ".as_bytes();
const OR: &[u8] = "or ".as_bytes();

pub(super) struct DmlParser<'a> {
    remaining: &'a [u8],
    column_indices: &'a FxHashMap<String, usize>,
}

impl<'a> DmlParser<'a> {
    pub(super) fn new(sql: &'a str, column_indices: &'a FxHashMap<String, usize>) -> Self {
        Self {
            remaining: sql.as_bytes(),
            column_indices,
        }
    }

    pub(super) fn parse_insert(&mut self) -> Option<ParsedRow> {
        self.remaining = self.remaining.get(INSERT_INTO.len()..)?;
        self.parse_table_name()?;

        let parsed_col_names = self.parse_column_names()?;
        self.parse_column_values(parsed_col_names)
    }

    pub(super) fn parse_update(&mut self) -> Option<(ParsedRow, ParsedRow)> {
        self.remaining = self.remaining.get(UPDATE.len()..)?;

        self.parse_table_name()?;

        let new = dbg!(self.parse_set_clause())?;
        let old = self.parse_where_clause()?;
        Some((old, new))
    }

    pub(super) fn parse_delete(&mut self) -> Option<ParsedRow> {
        self.remaining = self.remaining.get(DELETE_FROM.len()..)?;

        self.parse_table_name()?;

        self.parse_where_clause()
    }

    fn parse_column_names(&mut self) -> Option<Vec<&'a str>> {
        let mut in_quotes = false;
        let mut ret = vec![];
        let mut start = 0;
        for i in memchr3_iter(b'(', b')', b'"', self.remaining) {
            let c = self.remaining[i];
            if c == b')' && !in_quotes {
                self.remaining = &self.remaining[i + 1..];
                return Some(ret);
            } else if c == b'(' {
                continue;
            } else {
                if in_quotes {
                    // This came from utf-8 and the found character cannot split
                    // a UTF-8 character, so this unwrap should never panic
                    ret.push(unsafe { std::str::from_utf8_unchecked(&self.remaining[start..i]) });
                } else {
                    start = i + 1;
                }
                in_quotes = !in_quotes;
            }
        }
        None
    }

    fn parse_column_values(
        &mut self,
        column_names: Vec<&str>,
    ) -> Option<Vec<Option<Cow<'a, str>>>> {
        if !self.remaining.starts_with(VALUES) {
            return None;
        }
        self.remaining = &self.remaining[VALUES.len()..];

        let start_of_values = memchr(b'(', self.remaining)?;
        self.remaining = &self.remaining[start_of_values + 1..];
        let mut nesting_level = 0;
        let mut in_quotes = false;
        let mut start_of_value = 0;
        let mut quoted_value_contains_quote = false;
        let mut ret = vec![None; self.column_indices.len()];
        let mut col_no = 0;
        let mut i = 0;
        while i < self.remaining.len() {
            let c = self.remaining[i];
            if in_quotes {
                i += memchr(b'\'', &self.remaining[i..])?;
                if *self.remaining.get(i + 1)? == b'\'' {
                    // Skip the next one
                    quoted_value_contains_quote = true;
                    i += 2;
                    continue;
                }
                in_quotes = false;
                i += 1;
                continue;
            }

            if c == b'\'' {
                in_quotes = true
            } else if c == b'(' && !in_quotes {
                nesting_level += 1;
            } else if !in_quotes && (c == b',' || c == b')') {
                if c == b')' && nesting_level > 0 {
                    nesting_level -= 1;
                    i += 1;
                    continue;
                }

                if c == b',' && nesting_level > 0 {
                    i += 1;
                    continue;
                }

                let value = if self.remaining[start_of_value] == b'\'' {
                    assert_eq!(self.remaining[i - 1], b'\'');
                    Some(if quoted_value_contains_quote {
                        Cow::Owned(
                            unsafe {
                                std::str::from_utf8_unchecked(
                                    &self.remaining[start_of_value + 1..(i - 1)],
                                )
                            }
                            .replace("''", "'"),
                        )
                    } else {
                        Cow::Borrowed(unsafe {
                            std::str::from_utf8_unchecked(
                                &self.remaining[start_of_value + 1..(i - 1)],
                            )
                        })
                    })
                } else {
                    let s = &self.remaining[start_of_value..i];
                    assert_ne!(s, UNSUPPORTED_TYPE);
                    if s == NULL {
                        None
                    } else {
                        Some(Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(s) }))
                    }
                };
                // Get column index
                if let Some(index) = self.column_indices.get(column_names[col_no]) {
                    ret[*index] = value;
                }
                quoted_value_contains_quote = false;

                col_no += 1;
                start_of_value = i + 1;
            }
            i += 1;
        }
        Some(ret)
    }

    fn parse_table_name(&mut self) -> Option<()> {
        let mut in_quotes = false;
        let mut index = 0;
        for i in memchr3_iter(b' ', b'(', b'"', self.remaining) {
            let c = self.remaining[i];
            index = i;
            if c == b'"' {
                in_quotes = !in_quotes;
            } else if !in_quotes {
                break;
            }
        }
        self.remaining = &self.remaining[index..];
        Some(())
    }

    fn parse_set_clause(&mut self) -> Option<ParsedRow<'a>> {
        self.remaining = &self.remaining[memmem::find(self.remaining, SET)? + SET.len()..];

        let mut i = 0;
        let mut in_single_quotes = false;
        let mut in_double_quotes = false;
        let mut quoted_value_contains_quote = false;
        let mut in_column_name = true;
        let mut in_column_value = false;
        let mut in_special = false;
        let mut column_name = "";
        let mut start = 0;
        let mut nesting_level = 0;
        let mut values = vec![None; self.column_indices.len()];

        while i < self.remaining.len() {
            let c = self.remaining[i];
            let lookahead = self.remaining.get(i + 1);
            if in_single_quotes {
                i += memchr(b'\'', &self.remaining[i..])?;
                if *self.remaining.get(i + 1)? == b'\'' {
                    // Skip the next one
                    quoted_value_contains_quote = true;
                    i += 2;
                    continue;
                }
                in_single_quotes = false;
                if nesting_level == 0 {
                    let pos = self.column_indices.get(column_name)?;
                    values[*pos] = Some(if quoted_value_contains_quote {
                        Cow::Owned(
                            unsafe { std::str::from_utf8_unchecked(&self.remaining[start + 1..i]) }
                                .replace("''", "'"),
                        )
                    } else {
                        Cow::Borrowed(unsafe {
                            std::str::from_utf8_unchecked(&self.remaining[start + 1..i])
                        })
                    });
                    start = i + 1;
                    in_column_name = false;
                    in_column_value = false;
                }
                i += 1;
                continue;
            }

            if c == b'"' && in_column_name {
                if in_double_quotes {
                    in_double_quotes = false;
                    column_name =
                        unsafe { std::str::from_utf8_unchecked(&self.remaining[start + 1..i]) };
                    in_column_name = false;
                    start = i + 1;
                    i += 1;
                    continue;
                }
                in_double_quotes = true;
                start = i;
            } else if c == b'=' && !in_column_name && !in_column_value {
                in_column_value = true;
                i += 1;
                start = i + 1;
            } else if nesting_level == 0 && c == b' ' && lookahead == Some(&b'|') {
            } else if nesting_level == 0
                && c == b'|'
                && lookahead == Some(&b'|')
                && !in_single_quotes
            {
                i += memchr(b'\'', &self.remaining[i + 2..])? - 1;
            } else if c == b'\'' && in_column_value {
                if !in_special {
                    start = i;
                }
                in_single_quotes = true;
            } else if c == b',' && !in_column_value && !in_column_name {
                in_column_name = true;
                i += 1;
                start = i;
            } else if in_column_value && !in_single_quotes {
                if !in_special {
                    start = i;
                    in_special = true;
                }

                if c == b'(' {
                    nesting_level += 1;
                } else if c == b')' && nesting_level > 0 {
                    nesting_level -= 1;
                } else if (c == b',' || c == b' ' || c == b';') && nesting_level == 0 {
                    let s = &self.remaining[start..i];
                    assert_ne!(s, UNSUPPORTED_TYPE);
                    assert_ne!(s, UNSUPPORTED);
                    if s != NULL {
                        if let Some(pos) = self.column_indices.get(column_name) {
                            values[*pos] = Some(unsafe { std::str::from_utf8_unchecked(s) }.into());
                        }
                    }
                    start = i + 1;
                    in_column_value = false;
                    in_special = false;
                    in_column_name = true;
                }
            } else if !in_double_quotes
                && !in_single_quotes
                && self.remaining[i..].starts_with(WHERE)
            {
                break;
            }
            i += 1;
        }
        self.remaining = &self.remaining[i..];
        Some(values)
    }

    fn parse_where_clause(&mut self) -> Option<ParsedRow<'a>> {
        self.remaining = &self.remaining[memmem::find(self.remaining, WHERE)? + WHERE.len()..];

        let mut i = 0;

        let mut in_single_quotes = false;
        let mut in_double_quotes = false;
        let mut quoted_value_contains_quote = false;
        let mut in_column_name = true;
        let mut in_column_value = false;
        let mut in_special = false;
        let mut column_name = "";
        let mut start = 0;
        let mut nesting_level = 0;
        let mut values = vec![None; self.column_indices.len()];

        while i < self.remaining.len() {
            let c = self.remaining[i];
            let lookahead = self.remaining.get(i + 1);
            if in_single_quotes {
                i += memchr(b'\'', &self.remaining[i..])?;
                if *self.remaining.get(i + 1)? == b'\'' {
                    // Skip the next one
                    quoted_value_contains_quote = true;
                    i += 2;
                    continue;
                }
                in_single_quotes = false;
                if nesting_level == 0 {
                    if let Some(pos) = self.column_indices.get(column_name) {
                        values[*pos] = Some(if quoted_value_contains_quote {
                            Cow::Owned(
                                unsafe {
                                    std::str::from_utf8_unchecked(&self.remaining[start + 1..i])
                                }
                                .replace("''", "'"),
                            )
                        } else {
                            Cow::Borrowed(unsafe {
                                std::str::from_utf8_unchecked(&self.remaining[start + 1..i])
                            })
                        });
                    }
                    start = i + 1;
                    in_column_name = false;
                    in_column_value = false;
                }
                i += 1;
                continue;
            }

            if c == b'"' && in_column_name {
                if in_double_quotes {
                    in_double_quotes = false;
                    column_name =
                        unsafe { std::str::from_utf8_unchecked(&self.remaining[start + 1..i]) };
                    in_column_name = false;
                    start = i + 1;
                    i += 1;
                    continue;
                }
                in_double_quotes = true;
                start = i;
            } else if c == b'=' && !in_column_name && !in_column_value {
                in_column_value = true;
                i += 1;
                start = i + 1;
            } else if c == b'I' && !in_column_name && !in_column_value {
                if self.remaining[i..].starts_with(IS_NULL) {
                    i += IS_NULL.len();
                    start = i;
                    continue;
                }
            } else if nesting_level == 0 && c == b' ' && lookahead == Some(&b'|') {
            } else if nesting_level == 0
                && c == b'|'
                && lookahead == Some(&b'|')
                && !in_single_quotes
            {
                i += memchr(b'\'', &self.remaining[i + 2..])? - 1;
            } else if c == b'\'' && in_column_value {
                if !in_special {
                    start = i;
                }
                in_single_quotes = true;
            } else if c == b',' && !in_column_value && !in_column_name {
                in_column_name = true;
                i += 1;
                start = i;
            } else if in_column_value && !in_single_quotes {
                if !in_special {
                    start = i;
                    in_special = true;
                }

                if c == b'(' {
                    nesting_level += 1;
                } else if c == b')' && nesting_level > 0 {
                    nesting_level -= 1;
                } else if (c == b' ' || c == b';') && nesting_level == 0 {
                    let s = &self.remaining[start..i];
                    assert_ne!(s, UNSUPPORTED_TYPE);
                    assert_ne!(s, UNSUPPORTED);
                    if s != NULL {
                        let pos = self.column_indices.get(column_name)?;
                        values[*pos] = Some(unsafe { std::str::from_utf8_unchecked(s) }.into());
                    }
                    start = i + 1;
                    in_column_value = false;
                    in_special = false;
                    in_column_name = true;
                }
            } else if !in_column_value && !in_column_name {
                if c == b'a' && lookahead == Some(&b'n') && self.remaining[i..].starts_with(AND) {
                    i += AND.len();
                    start = i;
                    in_column_name = true;
                    continue;
                } else if c == b'o'
                    && lookahead == Some(&b'r')
                    && self.remaining[i..].starts_with(OR)
                {
                    // We don't support OR conditions (what would that even mean?)
                    return None;
                }
            }
            i += 1;
        }
        Some(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_insert() {
        let sql = r#"insert into "DOZER"."TEST"("ID","NAME","TS","DATE","C1","C2","N") values ('1','Acme',TO_TIMESTAMP('2020-02-01 00:00:00.'),TO_DATE('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),NULL,NULL,'A''B');
        "#;
        let column_indices = ["NAME", "ID", "TS", "C1", "DATE", "N"]
            .into_iter()
            .enumerate()
            .map(|(i, s)| (s.to_owned(), i))
            .collect();
        assert_eq!(
            DmlParser::new(sql, &column_indices).parse_insert(),
            Some(vec![
                Some("Acme".into()),
                Some("1".into()),
                Some("TO_TIMESTAMP('2020-02-01 00:00:00.')".into()),
                None,
                Some("TO_DATE('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS')".into()),
                Some("A'B".into()),
            ])
        )
    }

    #[test]
    fn test_parse_update() {
        let string = r#"update "DOZER"."TEST" set "ID" = '1', "NAME" = 'Acme', "TS" = TO_TIMESTAMP('2020-02-01 00:00:00.'), "DATE" = TO_DATE('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS'), "C1" = NULL, "C2" = NULL, "N" = 'A''B' where "ID" = '2' and "NAME" = 'Corp' and "TS" = TO_TIMESTAMP('2020-02-01 00:00:00.') and "DATE" = TO_DATE('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS') and "C1" IS NULL and "C2" IS NULL" and "N" = 'A''B';"#;
        let column_indices = ["NAME", "ID", "TS", "C1", "DATE", "N"]
            .into_iter()
            .enumerate()
            .map(|(i, s)| (s.to_owned(), i))
            .collect();
        assert_eq!(
            DmlParser {
                remaining: string.as_bytes(),
                column_indices: &column_indices
            }
            .parse_update(),
            Some((
                vec![
                    Some("Corp".into()),
                    Some("2".into()),
                    Some("TO_TIMESTAMP('2020-02-01 00:00:00.')".into()),
                    None,
                    Some("TO_DATE('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS')".into()),
                    Some("A'B".into()),
                ],
                vec![
                    Some("Acme".into()),
                    Some("1".into()),
                    Some("TO_TIMESTAMP('2020-02-01 00:00:00.')".into()),
                    None,
                    Some("TO_DATE('2020-02-01 00:00:00','YYYY-MM-DD HH24:MI:SS')".into()),
                    Some("A'B".into()),
                ],
            ))
        );
    }
}
