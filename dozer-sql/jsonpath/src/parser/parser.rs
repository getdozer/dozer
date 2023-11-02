use crate::parser::model::FilterExpression::{And, Or};
use crate::parser::model::{
    FilterExpression, FilterSign, Function, JsonPath, JsonPathIndex, Operand,
};
use dozer_types::json_types::JsonValue;
use pest::error::Error;
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "parser/grammar/json_path.pest"]
pub struct JsonPathParser;

/// the parsing function.
/// Since the parsing can finish with error the result is [[Result]]
#[allow(clippy::result_large_err)]
pub fn parse_json_path(jp_str: &str) -> Result<JsonPath, Error<Rule>> {
    Ok(parse_internal(
        JsonPathParser::parse(Rule::path, jp_str)?.next().unwrap(),
    ))
}

/// Internal function takes care of the logic by parsing the operators and unrolling the string into the final result.
fn parse_internal(rule: Pair<Rule>) -> JsonPath {
    match rule.as_rule() {
        Rule::path => rule
            .into_inner()
            .next()
            .map(parse_internal)
            .unwrap_or(JsonPath::Empty),
        Rule::current => JsonPath::Current(Box::new(
            rule.into_inner()
                .next()
                .map(parse_internal)
                .unwrap_or(JsonPath::Empty),
        )),
        Rule::chain => JsonPath::Chain(rule.into_inner().map(parse_internal).collect()),
        Rule::root => JsonPath::Root,
        Rule::wildcard => JsonPath::Wildcard,
        Rule::descent => parse_key(down(rule))
            .map(JsonPath::Descent)
            .unwrap_or(JsonPath::Empty),
        Rule::descent_w => JsonPath::DescentW,
        Rule::function => JsonPath::Fn(Function::Length),
        Rule::field => parse_key(down(rule))
            .map(JsonPath::Field)
            .unwrap_or(JsonPath::Empty),
        Rule::index => JsonPath::Index(parse_index(rule)),
        _ => JsonPath::Empty,
    }
}

/// parsing the rule 'key' with the structures either .key or .]'key'[
fn parse_key(rule: Pair<Rule>) -> Option<String> {
    match rule.as_rule() {
        Rule::key | Rule::key_unlim | Rule::string_qt => parse_key(down(rule)),
        Rule::key_lim | Rule::inner => Some(String::from(rule.as_str())),
        _ => None,
    }
}

fn parse_slice(mut pairs: Pairs<Rule>) -> JsonPathIndex {
    let mut start = 0;
    let mut end = 0;
    let mut step = 1;
    while pairs.peek().is_some() {
        let in_pair = pairs.next().unwrap();
        match in_pair.as_rule() {
            Rule::start_slice => start = in_pair.as_str().parse::<i32>().unwrap_or(start),
            Rule::end_slice => end = in_pair.as_str().parse::<i32>().unwrap_or(end),
            Rule::step_slice => step = down(in_pair).as_str().parse::<usize>().unwrap_or(step),
            _ => (),
        }
    }
    JsonPathIndex::Slice(start, end, step)
}

fn parse_unit_keys(mut pairs: Pairs<Rule>) -> JsonPathIndex {
    let mut keys = vec![];

    while pairs.peek().is_some() {
        keys.push(String::from(down(pairs.next().unwrap()).as_str()));
    }
    JsonPathIndex::UnionKeys(keys)
}

fn number_to_value(number: &str) -> JsonValue {
    number.parse::<f64>().ok().map(JsonValue::from).unwrap()
}

fn parse_unit_indexes(mut pairs: Pairs<Rule>) -> JsonPathIndex {
    let mut keys = vec![];

    while pairs.peek().is_some() {
        keys.push(number_to_value(pairs.next().unwrap().as_str()));
    }
    JsonPathIndex::UnionIndex(keys)
}

fn parse_chain_in_operand(rule: Pair<Rule>) -> Operand {
    match parse_internal(rule) {
        JsonPath::Chain(elems) => {
            if elems.len() == 1 {
                match elems.first() {
                    Some(JsonPath::Index(JsonPathIndex::UnionKeys(keys))) => {
                        Operand::val(JsonValue::from(keys.clone()))
                    }
                    Some(JsonPath::Index(JsonPathIndex::UnionIndex(keys))) => {
                        Operand::val(JsonValue::from(keys.clone()))
                    }
                    Some(JsonPath::Field(f)) => Operand::val(vec![f].into()),
                    _ => Operand::Dynamic(Box::new(JsonPath::Chain(elems))),
                }
            } else {
                Operand::Dynamic(Box::new(JsonPath::Chain(elems)))
            }
        }
        jp => Operand::Dynamic(Box::new(jp)),
    }
}

fn parse_filter_index(pair: Pair<Rule>) -> JsonPathIndex {
    JsonPathIndex::Filter(parse_logic(pair.into_inner()))
}

fn parse_logic(mut pairs: Pairs<Rule>) -> FilterExpression {
    let mut expr: Option<FilterExpression> = None;
    while pairs.peek().is_some() {
        let next_expr = parse_logic_and(pairs.next().unwrap().into_inner());
        match expr {
            None => expr = Some(next_expr),
            Some(e) => expr = Some(Or(Box::new(e), Box::new(next_expr))),
        }
    }
    expr.unwrap()
}

fn parse_logic_and(mut pairs: Pairs<Rule>) -> FilterExpression {
    let mut expr: Option<FilterExpression> = None;

    while pairs.peek().is_some() {
        let next_expr = parse_logic_atom(pairs.next().unwrap().into_inner());
        match expr {
            None => expr = Some(next_expr),
            Some(e) => expr = Some(And(Box::new(e), Box::new(next_expr))),
        }
    }
    expr.unwrap()
}

fn parse_logic_atom(mut pairs: Pairs<Rule>) -> FilterExpression {
    match pairs.peek().map(|x| x.as_rule()) {
        Some(Rule::logic) => parse_logic(pairs.next().unwrap().into_inner()),
        Some(Rule::atom) => {
            let left: Operand = parse_atom(pairs.next().unwrap());
            if pairs.peek().is_none() {
                FilterExpression::exists(left)
            } else {
                let sign: FilterSign = FilterSign::new(pairs.next().unwrap().as_str());
                let right: Operand = parse_atom(pairs.next().unwrap());
                FilterExpression::Atom(left, sign, right)
            }
        }
        Some(x) => panic!("unexpected => {:?}", x),
        None => panic!("unexpected none"),
    }
}

fn parse_atom(rule: Pair<Rule>) -> Operand {
    let atom = down(rule.clone());
    match atom.as_rule() {
        Rule::number => Operand::Static(number_to_value(rule.as_str())),
        Rule::string_qt => Operand::Static(JsonValue::from(down(atom).as_str())),
        Rule::chain => parse_chain_in_operand(down(rule)),
        Rule::boolean => Operand::Static(rule.as_str().parse::<bool>().unwrap().into()),
        _ => Operand::Static(JsonValue::NULL),
    }
}

fn parse_index(rule: Pair<Rule>) -> JsonPathIndex {
    let next = down(rule);
    match next.as_rule() {
        Rule::unsigned => JsonPathIndex::Single(number_to_value(next.as_str())),
        Rule::slice => parse_slice(next.into_inner()),
        Rule::unit_indexes => parse_unit_indexes(next.into_inner()),
        Rule::unit_keys => parse_unit_keys(next.into_inner()),
        Rule::filter => parse_filter_index(down(next)),
        _ => JsonPathIndex::Single(number_to_value(next.as_str())),
    }
}

fn down(rule: Pair<Rule>) -> Pair<Rule> {
    rule.into_inner().next().unwrap()
}
