use crate::types::Field;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde_json::json;

#[test]
fn test_field_serialize() {
    let int_v = Field::Int(1_i64);
    let int_b = int_v.to_bytes().unwrap();
    assert_eq!(int_v, Field::from_bytes(int_b).unwrap());

    let uint_v = Field::UInt(1_u64);
    let uint_b = uint_v.to_bytes().unwrap();
    assert_eq!(uint_v, Field::from_bytes(uint_b).unwrap());

    let float_v = Field::Float(OrderedFloat::from(1_f64));
    let float_b = float_v.to_bytes().unwrap();
    assert_eq!(float_v, Field::from_bytes(float_b).unwrap());

    let bool_v = Field::Boolean(true);
    let bool_b = bool_v.to_bytes().unwrap();
    assert_eq!(bool_v, Field::from_bytes(bool_b).unwrap());

    let str_v = Field::String("1".to_string());
    let str_b = str_v.to_bytes().unwrap();
    assert_eq!(str_v, Field::from_bytes(str_b).unwrap());

    let txt_v = Field::Text("1".to_string());
    let txt_b = txt_v.to_bytes().unwrap();
    assert_eq!(txt_v, Field::from_bytes(txt_b).unwrap());

    let bin_v = Field::Binary(Vec::from("1"));
    let bin_b = bin_v.to_bytes().unwrap();
    assert_eq!(bin_v, Field::from_bytes(bin_b).unwrap());

    let dec_v = Field::Decimal(Decimal::new(1, 0));
    let dec_b = dec_v.to_bytes().unwrap();
    assert_eq!(dec_v, Field::from_bytes(dec_b).unwrap());

    let ts_v = Field::Timestamp(DateTime::from(Utc.timestamp_millis(1000)));
    let ts_b = ts_v.to_bytes().unwrap();
    assert_eq!(ts_v, Field::from_bytes(ts_b).unwrap());

    let date_v = Field::Date(NaiveDate::from_ymd(2022, 12, 6));
    let date_b = date_v.to_bytes().unwrap();
    assert_eq!(date_v, Field::from_bytes(date_b).unwrap());

    let bson_v = Field::Bson(bincode::serialize(&json!({"a": 1})).unwrap());
    let bson_b = bson_v.to_bytes().unwrap();
    assert_eq!(bson_v, Field::from_bytes(bson_b).unwrap());

    let null_v = Field::Null;
    let null_b = null_v.to_bytes().unwrap();
    assert_eq!(null_v, Field::from_bytes(null_b).unwrap());
}
