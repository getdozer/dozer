use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use dozer_cache::cache::{expression::QueryExpression, index, LmdbCache, Cache};
use dozer_types::{record_to_json, json_value_to_field};
use serde_json::Value;

pub fn get_record(cache: Arc<LmdbCache>, key: Value) -> anyhow::Result<HashMap<String, Value>> {
  let key = match json_value_to_field(key) {
      Ok(key) => key,
      Err(e) => {
          panic!("error : {:?}", e);
      }
  };
  let key = index::get_primary_key(&[0], &[key]);
  let rec = cache.get(&key).context("record not found")?;
  let schema = cache.get_schema(&rec.schema_id.to_owned().context("schema_id not found")?)?;
  record_to_json(&rec, &schema)
}

/// Get multiple records
pub fn get_records(
  cache: Arc<LmdbCache>,
  exp: QueryExpression,
) -> anyhow::Result<Vec<HashMap<String, Value>>> {
  let records = cache.query("films", &exp)?;
  let schema = cache.get_schema(
      &records[0]
          .schema_id
          .to_owned()
          .context("schema_id not found")?,
  )?;
  let mut maps = vec![];
  for rec in records.iter() {
      let map = record_to_json(rec, &schema)?;
      maps.push(map);
  }
  Ok(maps)
}