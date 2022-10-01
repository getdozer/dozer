use std::hash::Hasher;
use std::mem::size_of_val;
use ahash::AHasher;
use anyhow::anyhow;
use dozer_types::types::{Field, Operation, Record, SchemaIdentifier};
use dozer_types::types::Field::{Binary, Boolean, Bson, Decimal, Float, Int, Null, Timestamp};
use crate::aggregation::Aggregator;
use crate::state::{StateStore};


pub enum FieldRule {
    Dimension(usize),
    Measure(Box<dyn Aggregator>)
}


struct SizedAggregationDataset {
    dataset_id: u16,
    output_schema_id: Option<SchemaIdentifier>,
    out_fields_count: usize,
    out_dimensions: Vec<(usize, usize)>,
    out_measures: Vec<(Box<dyn Aggregator>, usize)>
}

enum AggregatorOperation {
    Insert, Delete, Update
}


impl SizedAggregationDataset {

    pub fn new(
        dataset_id: u16, store: &mut dyn StateStore,
        output_schema_id: Option<SchemaIdentifier>, output_fields: Vec<FieldRule>) -> anyhow::Result<SizedAggregationDataset> {

        let out_fields_count = output_fields.len();
        let mut out_measures: Vec<(Box<dyn Aggregator>, usize)> = Vec::new();
        let mut out_dimensions: Vec<(usize, usize)> = Vec::new();
        let mut ctr = 0;

        for rule in output_fields.into_iter() {
            match rule {
                FieldRule::Measure(aggr) => {
                    out_measures.push((aggr, ctr));
                }
                FieldRule::Dimension(idx) => {
                    out_dimensions.push((idx, ctr));
                }
            }
            ctr += 1;
        }

        store.put(&dataset_id.to_ne_bytes(), &0_u16.to_ne_bytes())?;

        Ok(SizedAggregationDataset {
            dataset_id, out_measures, out_dimensions, out_fields_count, output_schema_id
        })
    }

    fn get_record_hash(&self, r: &Record) -> anyhow::Result<u64> {

        let mut hasher = AHasher::default();
        let mut ctr = 0;

        for dimension in &self.out_dimensions {
            hasher.write_i32(ctr);
            match &r.values[dimension.0] {
                Int(i) => { hasher.write_u8(1); hasher.write_i64(*i); }
                Float(f) => { hasher.write_u8(2); hasher.write(&((*f).to_ne_bytes())); }
                Boolean(b) => { hasher.write_u8(3); hasher.write_u8(if *b { 1_u8} else { 0_u8 }); }
                Field::String(s) => { hasher.write_u8(4); hasher.write(s.as_str().as_bytes()); }
                Binary(b) => { hasher.write_u8(5); hasher.write(b.as_ref()); }
                Decimal(d) => { hasher.write_u8(6); hasher.write(&d.serialize()); }
                Timestamp(t) => { hasher.write_u8(7); hasher.write_i64(t.timestamp()) }
                Bson(b) => { hasher.write_u8(8); hasher.write(b.as_ref()); }
                Null => {  hasher.write_u8(0); },
                _ => { return Err(anyhow!("Invalid field type")); }
            }
            ctr += 1;
        }
        Ok(hasher.finish())
    }

    fn fill_dimensions(&self, in_rec: &Record, out_rec: &mut Record) {

        for v in &self.out_dimensions {
            out_rec.values[v.1] = in_rec.values[v.0].clone();
        }
    }

    fn get_record_key(&self, hash: u64, database_id: u16) -> anyhow::Result<Vec<u8>> {
        let mut vec = Vec::with_capacity(size_of_val(&hash) + size_of_val(&database_id));
        vec.extend_from_slice(&database_id.to_ne_bytes());
        vec.extend_from_slice(&hash.to_ne_bytes());
        Ok(vec)
    }

    fn calc_and_fill_measures(
        &self, curr_state: Option<&[u8]>, deleted_record: Option<&Record>, inserted_record: Option<&Record>,
        out_rec_delete: &mut Record, out_rec_insert: &mut Record,
        op: AggregatorOperation) -> anyhow::Result<Vec<u8>> {

        let mut next_state = Vec::<u8>::new();
        let mut offset: usize = 0;

        for measure in &self.out_measures {

            let curr_state_slice = if curr_state.is_none() { None } else {
                let len = u16::from_ne_bytes(curr_state.unwrap()[offset .. offset + 2].try_into().unwrap());
                if len == 0 { None } else {
                    Some(&curr_state.unwrap()[offset + 2..offset + 2 + len as usize])
                }
            };

            if curr_state_slice.is_some() {
                let curr_value = measure.0.get_value(curr_state_slice.unwrap());
                out_rec_delete.values[measure.1] = curr_value;
            }

            let next_state_slice = match op {
                AggregatorOperation::Insert => { measure.0.insert(curr_state_slice, &inserted_record.unwrap())? }
                AggregatorOperation::Delete => { measure.0.delete(curr_state_slice, &deleted_record.unwrap())? }
                AggregatorOperation::Update => { measure.0.update(curr_state_slice, &deleted_record.unwrap(), &inserted_record.unwrap())? }
            };

            next_state.extend((next_state_slice.len() as u16).to_ne_bytes());
            offset += next_state_slice.len() + 2;

            if next_state_slice.len() > 0 {
                let next_value = measure.0.get_value(next_state_slice.as_slice());
                next_state.extend(next_state_slice);
                out_rec_insert.values[measure.1] = next_value;
            }
            else {
                out_rec_insert.values[measure.1] = Field::Null;
            }
        }

        Ok(next_state)

    }

    fn update_segment_count(&self, store: &mut dyn StateStore, key: Vec<u8>, delta: u64, decr: bool) -> anyhow::Result<u64> {

        let bytes = store.get(key.as_slice())?;
        let curr_count = if bytes.is_some() { u64::from_ne_bytes(bytes.unwrap().try_into().unwrap()) } else {0_u64};
        store.put(key.as_slice(), (if decr {curr_count - delta} else {curr_count + delta}).to_ne_bytes().as_slice())?;
        Ok(curr_count)
    }

    fn agg_delete(&self, store: &mut dyn StateStore, old: &Record) -> anyhow::Result<Operation> {

        let mut out_rec_insert = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let mut out_rec_delete = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);

        let record_hash = self.get_record_hash(&old)?;
        let record_key = self.get_record_key(record_hash, self.dataset_id)?;

        let record_count_key = self.get_record_key(record_hash, self.dataset_id + 1)?;
        let prev_count = self.update_segment_count(store, record_count_key, 1, true)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state, Some(&old), None,
            &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Delete
        )?;

        let res =
            if prev_count == 1 {
                self.fill_dimensions(&old, &mut out_rec_delete);
                Operation::Delete {old: out_rec_delete}
            }
            else {
                self.fill_dimensions(&old, &mut out_rec_insert);
                self.fill_dimensions(&old, &mut out_rec_delete);
                Operation::Update {new: out_rec_insert, old: out_rec_delete}
            };

        if prev_count > 0 {
            store.put(record_key.as_slice(), new_state.as_slice())?;
        }
        else {
            store.del(record_key.as_slice())?
        }
        Ok(res)

    }

    fn agg_insert(&self, store: &mut dyn StateStore, new: &Record) -> anyhow::Result<Operation> {

        let mut out_rec_insert = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let mut out_rec_delete = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let record_hash = self.get_record_hash(&new)?;
        let record_key = self.get_record_key(record_hash, self.dataset_id)?;

        let record_count_key = self.get_record_key(record_hash, self.dataset_id + 1)?;
        self.update_segment_count(store, record_count_key, 1, false)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state, None, Some(&new),
            &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Insert
        )?;

        let res =
            if curr_state.is_none() {
                self.fill_dimensions(&new, &mut out_rec_insert);
                Operation::Insert {new: out_rec_insert}
            }
            else {
                self.fill_dimensions(&new, &mut out_rec_insert);
                self.fill_dimensions(&new, &mut out_rec_delete);
                Operation::Update {new: out_rec_insert, old: out_rec_delete}
            };

        store.put(record_key.as_slice(), new_state.as_slice())?;

        Ok(res)

    }

    fn agg_update(&self, store: &mut dyn StateStore, old: &Record, new: &Record, record_hash: u64) -> anyhow::Result<Operation> {

        let mut out_rec_insert = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let mut out_rec_delete = Record::nulls(self.output_schema_id.clone(), self.out_fields_count);
        let record_key = self.get_record_key(record_hash, self.dataset_id)?;

        let curr_state = store.get(record_key.as_slice())?;
        let new_state = self.calc_and_fill_measures(
            curr_state, Some(&old), Some(&new),
            &mut out_rec_delete, &mut out_rec_insert, AggregatorOperation::Update
        )?;

        self.fill_dimensions(&new, &mut out_rec_insert);
        self.fill_dimensions(&old, &mut out_rec_delete);

        let res =
            Operation::Update {new: out_rec_insert, old: out_rec_delete};

        store.put(record_key.as_slice(), new_state.as_slice())?;

        Ok(res)

    }


    fn aggregate(&self, store: &mut dyn StateStore, op: Operation) -> anyhow::Result<Vec<Operation>> {

        match op {
            Operation::Insert {ref new} => {
                Ok(vec![self.agg_insert(store, &new)?])
            }
            Operation::Delete {ref old} => {
                Ok(vec![self.agg_delete(store, &old)?])
            }
            Operation::Update {ref old, ref new} => {

                let old_record_hash = self.get_record_hash(&old)?;
                let new_record_hash = self.get_record_hash(&new)?;

                if old_record_hash == new_record_hash {
                    Ok(vec![self.agg_update(store, old, new, old_record_hash)?])
                }
                else {
                    Ok(vec![self.agg_delete(store, old)?, self.agg_insert(store, &new)?])
                }
            }
            _ => { return Err(anyhow!("Invalid operation".to_string())); }
        }

    }


    fn get_accumulated(&self, _store: &mut dyn StateStore, _key: &[u8]) -> anyhow::Result<Option<Field>> {
        todo!();
    }
}




mod tests {

    use std::fs;
    use std::path::Path;
    use dozer_types::types::{Field, Operation, Record};
    use crate::aggregation::groupby::{FieldRule, SizedAggregationDataset};
    use crate::aggregation::operators::IntegerSumAggregator;
    use crate::state::lmdb::LmdbStateStoreManager;
    use crate::state::memory::MemoryStateStore;


    #[test]
    fn test_insert_update_delete() {

        let mut store = MemoryStateStore::new();

        let agg = SizedAggregationDataset::new(
            0x02_u16, &mut store, None,
            vec![
                FieldRule::Dimension(0), // City
                FieldRule::Dimension(1), // Country
                FieldRule::Measure(Box::new(IntegerSumAggregator::new(3))) // People
            ]
        ).unwrap();

        // Insert 10
        let i = Operation::Insert {
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Insert {
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ])
        });

        // Insert another 10
        let i = Operation::Insert {
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Update {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ]),
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(20)
            ])
        });

        // update from 10 to 5
        let i = Operation::Update {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ]),
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(5)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Update {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(20)
            ]),
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(15)
            ])
        });


        // Delete 5
        let i = Operation::Delete {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(5)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Update {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(15)
            ]),
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ])
        });

        // Delete last 10
        let i = Operation::Delete {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap()[0], Operation::Delete {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::Int(10)
            ])
        });
        println!("ciao")


    }

    #[test]
    fn test_insert_update_change_dims() {

        let mut store = MemoryStateStore::new();

        let agg = SizedAggregationDataset::new(
            0x02_u16, &mut store, None,
            vec![
                FieldRule::Dimension(0), // City
                FieldRule::Dimension(1), // Country
                FieldRule::Measure(Box::new(IntegerSumAggregator::new(3))) // People
            ]
        ).unwrap();

        // Insert 10
        let i = Operation::Insert {
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        assert!(agg.aggregate(&mut store, i).is_ok());

        // Insert 10
        let i = Operation::Insert {
            new: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        assert!(agg.aggregate(&mut store, i).is_ok());


        let i = Operation::Update {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ]),
            new: Record::new(None, vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap(), vec![
            Operation::Update {
                old: Record::new(None, vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(20)
                ]),
                new: Record::new(None, vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ])
            },
            Operation::Insert {
                new: Record::new(None, vec![
                    Field::String("Brescia".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ])
            }
        ]);


        let i = Operation::Update {
            old: Record::new(None, vec![
                Field::String("Milan".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ]),
            new: Record::new(None, vec![
                Field::String("Brescia".to_string()),
                Field::String("Lombardy".to_string()),
                Field::String("Italy".to_string()),
                Field::Int(10)
            ])
        };
        let o = agg.aggregate(&mut store, i);
        assert_eq!(o.unwrap(), vec![
            Operation::Delete {
                old: Record::new(None, vec![
                    Field::String("Milan".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ])
            },
            Operation::Update {
                old: Record::new(None, vec![
                    Field::String("Brescia".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10)
                ]),
                new: Record::new(None, vec![
                    Field::String("Brescia".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(20)
                ])
            }
        ]);


    }


    #[test]
    fn perf_test() {

        fs::create_dir(".data").is_ok();

        let sm = LmdbStateStoreManager::new(".data".to_string(), 1024*1024*1024*10);
        let ss = sm.unwrap();
        let mut store = ss.init_state_store("test".to_string()).unwrap();

        let agg = SizedAggregationDataset::new(
            0x02_u16, store.as_mut(), None,
            vec![
                FieldRule::Dimension(0), // City
                FieldRule::Dimension(1), // Region
                FieldRule::Dimension(2), // Country
                FieldRule::Measure(Box::new(IntegerSumAggregator::new(3)))
            ]
        ).unwrap();


        for _i in 0..1000000 {

            let op = Operation::Insert {
                new: Record::new(None, vec![
                    Field::String(format!("Milan{}", 1).to_string()),
                    Field::String("Italy".to_string()),
                    Field::String("Lombardy".to_string()),
                    Field::Int(10),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20),
                    Field::Int(20)
                ])
            };

            assert!(agg.aggregate(store.as_mut(), op).is_ok());

        }
        fs::remove_dir_all(".data").is_ok();



    }



}
