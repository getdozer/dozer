use anyhow::Ok;
use dozer_types::types::{Field, IndexDefinition, IndexType, Record, Schema, SchemaIdentifier};
use lmdb::{Database, RwTransaction, Transaction, WriteFlags};

pub struct Indexer<'a> {
    db: &'a Database,
}
impl<'a> Indexer<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    pub fn build_indexes(
        &self,
        parent_txn: &'a mut RwTransaction,
        rec: Record,
        schema: Schema,
        pkey: Vec<u8>,
    ) -> anyhow::Result<()> {
        let mut txn = parent_txn.begin_nested_txn()?;

        let identifier = &schema.identifier.unwrap();
        for index in schema.secondary_indexes.iter() {
            self._build_index(&mut txn, index, &rec, &identifier, &pkey)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn _build_index(
        &self,
        txn: &mut RwTransaction,
        index: &IndexDefinition,
        rec: &Record,
        identifier: &SchemaIdentifier,
        pkey: &Vec<u8>,
    ) -> anyhow::Result<()> {
        let keys = match index.typ {
            dozer_types::types::IndexType::SortedInverted => {
                self._build_index_sorted_inverted(&index.fields, &rec.values)
            }
            dozer_types::types::IndexType::HashInverted => todo!(),
        };

        for key in keys.iter() {
            let typ = &index.typ;
            let secondary_key = get_secondary_index(identifier, typ, key);
            txn.put::<Vec<u8>, Vec<u8>>(*self.db, &secondary_key, &pkey, WriteFlags::default())?;
        }
        Ok(())
    }

    fn _build_index_sorted_inverted(
        &self,
        index_fields: &Vec<usize>,
        values: &Vec<Field>,
    ) -> Vec<Vec<u8>> {
        let key: Vec<Vec<u8>> = index_fields
            .iter()
            .map(|idx| {
                let field = values[*idx].clone();
                let encoded: Vec<u8> = bincode::serialize(&field).unwrap();
                encoded
            })
            .collect();
        vec![key.join("#".as_bytes())]
    }

    fn _build_index_hash_inverted(
        &self,
        index_fields: &Vec<usize>,
        values: &Vec<Field>,
    ) -> Vec<Vec<u8>> {
        self._build_index_sorted_inverted(index_fields, values)
    }
}

pub fn get_secondary_index(
    identifier: &SchemaIdentifier,
    typ: &IndexType,
    key: &Vec<u8>,
) -> Vec<u8> {
    let key = key.clone();
    [
        bincode::serialize(&identifier).unwrap(),
        bincode::serialize(&typ).unwrap(),
        key,
    ]
    .join("#".as_bytes())
}
