use crate::pipeline::expression::execution::Expression;
use anyhow::{anyhow, Context};
use dozer_core::state::StateStore;
use dozer_types::types::Record;
use std::sync::Arc;

pub enum PrimaryIndexDefinition {
    AutoIncrement,
    FieldsBased(Vec<usize>),
}

pub struct SecondaryIndexDefinition {
    id: u16,
    exp: Expression,
}

pub struct ExpressionIndexedStore {
    dataset_id: u16,
    store: Arc<dyn StateStore>,
    primary_idx: PrimaryIndexDefinition,
    secondary_indexes: Vec<SecondaryIndexDefinition>,
    counter_key: [u8; 4],
}

impl ExpressionIndexedStore {
    pub fn new(
        dataset_id: u16,
        store: Arc<dyn StateStore>,
        primary_idx: PrimaryIndexDefinition,
        secondary_indexes: Vec<SecondaryIndexDefinition>,
    ) -> Self {
        let mut counter_key: [u8; 4] = [0; 4];
        counter_key[..2].copy_from_slice(&dataset_id.to_le_bytes());
        counter_key[2..].copy_from_slice(&0_u16.to_le_bytes());

        Self {
            dataset_id,
            store,
            primary_idx,
            secondary_indexes,
            counter_key,
        }
    }

    fn compose_key(index_id: u16, value: &[u8]) -> &[u8] {}

    pub fn put(&mut self, r: Record) -> anyhow::Result<()> {
        let primary_key = match &self.primary_idx {
            PrimaryIndexDefinition::AutoIncrement => {
                let curr_counter = match self.store.get(&self.counter_key)? {
                    Some(c) => {
                        u64::from_le_bytes(c.try_into().context("Unable to convert counter value")?)
                    }
                    _ => 0_u64,
                } + 1;
                self.store
                    .put(&self.counter_key, &curr_counter.to_le_bytes())?;
                curr_counter.to_le_bytes()
            }
            PrimaryIndexDefinition::FieldsBased(indexes) => r.get_key(indexes)?,
        };

        self.store
            .put(&primary_key, bincode::serialize(&r)?.as_slice())?;

        for idx in self.secondary_indexes {}

        Ok()
    }
}
