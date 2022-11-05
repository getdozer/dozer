// use dozer_types::errors::database::DatabaseError;
// use dozer_types::errors::database::DatabaseError::{
//     DeserializationError, InvalidKey, SerializationError,
// };
// use std::mem;
//
// pub struct SecondaryIndexKey<'a> {
//     index_id: u16,
//     value: &'a [u8],
// }
//
// impl<'a> SecondaryIndexKey<'a> {
//     pub fn new(index_id: u16, value: &'a [u8]) -> Self {
//         Self { index_id, value }
//     }
// }
//
// pub struct StateStoreIndexer {
//     dataset_id: u16,
//     counter_key: [u8; 4],
// }
//
// const COUNTER_KEY: u16 = 0_u16;
// const PRIMARY_IDX_KEY: u16 = 1_u16;
// const SECONDARY_IDX_KEY_LIST_KEY: u16 = 2_u16;
//
// impl StateStoreIndexer {
//     pub fn new(dataset_id: u16) -> Self {
//         let mut counter_key: [u8; 4] = [0; 4];
//         counter_key[..2].copy_from_slice(&dataset_id.to_le_bytes());
//         counter_key[2..].copy_from_slice(&COUNTER_KEY.to_le_bytes());
//
//         Self {
//             dataset_id,
//             counter_key,
//         }
//     }
//
//     fn create_key(&self, index_id: u16, data: &[u8]) -> Vec<u8> {
//         let mut full_key_vec = Vec::with_capacity(
//             data.len() + mem::size_of_val(&index_id) + mem::size_of_val(&self.dataset_id),
//         );
//         full_key_vec.extend_from_slice(&self.dataset_id.to_le_bytes());
//         full_key_vec.extend_from_slice(&index_id.to_le_bytes());
//         full_key_vec.extend(data);
//         full_key_vec
//     }
//
//     fn get_autogen_counter(&self, store: &mut dyn StateStore) -> Result<u64, DatabaseError> {
//         let curr_counter = match store.get(&self.counter_key)? {
//             Some(c) => u64::from_le_bytes(c.try_into().map_err(|e| DeserializationError {
//                 typ: "u64".to_string(),
//                 reason: Box::new(e),
//             })?),
//             _ => 1_u64,
//         };
//         store.put(&self.counter_key, &(curr_counter + 1).to_le_bytes())?;
//         Ok(curr_counter)
//     }
//
//     pub fn del(&mut self, store: &mut dyn StateStore, key: &[u8]) -> Result<(), DatabaseError> {
//         let sec_key_list_key = self.create_key(SECONDARY_IDX_KEY_LIST_KEY, key);
//         let full_pk = self.create_key(PRIMARY_IDX_KEY, key);
//
//         if let Some(buf) = store.get(sec_key_list_key.as_slice())? {
//             let sec_keys: Vec<Vec<u8>> =
//                 bincode::deserialize(buf).map_err(|e| DeserializationError {
//                     typ: "Vec<Vec<u8>>".to_string(),
//                     reason: Box::new(e),
//                 })?;
//             for key in &sec_keys {
//                 store.del(key, Some(full_pk.as_slice()))?;
//             }
//         }
//         store.del(full_pk.as_slice(), None)?;
//         Ok(())
//     }
//
//     pub fn put(
//         &mut self,
//         store: &mut dyn StateStore,
//         key: Option<&[u8]>,
//         value: &[u8],
//         indexes: Vec<SecondaryIndexKey>,
//     ) -> Result<Vec<u8>, DatabaseError> {
//         let pk_val = match key {
//             Some(v) => {
//                 self.del(store, v)?;
//                 Vec::from(v)
//             }
//             _ => {
//                 let ctr = self.get_autogen_counter(store)?.to_le_bytes();
//                 Vec::from(ctr.as_slice())
//             }
//         };
//
//         let full_pk = self.create_key(PRIMARY_IDX_KEY, pk_val.as_slice());
//         store.put(full_pk.as_slice(), value)?;
//
//         let full_secondary_keys: Vec<Vec<u8>> = indexes
//             .iter()
//             .map(|e| self.create_key(e.index_id, e.value))
//             .collect();
//
//         for fk in &full_secondary_keys {
//             store.put(fk.as_slice(), full_pk.as_slice())?;
//         }
//
//         let sec_key_list_key = self.create_key(SECONDARY_IDX_KEY_LIST_KEY, pk_val.as_slice());
//         let sec_key_list_val =
//             bincode::serialize(&full_secondary_keys).map_err(|e| SerializationError {
//                 typ: "Vec<Vec<u8>>".to_string(),
//                 reason: Box::new(e),
//             })?;
//         store.put(sec_key_list_key.as_slice(), sec_key_list_val.as_slice())?;
//         Ok(full_pk)
//     }
//
//     pub fn get<'a>(
//         &self,
//         store: &'a dyn StateStore,
//         pk_val: &[u8],
//     ) -> Result<Option<&'a [u8]>, DatabaseError> {
//         let full_pk = self.create_key(PRIMARY_IDX_KEY, pk_val);
//         store.get(full_pk.as_slice())
//     }
//
//     pub fn get_multi<'a>(
//         &self,
//         state: &'a mut dyn StateStore,
//         index_id: u16,
//         key: &[u8],
//     ) -> Result<Vec<&'a [u8]>, DatabaseError> {
//         let full_key = self.create_key(index_id, key);
//         let mut records = Vec::<&[u8]>::new();
//         let mut cursor = state.cursor()?;
//         if cursor.seek(full_key.as_slice())? {
//             loop {
//                 let kv = cursor.read()?;
//                 match kv {
//                     Some(t) => {
//                         if t.0 != full_key {
//                             break;
//                         }
//                         match state.get(t.1)? {
//                             Some(v) => records.push(v),
//                             _ => return Err(InvalidKey(format!("{:x?}", t.1))),
//                         }
//                     }
//                     _ => break,
//                 }
//                 if !cursor.next()? {
//                     break;
//                 }
//             }
//         }
//         Ok(records)
//     }
// }
