use serde::{self, Deserialize, Serialize};

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    str::from_utf8,
};
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct NodeHandle {
    pub ns: Option<u16>,
    pub id: String,
}

impl NodeHandle {
    pub fn new(ns: Option<u16>, id: String) -> Self {
        Self { ns, id }
    }
}

impl NodeHandle {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut r = Vec::<u8>::with_capacity(5);
        match self.ns {
            Some(ns) => {
                r.push(1_u8);
                r.extend(ns.to_le_bytes());
            }
            None => r.push(0_u8),
        }
        let id_buf = self.id.as_bytes();
        r.extend((id_buf.len() as u16).to_le_bytes());
        r.extend(id_buf);
        r
    }

    pub fn from_bytes(buffer: &[u8]) -> NodeHandle {
        match buffer[0] {
            1_u8 => {
                let ns = u16::from_le_bytes(buffer[1..3].try_into().unwrap());
                let id_len: u16 = u16::from_le_bytes(buffer[3..5].try_into().unwrap());
                let id = from_utf8(&buffer[5..5 + id_len as usize]).unwrap();
                NodeHandle::new(Some(ns), id.to_string())
            }
            _ => {
                let id_len: u16 = u16::from_le_bytes(buffer[1..3].try_into().unwrap());
                let id = from_utf8(&buffer[3..3 + id_len as usize]).unwrap();
                NodeHandle::new(None, id.to_string())
            }
        }
    }
}

impl Display for NodeHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ns_str = match self.ns {
            Some(ns) => ns.to_string(),
            None => "r".to_string(),
        };
        f.write_str(&format!("{}_{}", ns_str, self.id))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, bincode::Encode, bincode::Decode,
)]
/// A table's restartable state, any binary data.
pub struct RestartableState(pub Vec<u8>);

impl From<Vec<u8>> for RestartableState {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, bincode::Encode, bincode::Decode,
)]
/// A source's ingestion state.
pub enum SourceState {
    /// This source hasn't been ingested.
    NotStarted,
    /// This source has some data ingested, and it can't be restarted.
    NonRestartable,
    /// This source has some data ingested, and it can be restarted if it's given the state.
    Restartable(RestartableState),
}

/// Map from a `Source` node's handle to its state.
///
/// This uniquely identifies the state of the Dozer pipeline.
/// We generate this map on every commit, and it's:
///
/// - Written to `Log` so consumers of log know where the pipeline is when pipeline restarts, and can rollback if some events were not persisted to checkpoints.
/// - Written to checkpoints so when pipeline is restarted, we know where to tell the source to start from.
pub type SourceStates = HashMap<NodeHandle, SourceState>;

#[test]
fn test_handle_to_from_bytes() {
    let original = NodeHandle::new(Some(10), 100.to_string());
    let sz = original.to_bytes();
    let _decoded = NodeHandle::from_bytes(sz.as_slice());

    let original = NodeHandle::new(None, 100.to_string());
    let sz = original.to_bytes();
    let decoded = NodeHandle::from_bytes(sz.as_slice());

    assert_eq!(original, decoded)
}
