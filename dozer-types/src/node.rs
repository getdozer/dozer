use serde::{self, Deserialize, Serialize};

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    str::from_utf8,
};
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
/// A identifier made of two `u64`s.
pub struct OpIdentifier {
    /// High 64 bits of the identifier.
    pub txid: u64,
    /// Low 64 bits of the identifier.
    pub seq_in_tx: u64,
}

impl OpIdentifier {
    pub fn new(txid: u64, seq_in_tx: u64) -> Self {
        Self { txid, seq_in_tx }
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        let mut result = [0_u8; 16];
        result[0..8].copy_from_slice(&self.txid.to_be_bytes());
        result[8..16].copy_from_slice(&self.seq_in_tx.to_be_bytes());
        result
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let txid = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let seq_in_tx = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        Self::new(txid, seq_in_tx)
    }
}

pub type SourceStates = HashMap<NodeHandle, OpIdentifier>;

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
