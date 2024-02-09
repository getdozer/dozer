use dozer_ingestion_connector::dozer_types::node::OpIdentifier;

use crate::binlog::BinlogPosition;
use crate::MysqlStateError;

pub fn encode_state(pos: &BinlogPosition) -> OpIdentifier {
    let lsn = (pos.binlog_id << 32) | pos.position;

    OpIdentifier {
        txid: lsn,
        seq_in_tx: 0,
    }
}

impl TryFrom<OpIdentifier> for BinlogPosition {
    type Error = MysqlStateError;

    fn try_from(state: OpIdentifier) -> Result<Self, Self::Error> {
        let binlog_id = state.txid >> 32;
        let position = state.txid & 0x00000000ffffffff;

        Ok(BinlogPosition {
            binlog_id,
            position,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_decode_encode() {
        use super::*;
        let pos = BinlogPosition {
            binlog_id: 123,
            position: 456,
        };

        let state = encode_state(&pos);
        let pos2 = BinlogPosition::try_from(state).unwrap();

        assert_eq!(pos, pos2);
    }
}
