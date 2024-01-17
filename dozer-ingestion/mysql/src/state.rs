use crate::binlog::BinlogPosition;
use crate::MysqlStateError;
use dozer_ingestion_connector::dozer_types::node::RestartableState;

pub fn encode_state(pos: &BinlogPosition) -> RestartableState {
    let mut state = vec![];
    state.extend_from_slice(&pos.position.to_be_bytes());
    state.extend_from_slice(&pos.seq_no.to_be_bytes());
    state.extend(pos.clone().filename);

    state.into()
}

impl TryFrom<RestartableState> for BinlogPosition {
    type Error = MysqlStateError;

    fn try_from(state: RestartableState) -> Result<Self, Self::Error> {
        let position = u64::from_be_bytes(state.0[0..8].try_into()?);
        let seq_no = u64::from_be_bytes(state.0[8..16].try_into()?);
        let filename = state.0[16..].to_vec();

        Ok(BinlogPosition {
            position,
            seq_no,
            filename,
        })
    }
}
