use crate::connector::REPLICATION_SLOT_PREFIX;
use crate::PostgresStateError;
use dozer_ingestion_connector::dozer_types::node::RestartableState;
use postgres_protocol::Lsn;
use postgres_types::PgLsn;

#[derive(Clone)]
pub struct LsnWithSlot {
    pub lsn: (PgLsn, u64),
    pub slot_name: String,
}

pub fn encode_state(lsn: Lsn, seq_no: u64, slot_name: String) -> RestartableState {
    let slot_name_suffix = slot_name.replace(REPLICATION_SLOT_PREFIX, "");
    let mut state = vec![];
    state.extend_from_slice(&lsn.to_be_bytes());
    state.extend_from_slice(&seq_no.to_be_bytes());
    state.extend_from_slice(slot_name_suffix.as_bytes());
    state.into()
}

impl TryFrom<RestartableState> for LsnWithSlot {
    type Error = PostgresStateError;

    fn try_from(state: RestartableState) -> Result<Self, Self::Error> {
        let lsn = Lsn::from_be_bytes(state.0[0..8].try_into()?);
        let seq_no = u64::from_be_bytes(state.0[8..16].try_into()?);
        let slot_name = String::from_utf8(state.0[16..].into())?;

        Ok(LsnWithSlot {
            lsn: (PgLsn::from(lsn), seq_no),
            slot_name: format!("{REPLICATION_SLOT_PREFIX}{slot_name}"),
        })
    }
}
