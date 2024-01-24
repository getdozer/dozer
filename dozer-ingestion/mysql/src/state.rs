use dozer_ingestion_connector::dozer_types::node::OpIdentifier;

use crate::binlog::BinlogPosition;
use crate::MysqlStateError;

pub fn encode_state(pos: &BinlogPosition) -> OpIdentifier {
    todo!()
}

impl TryFrom<OpIdentifier> for BinlogPosition {
    type Error = MysqlStateError;

    fn try_from(state: OpIdentifier) -> Result<Self, Self::Error> {
        todo!()
    }
}
