use dozer_core::dag::{errors::ExecutionError, node::PortHandle};
use dozer_types::types::Record;
use lmdb::Database;

pub enum JoinOperatorType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

pub struct JoinConstraint {
    pub left_key_index: usize,
    pub right_key_index: usize,
}

trait RecordProducer {
    fn produce(&self, record: &Record) -> Result<Vec<Record>, ExecutionError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SourceChannel {
    Left,
    Right,
}

pub enum JoinSource {
    Table(PortHandle, JoinOperator),
    Join(JoinOperator, JoinOperator),
}

impl JoinSource {
    fn lookup(&self, key: &[&[u8]]) -> Result<Vec<Record>, ExecutionError> {
        match self {
            JoinSource::Table(from_port, to_operator) => {
                // get records from record reader
            }
            JoinSource::Join(from_operator, to_operator) => {
                // forward the lookup to the next join operator
                from_operator.lookup(key);
            }
        }
    }
}

pub struct JoinOperator {
    operator: JoinOperatorType,

    constraints: Vec<JoinConstraint>,

    left_source: Box<JoinSource>,
    right_source: Box<JoinSource>,

    // Lookup indexes
    pub left_join_index: Option<Database>,
    pub right_join_index: Option<Database>,
}

impl JoinOperator {
    fn insert(&self, from_source: SourceChannel, record: &Record) {
        if from_source == SourceChannel::Left {
            // update the left join index
            // lookup the right join source
            self.right_source.lookup(lookup_keys);
            // join the records
        } else {
            // update the right join index
            // lookup the left join source
            // join the records
        }
        todo!()
    }

    fn lookup(&self, key: &[&[u8]]) -> Result<Vec<Record>, ExecutionError> {
        self.left_source.lookup(key);
        self.right_source.lookup(key);
    }
}
