use dozer_types::{
    chrono::Duration,
    types::{Record, Schema},
};

struct _TumbleWindow {
    column: u16,
    interval: Duration,
}

struct _HopWindow {
    column: u16,
    hop_size: Duration,
    interval: Duration,
}

trait WindowProcessor {
    fn apply(record: &Record) -> Vec<Record>;
    fn get_output_schema(schema: &Schema) -> Schema;
}
