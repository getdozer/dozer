use dozer_types::{
    chrono::{Duration, DurationRound},
    types::{Field, FieldDefinition, FieldType, ProcessorRecord, Schema, SourceDefinition},
};

use crate::pipeline::errors::WindowError;

#[derive(Clone, Debug)]
pub enum WindowType {
    Tumble {
        column_index: usize,
        interval: Duration,
    },
    Hop {
        column_index: usize,
        hop_size: Duration,
        interval: Duration,
    },
}

impl WindowType {
    pub fn execute(&self, record: &ProcessorRecord) -> Result<Vec<ProcessorRecord>, WindowError> {
        match self {
            WindowType::Tumble {
                column_index,
                interval,
            } => execute_tumble_window(record, *column_index, *interval),
            WindowType::Hop {
                column_index,
                hop_size,
                interval,
            } => execute_hop_window(record, *column_index, *hop_size, *interval),
        }
    }

    pub fn get_output_schema(&self, schema: &Schema) -> Result<Schema, WindowError> {
        let mut output_schema = schema.clone();
        output_schema.fields.push(FieldDefinition::new(
            String::from("window_start"),
            FieldType::Timestamp,
            false,
            SourceDefinition::Dynamic,
        ));
        output_schema
            .primary_index
            .push(output_schema.fields.len() - 1);

        output_schema.fields.push(FieldDefinition::new(
            String::from("window_end"),
            FieldType::Timestamp,
            false,
            SourceDefinition::Dynamic,
        ));

        Ok(output_schema)
    }
}

fn execute_hop_window(
    record: &ProcessorRecord,
    column_index: usize,
    hop_size: Duration,
    interval: Duration,
) -> Result<Vec<ProcessorRecord>, WindowError> {
    let field = record.get_field_by_index(column_index);

    let windows = hop(field, hop_size, interval)?;

    let mut records = vec![];
    for (start, end) in windows.iter() {
        let mut window_record = record.clone();

        window_record.extend_direct_field(start.clone());
        window_record.extend_direct_field(end.clone());
        records.push(window_record);
    }

    Ok(records)
}

fn hop(
    field: &Field,
    hop_size: Duration,
    interval: Duration,
) -> Result<Vec<(Field, Field)>, WindowError> {
    if let Field::Timestamp(ts) = field {
        let starting_time = ts
            .duration_trunc(hop_size)
            .map_err(WindowError::TumbleRoundingError)?
            - interval
            + hop_size;

        let mut windows = vec![];
        let mut current = starting_time;
        while current < starting_time + interval {
            let start = current;
            let end = current + interval;
            windows.push((Field::Timestamp(start), Field::Timestamp(end)));
            current += hop_size;
        }

        Ok(windows)
    } else {
        Err(WindowError::TumbleInvalidColumnType())
    }
}

fn execute_tumble_window(
    record: &ProcessorRecord,
    column_index: usize,
    interval: Duration,
) -> Result<Vec<ProcessorRecord>, WindowError> {
    let field = record.get_field_by_index(column_index);

    let (start, end) = tumble(field, interval)?;

    let mut window_record = record.clone();
    window_record.extend_direct_field(start.clone());
    window_record.extend_direct_field(end.clone());

    Ok(vec![window_record])
}

fn tumble(field: &Field, interval: Duration) -> Result<(Field, Field), WindowError> {
    if let Field::Timestamp(ts) = field {
        let start = ts
            .duration_trunc(interval)
            .map_err(WindowError::TumbleRoundingError)?;
        let end = start + interval;
        Ok((Field::Timestamp(start), Field::Timestamp(end)))
    } else {
        Err(WindowError::TumbleInvalidColumnType())
    }
}
