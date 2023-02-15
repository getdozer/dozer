use dozer_types::{
    chrono::{Duration, DurationRound},
    types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
};

use super::errors::WindowError;

pub trait WindowFunction {
    fn execute(&self, record: &Record) -> Result<Vec<Record>, WindowError>;

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, WindowError> {
        let mut output_schema = schema.clone();
        output_schema.fields.push(FieldDefinition::new(
            String::from("window_start"),
            FieldType::Timestamp,
            false,
            SourceDefinition::Dynamic,
        ));
        output_schema.fields.push(FieldDefinition::new(
            String::from("window_end"),
            FieldType::Timestamp,
            false,
            SourceDefinition::Dynamic,
        ));
        Ok(output_schema)
    }
}

pub struct TumbleWindow {
    column_index: usize,
    interval: Duration,
}

impl TumbleWindow {
    pub fn new(column_index: usize, interval: Duration) -> Self {
        Self {
            column_index,
            interval,
        }
    }

    fn tumble(&self, field: &Field) -> Result<(Field, Field), WindowError> {
        if let Field::Timestamp(ts) = field {
            let start = ts
                .duration_trunc(self.interval)
                .map_err(WindowError::TumbleRoundingError)?;
            let end = start + self.interval;
            Ok((Field::Timestamp(start), Field::Timestamp(end)))
        } else {
            Err(WindowError::TumbleInvalidColumnType())
        }
    }
}

impl WindowFunction for TumbleWindow {
    fn execute(&self, record: &Record) -> Result<Vec<Record>, WindowError> {
        let field = record
            .get_value(self.column_index)
            .map_err(|_err| WindowError::TumbleInvalidColumnIndex())?;

        let (start, end) = self.tumble(field)?;

        let mut window_record = record.clone();
        window_record.push_value(start);
        window_record.push_value(end);

        Ok(vec![window_record])
    }
}

pub struct HopWindow {
    column_index: usize,
    hop_size: Duration,
    interval: Duration,
}

impl HopWindow {
    pub fn new(column_index: usize, hop_size: Duration, interval: Duration) -> Self {
        Self {
            column_index,
            hop_size,
            interval,
        }
    }

    fn hop(&self, field: &Field) -> Result<Vec<(Field, Field)>, WindowError> {
        if let Field::Timestamp(ts) = field {
            let starting_time = ts
                .duration_trunc(self.hop_size)
                .map_err(WindowError::TumbleRoundingError)?
                - self.interval;

            let mut windows = vec![];
            let mut current = starting_time;
            while current < starting_time + self.interval {
                let start = current;
                let end = current + self.interval;
                windows.push((Field::Timestamp(start), Field::Timestamp(end)));
                current += self.hop_size;
            }

            Ok(windows)
        } else {
            Err(WindowError::TumbleInvalidColumnType())
        }
    }
}

impl WindowFunction for HopWindow {
    fn execute(&self, record: &Record) -> Result<Vec<Record>, WindowError> {
        let field = record
            .get_value(self.column_index)
            .map_err(|_err| WindowError::TumbleInvalidColumnIndex())?;

        let windows = self.hop(field)?;

        let mut records = vec![];
        for (start, end) in windows.iter() {
            let mut window_record = record.clone();
            window_record.push_value(start.clone());
            window_record.push_value(end.clone());
            records.push(window_record);
        }

        Ok(records)
    }
}
