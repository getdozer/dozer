use dozer_types::{
    chrono::{Duration, DurationRound},
    types::{Field, Record, Schema},
};

use super::errors::WindowError;

pub trait WindowFunction {
    fn execute(&self, record: &Record) -> Result<Vec<Record>, WindowError>;
    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, WindowError>;
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

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, WindowError> {
        Ok(schema.clone())
    }
}

struct HopWindow {
    column_index: usize,
    _hop_size: Duration,
    _interval: Duration,
}

impl HopWindow {
    pub fn _new(column_index: usize, _hop_size: Duration, _interval: Duration) -> Self {
        Self {
            column_index,
            _hop_size,
            _interval,
        }
    }

    fn hop(&self, field: &Field) -> Result<Vec<(Field, Field)>, WindowError> {
        if let Field::Timestamp(_ts) = field {
            let windows = vec![];
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

    fn get_output_schema(&self, schema: &Schema) -> Result<Schema, WindowError> {
        Ok(schema.clone())
    }
}
