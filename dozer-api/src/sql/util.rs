#[derive(Debug)]
pub enum Iso8601Duration {
    Duration(chrono::Duration),
    DaysMilliseconds(i32, i32),
    MonthsDaysNanoseconds(i32, i32, i64),
    Months(i32),
}

impl std::fmt::Display for Iso8601Duration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Iso8601Duration::Duration(d) => std::fmt::Display::fmt(&d, f),
            Iso8601Duration::DaysMilliseconds(days, msecs) => {
                let secs = msecs.div_euclid(1_000);
                let msecs = msecs.rem_euclid(1_000);
                write!(f, "P{}DT{}.{:03}S", days, secs, msecs)
            }
            Iso8601Duration::MonthsDaysNanoseconds(months, days, nanos) => {
                let secs = nanos.div_euclid(1_000_000_000);
                let nanos = nanos.rem_euclid(1_000_000_000);
                write!(f, "P{}M{}DT{}.{:09}S", months, days, secs, nanos)
            }
            Iso8601Duration::Months(months) => {
                write!(f, "P{}M", months)
            }
        }
    }
}
