use dozer_types::thiserror::Error;

#[derive(Debug, Clone)]
pub struct ListOrFilterColumns {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Error)]
#[error("table not found: {}", table_name(schema.as_deref(), name))]
pub struct TableNotFound {
    pub schema: Option<String>,
    pub name: String,
}

fn table_name(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", schema, name),
        None => name.to_string(),
    }
}

pub fn warn_dropped_primary_index(table_name: &str) {
    dozer_types::log::warn!(
        "One or more primary index columns from the source table are \
                    not part of the defined schema for table: '{0}'. \
                    The primary index will therefore not be present in the Dozer table",
        table_name
    );
}

#[macro_export]
macro_rules! retry_on_network_failure {
    ($description:expr, $operation:expr, $network_error_predicate:expr $(, $reconnect:expr)? $(,)?) =>
        {
            $crate::retry_on_network_failure_impl!(
                $description,
                $operation,
                $network_error_predicate,
                tokio::time::sleep(RETRY_INTERVAL).await
                $(, $reconnect)?
            )
        }
}

#[macro_export]
macro_rules! blocking_retry_on_network_failure {
    ($description:expr, $operation:expr, $network_error_predicate:expr $(, $reconnect:expr)? $(,)?) =>
        {
            $crate::retry_on_network_failure_impl!(
                $description,
                $operation,
                $network_error_predicate,
                std::thread::sleep(RETRY_INTERVAL)
                $(, $reconnect)?
            )
        }
}

#[macro_export]
macro_rules! retry_on_network_failure_impl {
    ($description:expr, $operation:expr, $network_error_predicate:expr, $sleep:expr $(, $reconnect:expr)? $(,)?) => {
        loop {
            match $operation {
                ok @ Ok(_) => break ok,
                Err(err) => {
                    if ($network_error_predicate)(&err) {
                        const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
                        dozer_types::log::error!(
                            "network error during {}: {err:?}. retrying in {RETRY_INTERVAL:?}...",
                            $description
                        );
                        $sleep;
                        $($reconnect)?
                    } else {
                        break Err(err);
                    }
                }
            }
        }
    };
}
