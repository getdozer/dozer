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
