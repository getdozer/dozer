#[macro_export]
macro_rules! retry_on_network_failure {
    ($description:expr, $operation:expr, $network_error_predicate:expr $(, $reconnect:expr)?) => {
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
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        $($reconnect)?
                    } else {
                        break Err(err);
                    }
                }
            }
        }
    };
}
