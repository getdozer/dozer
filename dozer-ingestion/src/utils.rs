#[macro_export]
macro_rules! retry_on_network_failure {
    ($description:expr, $operation:expr, $network_error_predicate:expr $(, $reconnect:expr)?) => {
        loop {
            match $operation {
                ok @ Ok(_) => break ok,
                Err(err) => {
                    if ($network_error_predicate)(&err) {
                        dozer_types::log::error!(
                            "network error during {}: {err:?}. retrying...",
                            $description
                        );
                        $($reconnect)?
                    } else {
                        break Err(err);
                    }
                }
            }
        }
    };
}
