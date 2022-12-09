use dozer_tracing::init_metrics;
use metrics::Unit;
use metrics::{
    absolute_counter, counter, decrement_gauge, describe_counter, describe_gauge,
    describe_histogram, gauge, histogram, increment_counter, increment_gauge, register_counter,
    register_gauge, register_histogram,
};

fn main() {
    let server_name = "web03".to_string();

    init_metrics();

    let common_labels = &[("listener", "frontend")];

    // Go through describing the metrics:
    describe_counter!("requests_processed", "number of requests processed");
    describe_counter!("bytes_sent", Unit::Bytes, "total number of bytes sent");
    describe_gauge!("connection_count", "current number of client connections");
    describe_histogram!(
        "svc.execution_time",
        Unit::Milliseconds,
        "execution time of request handler"
    );
    describe_gauge!("unused_gauge", "some gauge we'll never use in this program");
    describe_histogram!(
        "unused_histogram",
        Unit::Seconds,
        "some histogram we'll also never use in this program"
    );

    // And registering them:
    let counter1 = register_counter!("test_counter");
    counter1.increment(1);
    let counter2 = register_counter!("test_counter", "type" => "absolute");
    counter2.absolute(42);

    let gauge1 = register_gauge!("test_gauge");
    gauge1.increment(1.0);
    let gauge2 = register_gauge!("test_gauge", "type" => "decrement");
    gauge2.decrement(1.0);
    let gauge3 = register_gauge!("test_gauge", "type" => "set");
    gauge3.set(3.1459);

    let histogram1 = register_histogram!("test_histogram");
    histogram1.record(0.57721);

    // All the supported permutations of `counter!` and its increment/absolute versions:
    counter!("bytes_sent", 64);
    counter!("bytes_sent", 64, "listener" => "frontend");
    counter!("bytes_sent", 64, "listener" => "frontend", "server" => server_name.clone());
    counter!("bytes_sent", 64, common_labels);

    increment_counter!("requests_processed");
    increment_counter!("requests_processed", "request_type" => "admin");
    increment_counter!("requests_processed", "request_type" => "admin", "server" => server_name.clone());
    increment_counter!("requests_processed", common_labels);

    absolute_counter!("bytes_sent", 64);
    absolute_counter!("bytes_sent", 64, "listener" => "frontend");
    absolute_counter!("bytes_sent", 64, "listener" => "frontend", "server" => server_name.clone());
    absolute_counter!("bytes_sent", 64, common_labels);

    // All the supported permutations of `gauge!` and its increment/decrement versions:
    gauge!("connection_count", 300.0);
    gauge!("connection_count", 300.0, "listener" => "frontend");
    gauge!("connection_count", 300.0, "listener" => "frontend", "server" => server_name.clone());
    gauge!("connection_count", 300.0, common_labels);
    increment_gauge!("connection_count", 300.0);
    increment_gauge!("connection_count", 300.0, "listener" => "frontend");
    increment_gauge!("connection_count", 300.0, "listener" => "frontend", "server" => server_name.clone());
    increment_gauge!("connection_count", 300.0, common_labels);
    decrement_gauge!("connection_count", 300.0);
    decrement_gauge!("connection_count", 300.0, "listener" => "frontend");
    decrement_gauge!("connection_count", 300.0, "listener" => "frontend", "server" => server_name.clone());
    decrement_gauge!("connection_count", 300.0, common_labels);

    // All the supported permutations of `histogram!`:
    histogram!("svc.execution_time", 70.0);
    histogram!("svc.execution_time", 70.0, "type" => "users");
    histogram!("svc.execution_time", 70.0, "type" => "users", "server" => server_name.clone());
    histogram!("svc.execution_time", 70.0, common_labels);
}
