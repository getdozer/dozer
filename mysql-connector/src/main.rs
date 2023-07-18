use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::errors::Error;
use mysql_cdc::ssl_mode::SslMode;
use serde::{Deserialize, Serialize};

// Define a DozerAction enum to represent the Dozer inserts, updates, and deletes.
#[derive(Debug, Serialize, Deserialize)]
enum DozerAction {
    Insert,
    Update,
    Delete,
}

fn main() -> Result<(), Error> {
    // Configure the BinlogOptions for replication.
    let gtid_set = "your_gtid_set_here";
    let options = BinlogOptions::from_mysql_gtid(gtid_set)?;

    // Customize your MySQL connection parameters
    let username = "debezium";
    let password = "dbz";
    let blocking = true;
    let ssl_mode = SslMode::Disabled;

    // Create the ReplicaOptions with BinlogOptions and MySQL credentials
    let replica_options = mysql_cdc::replica_options::ReplicaOptions {
        username: username.to_string(),
        password: password.to_string(),
        blocking,
        ssl_mode,
        binlog: options,
        ..Default::default()
    };

    // Create a new BinlogClient with the ReplicaOptions
    let mut client = BinlogClient::new(replica_options);

    for result in client.replicate()? {
        let (_header, event) = result?;

        // Process the MySQL binlog event and convert it to Dozer action.
        let dozer_action = match event {
            mysql_cdc::events::BinlogEvent::WriteRows(_)
            | mysql_cdc::events::BinlogEvent::UpdateRows(_)
            | mysql_cdc::events::BinlogEvent::DeleteRows(_) => DozerAction::Insert, // Replace with appropriate mapping for Update and Delete
            _ => continue, // Skip other events
        };

        // Send the Dozer action to the Dozer Kafka topic or any other processing logic.
        println!("Dozer Action: {:?}", dozer_action);
    }

    Ok(())
}
