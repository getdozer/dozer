use std::cell::RefCell;
use postgres::Error;
use std::sync::{Arc, Mutex};
use anyhow::Context;
use log::debug;
use postgres::Client;
use tokio::runtime::Runtime;
use crate::connectors::ingestor::Ingestor;
use crate::connectors::postgres::helper;
use crate::connectors::postgres::iterator::{Details, ReplicationState};
use crate::connectors::postgres::replicator::CDCHandler;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use tokio_postgres::SimpleQueryMessage;

pub struct PostgresIteratorHandler {
    pub details: Arc<Details>,
    pub lsn: RefCell<Option<String>>,
    pub state: RefCell<ReplicationState>,
    pub ingestor: Arc<Mutex<Ingestor>>
}

impl PostgresIteratorHandler {
    /*
     Replication involves 3 states
        1) Pending
        - Initialize a replication slot.
        - Initialize snapshots

        2) SnapshotInProgress
        - Sync initial snapshots of specified tables
        - Commit with lsn

        3) Replicating
        - Replicate CDC events using lsn
    */
    pub fn _start(&mut self) -> anyhow::Result<()> {
        let details = Arc::clone(&self.details);
        let conn_str = details.conn_str.to_owned();
        let client = Arc::new(RefCell::new(helper::connect(conn_str)?));

        // TODO: Handle cases:
        // - When snapshot replication is not completed
        // - When there is gap between available lsn (in case when slot dropped and new created) and last lsn
        // - When publication tables changes
        if let None = self.lsn.clone().into_inner() {
            debug!("\nCreating Slot....");
            if let Ok(true) = self.replication_exist(client.clone()) {
                // We dont have lsn, so we need to drop replication slot and start from scratch
                self.drop_replication_slot(client.clone()).context("Replication slot drop failed")?;
            }

            client
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")?;

            let replication_slot_lsn = self.create_replication_slot(client.clone())
                .context("Failed to create replication slot")?;

            self.lsn.replace(replication_slot_lsn);
            self.state.clone().replace(ReplicationState::SnapshotInProgress);

            /* #####################        SnapshotInProgress         ###################### */
            debug!("\nInitializing snapshots...");

            let snapshotter = PostgresSnapshotter {
                tables: details.tables.to_owned(),
                conn_str: details.conn_str_plain.to_owned(),
                ingestor: Arc::clone(&self.ingestor),
            };
            let tables = snapshotter.sync_tables()?;

            debug!("\nInitialized with tables: {:?}", tables);

            client.borrow_mut().simple_query("COMMIT;")?;
        }

        self.state.clone().replace(ReplicationState::Replicating);

        /*  ####################        Replicating         ######################  */
        self.replicate()?;

        Ok(())
    }

    fn drop_replication_slot(&self, client: Arc<RefCell<Client>>) -> anyhow::Result<()> {
        let slot = self.details.slot_name.clone();
        let res =
            client
                .borrow_mut()
                .simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref());
        match res {
            Ok(_) => debug!("dropped replication slot {}", slot),
            Err(_) => debug!("failed to drop replication slot..."),
        }
        Ok(())
    }

    fn create_replication_slot(&self, client: Arc<RefCell<Client>>) -> Result<Option<String>, Error> {
        let details = Arc::clone(&self.details);

        let create_replication_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            details.slot_name
        );

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&create_replication_slot_query)?;

        let lsn = if let SimpleQueryMessage::Row(row) = &slot_query_row[0] {
            row.get("consistent_point").unwrap()
        } else {
            panic!("unexpected query message");
        };

        Ok(Option::from(lsn.to_string()))
    }

    fn replication_exist(&self, client: Arc<RefCell<Client>>) -> anyhow::Result<bool> {
        let details = Arc::clone(&self.details);

        let replication_slot_info_query = format!(
            r#"SELECT * FROM pg_replication_slots where slot_name = '{}';"#,
            details.slot_name
        );

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&replication_slot_info_query)
            .context("fetch of replication slot info failed")?;

        if let SimpleQueryMessage::Row(_row) = &slot_query_row[0] {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn replicate(&self) -> Result<(), Error> {
        let rt = Runtime::new().unwrap();
        let ingestor = self.ingestor.clone();
        let lsn = self.lsn.borrow();
        let lsn = match lsn.as_ref() {
            Some(x) => x.to_string(),
            None => panic!("lsn not stored..."),
        };
        let publication_name = self.details.publication_name.clone();
        let slot_name = self.details.slot_name.clone();
        rt.block_on(async {
            let replicator = CDCHandler {
                conn_str: self.details.conn_str.clone(),
                ingestor,
                lsn,
                publication_name,
                slot_name,
            };
            replicator.start().await
        })
    }
}