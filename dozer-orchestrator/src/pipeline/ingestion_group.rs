use crate::ConnectionService;
use crossbeam::channel::{unbounded, Receiver};
use dozer_ingestion::connectors::connector::TableInfo;
use dozer_ingestion::connectors::seq_no_resolver::SeqNoResolver;
use dozer_ingestion::connectors::storage::{RocksConfig, Storage};
use dozer_types::errors::orchestrator::OrchestrationError;
use dozer_types::models::connection::Connection;
use dozer_types::types::{Operation, OperationEvent};
use std::sync::{Arc, Mutex};
use std::thread::spawn;

pub trait IterationForwarder: Send + Sync {
    fn forward(&self, event: OperationEvent, schema_id: u16) -> Result<(), OrchestrationError>;
}

pub struct ChannelForwarder {
    pub sender: crossbeam::channel::Sender<(OperationEvent, u16)>,
}

impl IterationForwarder for ChannelForwarder {
    fn forward(&self, event: OperationEvent, schema_id: u16) -> Result<(), OrchestrationError> {
        let send_res = self.sender.send((event, schema_id));
        match send_res {
            Ok(_) => Ok(()),
            Err(_) => Err(OrchestrationError::IngestionForwarderError),
        }
    }
}

pub struct IngestionGroup {}

impl IngestionGroup {
    pub fn run_ingestion(
        &self,
        connections: Vec<Connection>,
        table_names: Vec<String>,
    ) -> Receiver<(OperationEvent, u16)> {
        let storage_config = RocksConfig::default();
        let storage_client = Arc::new(Storage::new(storage_config));

        let (sender, receiver) = unbounded::<(OperationEvent, u16)>();
        let forwarder: Arc<Box<dyn IterationForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender }));

        let table_names_ref = Arc::new(table_names);
        let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
        seq_resolver.init();
        let seq_no_resolver_ref = Arc::new(Mutex::new(seq_resolver));

        for connection in connections {
            let client = Arc::clone(&storage_client);
            let fw = Arc::clone(&forwarder);
            let t_names = Arc::clone(&table_names_ref);
            let sec_no_resolver = Arc::clone(&seq_no_resolver_ref);
            spawn(move || -> Result<(), OrchestrationError> {
                let mut connector = ConnectionService::get_connector(connection.to_owned());

                let tables = connector.get_tables().unwrap();
                let tables: Vec<TableInfo> = tables
                    .iter()
                    .filter(|t| {
                        let v = t_names.iter().find(|n| (*n).clone() == t.name.clone());
                        v.is_some()
                    })
                    .cloned()
                    .collect();

                connector
                    .initialize(client, Some(tables))
                    .map_err(OrchestrationError::ConnectorError);

                let mut iterator = connector.iterator(sec_no_resolver);
                loop {
                    let msg = iterator.next().unwrap();
                    let schema_id = match msg.operation.clone() {
                        Operation::Delete { old } => old.schema_id,
                        Operation::Insert { new } => new.schema_id,
                        Operation::Update { old: _, new } => new.schema_id,
                    }
                    .unwrap();

                    fw.forward(msg, schema_id.id as u16)?;
                }
            });
        }

        receiver
    }
}
