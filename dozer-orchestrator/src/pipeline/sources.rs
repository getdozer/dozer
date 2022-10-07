use std::collections::HashMap;
use std::sync::Arc;

use anyhow::bail;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::{ChannelManager, SourceChannelForwarder};
use dozer_ingestion::connectors::connector::TableInfo;

use crate::services::connection::ConnectionService;
use dozer_core::dag::node::{Source, SourceFactory};
use dozer_core::state::StateStore;
use dozer_ingestion::connectors::storage::{RocksConfig, Storage};
use dozer_types::models::connection::Connection;
use dozer_types::types::{Operation, Schema};

pub struct ConnectorSourceFactory {
    connections: Vec<Connection>,
    table_names: Vec<String>,
    port_map: HashMap<u16, Schema>,
    pub table_map: HashMap<String, u16>,
}

impl ConnectorSourceFactory {
    pub fn new(
        connections: Vec<Connection>,
        table_names: Vec<String>,
        source_schemas: Vec<Schema>,
    ) -> Self {
        let (port_map, table_map) = Self::_get_maps(&source_schemas, &table_names).unwrap();
        Self {
            connections,
            port_map,
            table_map,
            table_names,
        }
    }

    fn _get_maps(
        source_schemas: &Vec<Schema>,
        table_names: &Vec<String>,
    ) -> anyhow::Result<(HashMap<u16, Schema>, HashMap<String, u16>)> {
        let mut port_map: HashMap<u16, Schema> = HashMap::new();
        let mut table_map: HashMap<String, u16> = HashMap::new();
        let mut idx = 0;
        source_schemas.iter().for_each(|schema| {
            let id = schema.identifier.clone().unwrap().id;
            let table_name = &table_names[idx];
            port_map.insert(id as u16, schema.clone());
            table_map.insert(table_name.to_owned(), id as u16);
            idx += 1;
        });

        Ok((port_map, table_map))
    }
}

impl SourceFactory for ConnectorSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        let keys = self.port_map.to_owned().into_keys().collect();
        println!("{:?}", keys);
        keys
    }

    fn build(&self) -> Box<dyn Source> {
        Box::new(ConnectorSource {
            connections: self.connections.to_owned(),
            table_names: self.table_names.to_owned(),
            port_map: self.port_map.to_owned(),
            table_map: self.table_map.to_owned(),
        })
    }
}

pub struct ConnectorSource {
    port_map: HashMap<u16, Schema>,
    table_map: HashMap<String, u16>,
    connections: Vec<Connection>,
    table_names: Vec<String>,
}

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        _cm: &dyn ChannelManager,
        _state: &mut dyn StateStore,
        _from_seq: Option<u64>,
    ) -> anyhow::Result<()> {
        let connection = &self.connections[0];

        let mut connector = ConnectionService::get_connector(connection.to_owned());

        // Filter for the tables selected.
        let tables = connector.get_tables()?;
        let tables: Vec<TableInfo> = tables
            .iter()
            .filter(|t| {
                let v = self
                    .table_names
                    .iter()
                    .find(|n| *n.clone() == t.name.clone());
                v.is_some()
            })
            .map(|t| t.clone())
            .collect();

        let storage_config = RocksConfig::default();
        let storage_client = Arc::new(Storage::new(storage_config));
        connector.initialize(storage_client, Some(tables)).unwrap();

        let mut iterator = connector.iterator();
        loop {
            let msg = iterator.next().unwrap();
            let schema_id = match msg.operation.clone() {
                Operation::Delete { old } => old.schema_id,
                Operation::Insert { new } => new.schema_id,
                Operation::Update { old: _, new } => new.schema_id,
                Operation::Terminate => bail!("Source shouldn't receive Terminate"),
                Operation::SchemaUpdate { new: _ } => bail!("Source shouldn't get SchemaUpdate"),
            }
            .unwrap();
            fw.send(msg, schema_id.id as u16).unwrap();
        }
        Ok(())
    }

    fn get_output_schema(&self, port: PortHandle) -> Schema {
        let val = self.port_map.get(&port).unwrap();
        val.to_owned()
    }
}
