use log::debug;
use std::collections::HashMap;

use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::{ChannelManager, SourceChannelForwarder};

use crate::pipeline::ingestion_group::IngestionGroup;
use dozer_core::dag::node::{Source, SourceFactory};
use dozer_core::state::StateStore;
use dozer_types::models::connection::Connection;
use dozer_types::types::Schema;

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
        debug!("{:?}", keys);
        keys
    }

    fn build(&self) -> Box<dyn Source> {
        Box::new(ConnectorSource {
            connections: self.connections.to_owned(),
            table_names: self.table_names.to_owned(),
            port_map: self.port_map.to_owned(),
            _table_map: self.table_map.to_owned(),
        })
    }
}

pub struct ConnectorSource {
    port_map: HashMap<u16, Schema>,
    _table_map: HashMap<String, u16>,
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
        let ingestion_group = IngestionGroup {};
        let receiver =
            ingestion_group.run_ingestion(self.connections.to_owned(), self.table_names.to_owned());

        loop {
            let (op, port) = receiver.iter().next().unwrap();
            fw.send(op.seq_no, op.operation, port)?;
        }
    }

    fn get_output_schema(&self, port: PortHandle) -> Schema {
        let val = self.port_map.get(&port).unwrap();
        val.to_owned()
    }
}
