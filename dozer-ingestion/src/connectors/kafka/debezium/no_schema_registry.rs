use crate::connectors::kafka::debezium::schema::map_schema;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumMessage;
use crate::connectors::TableInfo;
use crate::errors::DebeziumError::{BytesConvertError, DebeziumConnectionError, JsonDecodeError};
use crate::errors::{ConnectorError, DebeziumError, DebeziumStreamError};
use dozer_types::ingestion_types::KafkaConfig;
use dozer_types::serde_json;

use dozer_types::types::ReplicationChangesTrackingType;
use kafka::client::{FetchOffset, GroupOffsetStorage};
use kafka::consumer::Consumer;

pub struct NoSchemaRegistry {}

impl NoSchemaRegistry {
    pub fn get_schema(
        table_names: Option<Vec<TableInfo>>,
        config: KafkaConfig,
    ) -> Result<
        Vec<(
            String,
            dozer_types::types::Schema,
            ReplicationChangesTrackingType,
        )>,
        ConnectorError,
    > {
        table_names.map_or(Ok(vec![]), |tables| {
            tables.get(0).map_or(Ok(vec![]), |table| {
                let mut con = Consumer::from_hosts(vec![config.broker.clone()])
                    .with_topic(table.table_name.clone())
                    .with_fallback_offset(FetchOffset::Earliest)
                    .with_offset_storage(GroupOffsetStorage::Kafka)
                    .create()
                    .map_err(DebeziumConnectionError)?;

                let mut schemas = vec![];
                let mss = con.poll().map_err(|e| {
                    DebeziumError::DebeziumStreamError(DebeziumStreamError::PollingError(e))
                })?;

                if !mss.is_empty() {
                    for ms in mss.iter() {
                        for m in ms.messages() {
                            let value_struct: DebeziumMessage = serde_json::from_str(
                                std::str::from_utf8(m.value).map_err(BytesConvertError)?,
                            )
                            .map_err(JsonDecodeError)?;
                            let key_struct: DebeziumMessage = serde_json::from_str(
                                std::str::from_utf8(m.key).map_err(BytesConvertError)?,
                            )
                            .map_err(JsonDecodeError)?;

                            let (mapped_schema, _fields_map) = map_schema(
                                &value_struct.schema,
                                &key_struct.schema,
                            )
                            .map_err(|e| {
                                ConnectorError::DebeziumError(DebeziumError::DebeziumSchemaError(e))
                            })?;

                            schemas.push((
                                table.table_name.clone(),
                                mapped_schema,
                                ReplicationChangesTrackingType::FullChanges,
                            ));
                        }
                    }
                }

                Ok(schemas)
            })
        })
    }
}
