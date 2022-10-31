use dozer_types::serde::{self, Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Event {
    #[prost(enumeration = "EventType", tag = "1")]
    pub r#type: i32,
    #[prost(message, optional, tag = "2")]
    pub detail: ::core::option::Option<::prost_wkt_types::Value>,
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EventType {
    SchemaChange = 0,
    RecordUpdate = 1,
    RecordInsert = 2,
    RecordDelete = 3,
}
impl EventType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EventType::SchemaChange => "schema_change",
            EventType::RecordUpdate => "record_update",
            EventType::RecordInsert => "record_insert",
            EventType::RecordDelete => "record_delete",
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(crate = "self::serde")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OnChangeResponse {
    #[prost(message, optional, tag = "1")]
    pub event: ::core::option::Option<Event>,
}
