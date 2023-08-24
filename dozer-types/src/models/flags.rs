use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, prost::Message)]
pub struct Flags {
    /// dynamic grpc enabled; Default: true
    #[prost(bool, tag = "1", default = true)]
    #[serde(default = "default_true")]
    pub dynamic: bool,
    /// http1 + web support for grpc. This is required for browser clients.; Default: true
    #[prost(bool, tag = "2", default = true)]
    #[serde(default = "default_true")]
    pub grpc_web: bool,

    /// push events enabled.; Default: true
    #[prost(bool, tag = "3", default = true)]
    #[serde(default = "default_true")]
    pub push_events: bool,

    /// require authentication to access grpc server reflection service if true.; Default: false
    #[prost(bool, tag = "4", default = false)]
    #[serde(default = "default_false")]
    pub authenticate_server_reflection: bool,
}

fn default_true() -> bool {
    true
}
fn default_false() -> bool {
    false
}
