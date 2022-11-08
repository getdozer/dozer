use super::util::convert_dozer_type_to_proto_type;
use crate::errors::GenerationError;
use dozer_types::{
    models::api_endpoint::ApiEndpoint,
    serde::{self, Deserialize, Serialize},
};
use heck::{ToPascalCase, ToUpperCamelCase};
use std::fmt::Write;
use std::{collections::HashMap, vec};

pub struct ProtoService {
    schema: dozer_types::types::Schema,
    schema_name: String,
    endpoint: ApiEndpoint,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub struct ProtoMetadata {
    package_name: String,
    service_name: String,
    rpc_functions: Vec<RPCFunction>,
    messages: Vec<RPCMessage>,
    import_libs: Vec<String>,
    pub functions_with_type: HashMap<String, GrpcType>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub struct RPCFunction {
    name: String,
    argument: String,
    response: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub struct RPCMessage {
    name: String,
    props: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub enum GrpcType {
    List,
    GetById,
    Query,
    OnInsert,
    OnUpdate,
    OnDelete,
    OnSchemaChange,
}

impl ProtoService {
    pub fn new(
        schema: dozer_types::types::Schema,
        schema_name: String,
        endpoint: ApiEndpoint,
    ) -> Self {
        Self {
            schema,
            endpoint,
            schema_name,
        }
    }

    fn _get_message(&self) -> (RPCFunction, Vec<RPCMessage>) {
        let get_request_str = format!(
            "Get{}Request",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let get_response_str = format!(
            "Get{}Response",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let get_fnc = RPCFunction {
            name: self.endpoint.name.to_owned(),
            argument: get_request_str.to_owned(),
            response: get_response_str.to_owned(),
        };
        let get_request = RPCMessage {
            name: get_request_str,
            props: vec![],
        };
        let get_response = RPCMessage {
            name: get_response_str,
            props: vec![format!(
                "repeated {} data = 1;\n",
                self.schema_name.to_owned().to_pascal_case()
            )],
        };
        (get_fnc, vec![get_request, get_response])
    }

    fn _get_by_id_message(&self) -> Result<(RPCFunction, Vec<RPCMessage>), GenerationError> {
        let get_by_id_req_str = format!(
            "Get{}ByIdRequest",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let get_by_id_res_str = format!(
            "Get{}ByIdResponse",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let get_byid_fnc = RPCFunction {
            name: String::from("by_id"),
            argument: get_by_id_req_str.to_owned(),
            response: get_by_id_res_str.to_owned(),
        };

        let primary_idx = self.schema.primary_index.first().ok_or_else(|| {
            GenerationError::MissingPrimaryKeyToQueryById(self.endpoint.name.to_owned())
        })?;
        let primary_type = &self.schema.fields[primary_idx.to_owned()];
        let primary_proto_type = convert_dozer_type_to_proto_type(primary_type.typ.to_owned())?;
        let primary_field = primary_type.name.to_owned();
        let get_by_id_request_model = RPCMessage {
            name: get_by_id_req_str,
            props: vec![format!("{} {} = 1;\n", primary_proto_type, primary_field)],
        };

        let get_by_id_response_model = RPCMessage {
            name: get_by_id_res_str,
            props: vec![format!(
                "optional {} {} = 1;\n",
                self.schema_name.to_owned().to_pascal_case(),
                self.schema_name.to_owned().to_lowercase()
            )],
        };
        Ok((
            get_byid_fnc,
            vec![get_by_id_request_model, get_by_id_response_model],
        ))
    }

    fn _filter_expression_model(&self) -> RPCMessage {
        let props_message: Vec<String> = self
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| -> String {
                let mut result = "".to_owned();
                let proto_type = convert_dozer_type_to_proto_type(field.typ.to_owned()).unwrap();
                let _ = writeln!(
                    result,
                    "  {}Expression {} = {}; ",
                    proto_type.to_upper_camel_case(),
                    field.name,
                    idx + 1
                );
                result
            })
            .collect();
        let mut props_array: Vec<String> = vec!["oneof expression {\n".to_owned()];
        props_array.extend(props_message);
        props_array.push("}\n".to_owned());
        props_array.push("repeated FilterExpression and = 5;\n".to_owned());

        RPCMessage {
            name: "FilterExpression".to_owned(),
            props: props_array,
        }
    }

    fn _type_expression(&self) -> Vec<RPCMessage> {
        let operator = vec!["eq", "lt", "lte", "gt", "gte"];
        let mut types: Vec<String> = self
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(_idx, field)| -> String {
                convert_dozer_type_to_proto_type(field.typ.to_owned()).unwrap()
            })
            .collect();
        types.sort();
        types.dedup();
        let result: Vec<RPCMessage> = types
            .iter()
            .map(|type_name| {
                let mut props_array: Vec<String> = operator
                    .iter()
                    .enumerate()
                    .map(|(idx, &opt)| format!("  {} {} = {};\n", type_name, opt, idx + 1))
                    .collect();
                props_array.insert(0, "oneof exp {\n".to_owned());
                props_array.push("}\n".to_owned());
                RPCMessage {
                    name: format!("{}Expression", type_name.to_upper_camel_case()),
                    props: props_array,
                }
            })
            .collect();
        result
    }

    fn _query_message(&self) -> (RPCFunction, Vec<RPCMessage>) {
        let query_request_str = format!(
            "Query{}Request",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let query_response_str = format!(
            "Query{}Response",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let query_fnc = RPCFunction {
            name: String::from("query"),
            argument: query_request_str.to_owned(),
            response: query_response_str.to_owned(),
        };
        let query_request = RPCMessage {
            name: query_request_str,
            props: vec![
                "optional FilterExpression filter = 1; \n".to_owned(),
                "repeated SortOptions order_by = 2; \n".to_owned(),
                "optional uint32 limit = 3; \n".to_owned(),
                "optional uint32 skip = 4; \n".to_owned(),
            ],
        };

        let query_response = RPCMessage {
            name: query_response_str,
            props: vec![format!(
                "repeated {} {} = 1;\n",
                self.schema_name.to_owned().to_pascal_case(),
                self.schema_name.to_owned().to_lowercase()
            )],
        };
        (query_fnc, vec![query_request, query_response])
    }

    fn _on_insert_message(&self) -> (RPCFunction, Vec<RPCMessage>) {
        let on_insert_request_str = "OnInsertRequest".to_string();
        let on_insert_response_str = "OnInsertResponse".to_string();
        let on_insert_fnc = RPCFunction {
            name: String::from("on_insert"),
            argument: on_insert_request_str.to_owned(),
            response: format!("stream {}", on_insert_response_str),
        };
        let on_insert_request = RPCMessage {
            name: on_insert_request_str,
            props: vec![],
        };

        let on_insert_response = RPCMessage {
            name: on_insert_response_str,
            props: vec!["google.protobuf.Value detail = 1;\n".to_owned()],
        };
        (on_insert_fnc, vec![on_insert_request, on_insert_response])
    }

    fn _on_update_message(&self) -> (RPCFunction, Vec<RPCMessage>) {
        let on_update_request_str = "OnUpdateRequest".to_string();
        let on_update_response_str = "OnUpdateResponse".to_string();
        let on_update_fnc = RPCFunction {
            name: String::from("on_update"),
            argument: on_update_request_str.to_owned(),
            response: format!("stream {}", on_update_response_str),
        };
        let on_update_request = RPCMessage {
            name: on_update_request_str,
            props: vec![],
        };

        let on_update_response = RPCMessage {
            name: on_update_response_str,
            props: vec!["google.protobuf.Value detail = 1;\n".to_owned()],
        };
        (on_update_fnc, vec![on_update_request, on_update_response])
    }

    fn _on_delete_message(&self) -> (RPCFunction, Vec<RPCMessage>) {
        let on_delete_request_str = "OnDeleteRequest".to_string();
        let on_delete_response_str = "OnDeleteResponse".to_string();
        let on_delete_fnc = RPCFunction {
            name: String::from("on_delete"),
            argument: on_delete_request_str.to_owned(),
            response: format!("stream {}", on_delete_response_str),
        };
        let on_delete_request = RPCMessage {
            name: on_delete_request_str,
            props: vec![],
        };

        let on_delete_response = RPCMessage {
            name: on_delete_response_str,
            props: vec!["google.protobuf.Value detail = 1;\n".to_owned()],
        };
        (on_delete_fnc, vec![on_delete_request, on_delete_response])
    }

    fn _on_schema_change_message(&self) -> (RPCFunction, Vec<RPCMessage>) {
        let on_schema_change_request_str = "OnSchemaChangeRequest".to_string();
        let on_schema_change_response_str = "OnSchemaChangeResponse".to_string();
        let on_schema_change_fnc = RPCFunction {
            name: String::from("on_schema_change"),
            argument: on_schema_change_request_str.to_owned(),
            response: format!("stream {}", on_schema_change_response_str),
        };
        let on_schema_change_request = RPCMessage {
            name: on_schema_change_request_str,
            props: vec![],
        };

        let on_schema_change_response = RPCMessage {
            name: on_schema_change_response_str,
            props: vec!["google.protobuf.Value detail = 1;\n".to_owned()],
        };
        (
            on_schema_change_fnc,
            vec![on_schema_change_request, on_schema_change_response],
        )
    }
    fn _sort_option_model(&self) -> RPCMessage {
        RPCMessage {
            name: "SortOptions".to_owned(),
            props: vec![
                "enum SortDirection { \n".to_owned(),
                "  asc = 0; \n".to_owned(),
                "  desc = 1; \n".to_owned(),
                "} \n".to_owned(),
                "string field_name = 1; \n".to_owned(),
                "SortDirection direction = 2; \n".to_owned(),
            ],
        }
    }

    fn _main_model(&self) -> RPCMessage {
        let props_message: Vec<String> = self
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| -> String {
                let mut result = "".to_owned();
                if field.nullable {
                    result.push_str("optional ");
                }
                let proto_type = convert_dozer_type_to_proto_type(field.typ.to_owned()).unwrap();
                let _ = writeln!(result, "{} {} = {}; ", proto_type, field.name, idx + 1);
                result
            })
            .collect();

        RPCMessage {
            name: self.schema_name.to_pascal_case(),
            props: props_message,
        }
    }
    pub fn libs_by_type(&self) -> Result<Vec<String>, GenerationError> {
        let type_need_import_libs = [
            "google.protobuf.Timestamp",
            "google.protobuf.Value",
            "google.protobuf.ListValue",
        ];
        let mut libs_import: Vec<String> = self
            .schema
            .fields
            .iter()
            .map(|field| convert_dozer_type_to_proto_type(field.to_owned().typ).unwrap())
            .filter(|proto_type| -> bool {
                type_need_import_libs.contains(&proto_type.to_owned().as_str())
            })
            .map(|proto_type| match proto_type.as_str() {
                "google.protobuf.Timestamp" => "google/protobuf/timestamp.proto".to_owned(),
                "google.protobuf.ListValue" => "google/protobuf/struct.proto".to_owned(),
                "google.protobuf.Any" => "google/protobuf/any.proto".to_owned(),
                _ => "".to_owned(),
            })
            .collect();
        // default always have struct for streaming on change type
        libs_import.push("google/protobuf/struct.proto".to_owned());
        libs_import.sort();
        libs_import.dedup();
        Ok(libs_import)
    }

    pub fn get_grpc_metadata(&self) -> Result<ProtoMetadata, GenerationError> {
        let package_name = String::from("Dozer");
        let service_name = format!("{}Service", self.endpoint.name.to_owned().to_pascal_case());

        let get_rpc = self._get_message();
        let get_by_id_rpc = self._get_by_id_message()?;
        let query_rpc = self._query_message();
        let on_insert_rpc = self._on_insert_message();
        let on_update_rpc = self._on_update_message();
        let on_delete_rpc = self._on_delete_message();
        let on_schema_change_rpc = self._on_schema_change_message();

        let main_model = self._main_model();
        let filter_exp_model = self._filter_expression_model();
        let type_exp_model = self._type_expression();
        let sort_exp_model = self._sort_option_model();

        let rpc_functions = vec![
            get_rpc.to_owned().0,
            get_by_id_rpc.to_owned().0,
            query_rpc.to_owned().0,
            on_insert_rpc.to_owned().0,
            on_update_rpc.to_owned().0,
            on_delete_rpc.to_owned().0,
            on_schema_change_rpc.to_owned().0,
        ];
        let mut rpc_message = vec![main_model, sort_exp_model];
        rpc_message.extend(get_rpc.to_owned().1);
        rpc_message.extend(get_by_id_rpc.to_owned().1);
        rpc_message.extend(query_rpc.to_owned().1);
        rpc_message.push(filter_exp_model);
        rpc_message.extend(type_exp_model);
        rpc_message.extend(on_insert_rpc.to_owned().1);
        rpc_message.extend(on_update_rpc.to_owned().1);
        rpc_message.extend(on_delete_rpc.to_owned().1);
        rpc_message.extend(on_schema_change_rpc.to_owned().1);

        let mut function_with_type = HashMap::new();
        function_with_type.insert(get_rpc.0.name, GrpcType::List);
        function_with_type.insert(get_by_id_rpc.0.name, GrpcType::GetById);
        function_with_type.insert(query_rpc.0.name, GrpcType::Query);
        function_with_type.insert(on_insert_rpc.0.name, GrpcType::OnInsert);
        function_with_type.insert(on_update_rpc.0.name, GrpcType::OnUpdate);
        function_with_type.insert(on_delete_rpc.0.name, GrpcType::OnDelete);
        function_with_type.insert(on_schema_change_rpc.0.name, GrpcType::OnSchemaChange);

        let import_libs: Vec<String> = self.libs_by_type()?;
        let metadata = ProtoMetadata {
            package_name,
            service_name,
            rpc_functions,
            messages: rpc_message,
            functions_with_type: function_with_type,
            import_libs,
        };
        Ok(metadata)
    }
}
