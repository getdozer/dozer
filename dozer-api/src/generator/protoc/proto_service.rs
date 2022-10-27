use crate::errors::GenerationError;

use super::util::convert_dozer_type_to_proto_type;
use dozer_types::{
    models::api_endpoint::ApiEndpoint,
    serde::{self, Deserialize, Serialize},
};
use heck::ToPascalCase;
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
                "repeated {} {} = 1;\n",
                self.schema_name.to_owned().to_pascal_case(),
                self.schema_name.to_owned().to_lowercase()
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

    fn _sort_option_model(&self) -> RPCMessage {
        RPCMessage {
            name: "SortOptions".to_owned(),
            props: vec![
                "enum SortDirection { \n".to_owned(),
                "  asc = 0; \n".to_owned(),
                "  desc = 1; \n".to_owned(),
                "} \n".to_owned(),
                "string field_name = 1; \n".to_owned(),
                "SortDirection direction = 3; \n".to_owned(),
            ],
        }
    }

    fn _filter_expression_model(&self) -> RPCMessage {
        RPCMessage {
            name: "FilterExpression".to_owned(),
            props: vec![
                "oneof expression {\n".to_owned(),
                "  SimpleExpression simple = 1;\n".to_owned(),
                "  AndExpression and = 2;\n".to_owned(),
                "} \n".to_owned(),
            ],
        }
    }

    fn _simple_filter_expression_model(&self) -> RPCMessage {
        RPCMessage {
            name: "SimpleExpression".to_owned(),
            props: vec![
                "enum Operator {\n".to_owned(),
                "  LT = 0;\n".to_owned(),
                "  LTE = 1;\n".to_owned(),
                "  EQ = 2;\n".to_owned(),
                "  GT = 3;\n".to_owned(),
                "  GTE = 4;\n".to_owned(),
                "  Contains = 5;\n".to_owned(),
                "  MatchesAny = 6;\n".to_owned(),
                "  MatchesAll = 7;\n".to_owned(),
                "} \n".to_owned(),
                "string field = 1;\n".to_owned(),
                "Operator operator = 2;\n".to_owned(),
                "google.protobuf.Value value = 3;\n".to_owned(),
            ],
        }
    }

    fn _and_filter_expression_model(&self) -> RPCMessage {
        RPCMessage {
            name: "AndExpression".to_owned(),
            props: vec!["repeated FilterExpression and_expression = 1;\n".to_owned()],
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

    pub fn get_grpc_metadata(&self) -> Result<ProtoMetadata, GenerationError> {
        let package_name = String::from("Dozer");
        let service_name = format!("{}Service", self.endpoint.name.to_owned().to_pascal_case());
        let get_rpc = self._get_message();
        let get_by_id_rpc = self._get_by_id_message()?;
        let query_rpc = self._query_message();
        let main_model = self._main_model();
        let sort_exp_model = self._sort_option_model();
        let filter_expression_model = self._filter_expression_model();
        let simple_expression_model = self._simple_filter_expression_model();
        let and_expression_model = self._and_filter_expression_model();

        let rpc_functions = vec![
            get_rpc.to_owned().0,
            get_by_id_rpc.to_owned().0,
            query_rpc.to_owned().0,
        ];
        let mut rpc_message = vec![
            main_model,
            sort_exp_model,
            filter_expression_model,
            simple_expression_model,
            and_expression_model,
        ];
        rpc_message.extend(get_rpc.to_owned().1);
        rpc_message.extend(get_by_id_rpc.to_owned().1);
        rpc_message.extend(query_rpc.to_owned().1);
        let mut function_with_type = HashMap::new();

        function_with_type.insert(get_rpc.0.name, GrpcType::List);
        function_with_type.insert(get_by_id_rpc.0.name, GrpcType::GetById);
        function_with_type.insert(query_rpc.0.name, GrpcType::Query);

        let import_libs = vec![String::from("google/protobuf/struct.proto")];
        let metadata = ProtoMetadata {
            package_name,
            service_name,
            rpc_functions,
            messages: rpc_message,
            functions_with_type: function_with_type,
            import_libs: import_libs,
        };

        Ok(metadata)
    }
}
