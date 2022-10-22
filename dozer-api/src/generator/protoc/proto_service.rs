use std::{collections::HashMap, vec};

use dozer_types::{
    models::api_endpoint::ApiEndpoint,
    serde::{self, Deserialize, Serialize},
};
use heck::ToPascalCase;

use super::util::convert_dozer_type_to_proto_type;

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
    ) -> anyhow::Result<Self> {
        let service = Self {
            schema,
            endpoint,
            schema_name,
        };
        Ok(service)
    }

    fn _get_message(&self) -> anyhow::Result<(RPCFunction, Vec<RPCMessage>)> {
        let get_request_str = format!(
            "Get{}Request",
            self.endpoint.name.to_owned().to_owned().to_pascal_case()
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
            name: get_request_str.to_owned(),
            props: vec![],
        };
        let get_response = RPCMessage {
            name: get_response_str.to_owned(),
            props: vec![format!(
                "repeated {} {} = 1;\n",
                self.schema_name.to_owned().to_pascal_case(),
                self.schema_name.to_owned().to_lowercase()
            )],
        };
        Ok((get_fnc, vec![get_request, get_response]))
    }

    fn _get_by_id_message(&self) -> anyhow::Result<(RPCFunction, Vec<RPCMessage>)> {
        let get_by_id_req_str = format!(
            "Get{}ByIdRequest",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let get_by_id_res_str = format!(
            "Get{}ByIdResponse",
            self.endpoint.name.to_owned().to_pascal_case()
        );
        let get_byid_fnc = RPCFunction {
            name: format!("{}_by_id", self.endpoint.name.to_owned()),
            argument: get_by_id_req_str.to_owned(),
            response: get_by_id_res_str.to_owned(),
        };
        //let proto_type = convert_dozer_type_to_proto_type(field.typ.to_owned()).unwrap();
        let primary_idx = self.schema.primary_index.first().unwrap().to_owned();
        let primary_type = &self.schema.fields[primary_idx];
        let primary_proto_type =
            convert_dozer_type_to_proto_type(primary_type.typ.to_owned()).unwrap();
        let get_by_id_request_model = RPCMessage {
            name: get_by_id_req_str.to_owned(),
            props: vec![format!("{} id = 1;\n", primary_proto_type)],
        };

        let get_by_id_response_model = RPCMessage {
            name: get_by_id_res_str.to_owned(),
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

    fn _query_message(&self) -> anyhow::Result<(RPCFunction, Vec<RPCMessage>)> {
        let query_request_str = format!(
            "Query{}Request",
            self.endpoint.name.to_owned().to_pascal_case().to_owned()
        );
        let query_response_str = format!(
            "Query{}Response",
            self.endpoint.name.to_owned().to_pascal_case().to_owned()
        );
        let query_fnc = RPCFunction {
            name: format!("query_{}", self.endpoint.name.to_owned().to_owned()),
            argument: query_request_str.to_owned(),
            response: query_response_str.to_owned(),
        };
        let query_request = RPCMessage {
            name: query_request_str.to_owned(),
            props: vec![],
        };

        let query_response = RPCMessage {
            name: query_response_str.to_owned(),
            props: vec![format!(
                "repeated {} {} = 1;\n",
                self.schema_name.to_owned().to_pascal_case(),
                self.schema_name.to_owned().to_lowercase()
            )],
        };
        Ok((query_fnc, vec![query_request, query_response]))
    }

    fn _main_model(&self) -> anyhow::Result<RPCMessage> {
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
                result.push_str(&format!("{} {} = {}; \n", proto_type, field.name, idx + 1));
                return result;
            })
            .collect();

        let main_model_message = RPCMessage {
            name: self.endpoint.name.to_owned().to_pascal_case().to_owned(),
            props: props_message,
        };
        Ok(main_model_message)
    }

    pub fn get_grpc_metadata(&self) -> anyhow::Result<ProtoMetadata> {
        // let package_name = format!("Dozer{}", self.endpoint.name.to_owned().to_pascal_case());
        // let service_name = format!("{}Service", self.endpoint.name.to_owned().to_pascal_case());
        let package_name = "Dozer".to_owned();
        let service_name = "Service".to_owned();
        let get_rpc = self._get_message()?;
        let get_by_id_rpc = self._get_by_id_message()?;
        let query_rpc = self._query_message()?;
        let main_model = self._main_model()?;

        let rpc_functions = vec![
            get_rpc.to_owned().0,
            get_by_id_rpc.to_owned().0,
            query_rpc.to_owned().0,
        ];
        let mut rpc_message = vec![main_model];
        rpc_message.extend(get_rpc.to_owned().1);
        rpc_message.extend(get_by_id_rpc.to_owned().1);
        rpc_message.extend(query_rpc.to_owned().1);
        let mut function_with_type = HashMap::new();

        function_with_type.insert(get_rpc.to_owned().0.name, GrpcType::List);
        function_with_type.insert(get_by_id_rpc.to_owned().0.name, GrpcType::GetById);
        function_with_type.insert(query_rpc.to_owned().0.name, GrpcType::Query);

        let metadata = ProtoMetadata {
            package_name,
            service_name,
            rpc_functions,
            messages: rpc_message,
            functions_with_type: function_with_type,
        };

        Ok(metadata)
    }
}
