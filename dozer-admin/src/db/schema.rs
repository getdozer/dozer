// @generated automatically by Diesel CLI.

diesel::table! {
    connections (id) {
        id -> Text,
        auth -> Text,
        name -> Text,
        db_type -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    endpoints (id) {
        id -> Text,
        name -> Text,
        path -> Text,
        enable_rest -> Bool,
        enable_grpc -> Bool,
        sql -> Text,
        data_maper -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    source_endpoints (source_id, endpoint_id) {
        source_id -> Text,
        endpoint_id -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    sources (id) {
        id -> Text,
        name -> Text,
        dest_table_name -> Text,
        source_table_name -> Text,
        connection_id -> Text,
        history_type -> Text,
        refresh_config -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::joinable!(source_endpoints -> endpoints (endpoint_id));
diesel::joinable!(source_endpoints -> sources (source_id));
diesel::joinable!(sources -> connections (connection_id));

diesel::allow_tables_to_appear_in_same_query!(connections, endpoints, source_endpoints, sources,);
