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
        enable_rest -> Nullable<Bool>,
        enable_grpc -> Nullable<Bool>,
        sql -> Text,
        data_maper -> Text,
        source_ids -> Text,
        history_type -> Text,
        refresh_config -> Text,
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

diesel::joinable!(sources -> connections (connection_id));

diesel::allow_tables_to_appear_in_same_query!(
    connections,
    endpoints,
    sources,
);
