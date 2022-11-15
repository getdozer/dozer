// @generated automatically by Diesel CLI.

diesel::table! {
    apps (id) {
        id -> Text,
        name -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    connections (id) {
        id -> Text,
        app_id -> Text,
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
        app_id -> Text,
        name -> Text,
        path -> Text,
        enable_rest -> Bool,
        enable_grpc -> Bool,
        sql -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    source_endpoints (source_id, endpoint_id) {
        source_id -> Text,
        endpoint_id -> Text,
        app_id -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    sources (id) {
        id -> Text,
        app_id -> Text,
        name -> Text,
        table_name -> Text,
        connection_id -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::joinable!(connections -> apps (app_id));
diesel::joinable!(endpoints -> apps (app_id));
diesel::joinable!(source_endpoints -> apps (app_id));
diesel::joinable!(source_endpoints -> endpoints (endpoint_id));
diesel::joinable!(source_endpoints -> sources (source_id));
diesel::joinable!(sources -> apps (app_id));
diesel::joinable!(sources -> connections (connection_id));

diesel::allow_tables_to_appear_in_same_query!(
    apps,
    connections,
    endpoints,
    source_endpoints,
    sources,
);
