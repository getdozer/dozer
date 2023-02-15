// @generated automatically by Diesel CLI.

diesel::table! {
    apps (id) {
        id -> Text,
        name -> Text,
        config -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

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

diesel::allow_tables_to_appear_in_same_query!(apps, connections,);
