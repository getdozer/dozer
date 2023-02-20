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
        config -> Text,
        name -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::allow_tables_to_appear_in_same_query!(apps, connections,);
