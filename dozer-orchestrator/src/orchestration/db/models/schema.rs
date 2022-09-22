// @generated automatically by Diesel CLI.

diesel::table! {
  connections (id) {
      id -> Text,
      auth -> Text,
      db_type -> Text,
  }
}

diesel::table! {
  sources (id) {
      id -> Text,
      table_name -> Nullable<Text>,
      connection_id -> Nullable<Text>,
      connection_table_name -> Nullable<Text>,
      data_layout -> Nullable<Text>,
      refresh_options -> Nullable<Text>,
  }
}

diesel::joinable!(sources -> connections (connection_id));

diesel::allow_tables_to_appear_in_same_query!(connections, sources,);
