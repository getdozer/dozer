use crate::models::endpoint::{FullText, SecondaryIndex, SecondaryIndexConfig, SortedInverted};

#[test]
fn standard() {
    let secondary = r#"skip_default:
  - field1
  - field2
create:
    - !SortedInverted
        fields:
            - field1
            - field2
    - !FullText
        field: field3
"#;
    let config: SecondaryIndexConfig = serde_yaml::from_str(secondary).unwrap();
    assert_eq!(config.skip_default, vec!["field1", "field2"]);
    assert_eq!(
        config.create,
        vec![
            SecondaryIndex::SortedInverted(SortedInverted {
                fields: vec!["field1".to_string(), "field2".to_string()]
            }),
            SecondaryIndex::FullText(FullText {
                field: "field3".to_string()
            }),
        ]
    );
}

#[test]
fn empty() {
    let secondary = "";
    let config: SecondaryIndexConfig = serde_yaml::from_str(secondary).unwrap();
    assert_eq!(config.skip_default, Vec::<String>::new());
    assert_eq!(config.create, vec![]);
}
