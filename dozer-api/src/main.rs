use std::collections::HashMap;

use dozer_types::types::Field;

fn main() {
    let mut x: HashMap<String, String> = HashMap::new();

    let a = Field::String("vivek".to_string());
    let val = serde_json::to_string(&a).unwrap();
    x.insert("a".to_string(), val);

    let str = serde_json::to_string(&x).unwrap();

    println!("JSON: {}", str);
}
