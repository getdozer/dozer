use dozer_types::json_types::json;

use super::*;

async fn call_function(module: &str, args: Vec<JsonValue>) -> Result<JsonValue, AnyError> {
    let (mut runtime, functions) =
        Runtime::new::<fn() -> Extension>(vec![format!("src/runtime/{module}")], vec![]).await?;
    runtime.call_function(functions[0], args).await
}

#[tokio::test]
async fn test_runtime() {
    assert_eq!(
        call_function("square.js", vec![json!(2.0)]).await.unwrap(),
        json!(4.0)
    );
}

#[tokio::test]
async fn test_function_call_exception() {
    let error = call_function("exception.js", vec![]).await.unwrap_err();
    assert_eq!(error.to_string(), "uncaught javascript exception");
}

#[tokio::test]
async fn test_async_function_call() {
    let Ok(result) = call_function("fetch.js", vec![])
        .await
        .unwrap()
        .into_array()
    else {
        panic!("expected array")
    };
    assert!(!result.is_empty());
}

#[tokio::test]
async fn test_async_function_call_exception() {
    let error = call_function("fetch_exception.js", vec![])
        .await
        .unwrap_err();
    assert!(error.to_string().starts_with("SyntaxError: "));
}
