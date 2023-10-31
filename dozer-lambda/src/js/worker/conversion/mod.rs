use deno_runtime::{
    deno_core::{anyhow::Context, error::AnyError, serde_v8},
    deno_napi::v8::{self, HandleScope, Local, Name, Object, Value},
};
use dozer_types::{json_types::field_to_json_value, types::Field};

pub fn operation_to_v8_value<'s>(
    scope: &mut HandleScope<'s>,
    index: f64,
    typ: &str,
    field_names: &[String],
    new_values: Vec<Field>,
    old_values: Option<Vec<Field>>,
) -> Result<Local<'s, Value>, AnyError> {
    let mut names = vec![
        string_to_v8_name(scope, "index")?,
        string_to_v8_name(scope, "typ")?,
        string_to_v8_name(scope, "new")?,
    ];
    if old_values.is_some() {
        names.push(string_to_v8_name(scope, "old")?);
    }

    let mut values = vec![
        v8::Number::new(scope, index).into(),
        string_to_v8_name(scope, typ)?.into(),
        fields_to_v8_value(scope, field_names, new_values)?,
    ];
    if let Some(old_values) = old_values {
        values.push(fields_to_v8_value(scope, field_names, old_values)?);
    }

    Ok(create_v8_object(scope, &names, &values))
}

fn string_to_v8_name<'s>(
    scope: &mut HandleScope<'s>,
    s: &str,
) -> Result<Local<'s, Name>, AnyError> {
    v8::String::new(scope, s)
        .map(Into::into)
        .context(format!("string too long: {}", s.len()))
}

fn create_v8_object<'s>(
    scope: &mut HandleScope<'s>,
    names: &[Local<Name>],
    values: &[Local<Value>],
) -> Local<'s, Value> {
    let prototype = v8::null(scope).into();
    Object::with_prototype_and_properties(scope, prototype, names, values).into()
}

fn fields_to_v8_value<'s>(
    scope: &mut HandleScope<'s>,
    field_names: &[String],
    fields: Vec<Field>,
) -> Result<Local<'s, Value>, AnyError> {
    let names = field_names
        .iter()
        .map(|name| string_to_v8_name(scope, name))
        .collect::<Result<Vec<_>, _>>()?;
    let values = fields
        .into_iter()
        .map(|field| field_to_v8_value(scope, field))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(create_v8_object(scope, &names, &values))
}

fn field_to_v8_value<'s>(
    scope: &mut HandleScope<'s>,
    field: Field,
) -> Result<Local<'s, Value>, AnyError> {
    let field = field_to_json_value(field)?;
    serde_v8::to_v8(scope, field).map_err(Into::into)
}
