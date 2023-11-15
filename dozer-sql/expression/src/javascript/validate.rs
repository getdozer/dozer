use dozer_types::types::{FieldType, Schema};

use crate::{error::Error, execution::Expression};

pub fn validate_args(
    function_name: String,
    args: &[Expression],
    schema: &Schema,
) -> Result<(), Error> {
    if args.len() != 1 {
        return Err(Error::InvalidNumberOfArguments {
            function_name,
            expected: 1..2,
            actual: args.len(),
        });
    }
    let typ = args[0].get_type(schema)?;
    if typ.return_type != FieldType::Json {
        return Err(Error::InvalidFunctionArgumentType {
            function_name,
            argument_index: 0,
            expected: vec![FieldType::Json],
            actual: typ.return_type,
        });
    }
    Ok(())
}
