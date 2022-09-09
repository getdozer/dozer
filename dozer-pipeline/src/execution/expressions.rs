use std::sync::Arc;

pub(crate) mod values;
mod math_operators;
pub(crate) mod comparators;

trait Value {

}

trait StringValue {


}

trait DecimalValue {

}

trait IntegerValue {

}

struct Operation {
    left : Arc<dyn Value>

}