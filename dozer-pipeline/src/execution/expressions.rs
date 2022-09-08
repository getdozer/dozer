use std::sync::Arc;

mod values;
mod math_operators;
mod comparators;

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