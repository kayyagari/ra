use log::debug;
use crate::errors::EvalError;
use crate::search::{ComparisonOperator, Filter, SearchParamPrefix};

pub fn param_to_filter<'r>(name: &str, mut value: &str) -> Filter<'r> {
    let mut op = ComparisonOperator::EQ;
    if value.len() > 2 {
        let (prefix_str, suffix) = value.split_at(2);
        let mut prefix = SearchParamPrefix::from(prefix_str);
        if prefix == SearchParamPrefix::Unknown {
            debug!("unknown prefix {}, defaulting to eq", prefix_str);
            prefix = SearchParamPrefix::Eq;
        }
        op = ComparisonOperator::from(prefix);
        value = suffix;
    }

    Filter::SimpleFilter {identifier: name.to_string(), operator: op, value: value.to_string()}
}