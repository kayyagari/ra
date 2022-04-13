use log::debug;
use crate::errors::EvalError;
use crate::res_schema::SchemaDef;
use crate::ResourceDef;
use crate::search::{ComparisonOperator, Filter, SearchParamPrefix, SearchParamType};

pub fn param_to_filter<'r>(name: &str, mut value: &str, rd: &ResourceDef, sd: &SchemaDef) -> Result<Filter<'r>, EvalError> {
    let spd_and_expr = sd.get_search_param_expr_for_res(name, &rd.name);
    if let None = spd_and_expr {
        return Err(EvalError::new(format!("there is no search parameter defined with code {} on {}", name, rd.name)));
    }

    let (spd, _) = spd_and_expr.unwrap();

    let mut op = ComparisonOperator::EQ;
    if spd.param_type != SearchParamType::String && value.len() > 2 {
        let (prefix_str, suffix) = value.split_at(2);
        let mut prefix = SearchParamPrefix::from(prefix_str);
        if prefix == SearchParamPrefix::Unknown {
            debug!("unknown prefix {}, defaulting to eq", prefix_str);
            prefix = SearchParamPrefix::Eq;
        }
        op = ComparisonOperator::from(prefix);
        value = suffix;
    }

    let filter = Filter::SimpleFilter {identifier: name.to_string(), operator: op, value: value.to_string()};
    Ok(filter)
}