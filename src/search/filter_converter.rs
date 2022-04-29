use log::debug;
use crate::errors::EvalError;
use crate::res_schema::SchemaDef;
use crate::ResourceDef;
use crate::search::{ComparisonOperator, Filter, SearchParamPrefix, SearchParamType};

pub fn param_to_filter<'r>(name: &str, mut value: &str, rd: &ResourceDef, sd: &SchemaDef) -> Result<Filter<'r>, EvalError> {
    debug!("creating a filter from the query parameter {} with value {}", name, value);
    let at_name = name.split(":").next().unwrap();
    let spd_and_expr = sd.get_search_param_expr_for_res(at_name, &rd.name);
    if let None = spd_and_expr {
        return Err(EvalError::new(format!("there is no search parameter defined with code {} on {}", at_name, rd.name)));
    }

    let (spd, _) = spd_and_expr.unwrap();

    let mut op = ComparisonOperator::EQ;
    match spd.param_type {
        SearchParamType::Number | SearchParamType::Date | SearchParamType::Quantity => {
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
        },
        _ => {}
    }

    let filter = Filter::SimpleFilter {identifier: name.to_string(), operator: op, value: value.to_string()};
    Ok(filter)
}

#[cfg(test)]
mod tests {
    use std::process::id;
    use crate::search::{ComparisonOperator, Filter};
    use crate::search::filter_converter::param_to_filter;
    use crate::utils::test_utils::TestContainer;

    #[test]
    fn test_param_to_filter() {
        let tc = TestContainer::new();
        let (db, sd) = tc.setup_db_with_example_patient().unwrap();
        let rd = sd.resources.get("Patient").unwrap();
        let filter = param_to_filter("name", "equardo", rd, &sd).unwrap(); // the eq in equardo shouldn't be treated as a prefix
        match &filter {
            Filter::SimpleFilter {identifier, operator, value} => {
                assert_eq!("name", identifier);
                assert_eq!(&ComparisonOperator::EQ, operator);
                assert_eq!("equardo", value);
            },
            _ => {
                assert!(false, "unexpected filter type");
            }
        }
    }
}