use std::borrow::Borrow;
use std::rc::Rc;
use crate::rapath::EvalResult;
use crate::rapath::expr::Ast;
use crate::rapath::stypes::SystemType;

pub fn empty<'b>(base: Rc<SystemType<'b>>, _: &'b Vec<Ast<'b>>) -> EvalResult<'b> {
    match base.borrow() {
        SystemType::Collection(c) => {
            Ok(Rc::new(SystemType::Boolean(c.is_empty())))
        },
        _ => Ok(Rc::new(SystemType::Boolean(false)))
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use crate::rapath::engine::eval;
    use crate::rapath::stypes::{Collection, SystemString, SystemType};
    use crate::utils::test_utils::parse_expression;

    #[test]
    fn test_empty() {
        let mut candidates = Vec::new();
        candidates.push((SystemType::Collection(Collection::new()), true));
        candidates.push((SystemType::String(SystemString::new(String::from("this is not empty"))), false));
        let mut data = Collection::new();
        data.push(Rc::new(SystemType::Boolean(true)));
        data.push(Rc::new(SystemType::Boolean(false)));
        candidates.push((SystemType::Collection(data), false));

        let expr = parse_expression("empty()");
        for (base, expected) in candidates {
            let result = eval(&expr, Rc::new(base)).unwrap();
            let result = result.as_bool().unwrap();
            assert_eq!(expected, result);
        }
    }
}