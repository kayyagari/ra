use std::borrow::Borrow;
use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::engine::eval;
use crate::rapath::EvalResult;
use crate::rapath::expr::Ast;
use crate::rapath::stypes::{Collection, SystemType};

pub fn union<'b>(base: Rc<SystemType<'b>>, args: &'b Vec<Ast<'b>>) -> EvalResult<'b> {
    let arg_len = args.len();
    if arg_len == 0 {
        return Err(EvalError::from_str("missing argument for union function"));
    }

    if arg_len > 1 {
        return Err(EvalError::new(format!("incorrect number of arguments passed to union function. Expected 1, found {}", arg_len)));
    }

    let rhs = eval(&args[0], Rc::clone(&base))?;
    combine_unique(base, rhs)
}

pub fn combine_unique<'b>(lr: Rc<SystemType<'b>>, rr: Rc<SystemType<'b>>) -> EvalResult<'b> {
    let mut uc = Collection::new();
    add_unique_values_to_collection(&mut uc, lr);
    add_unique_values_to_collection(&mut uc, rr);
    Ok(Rc::new(SystemType::Collection(uc)))
}

fn add_unique_values_to_collection<'b>(uc: &mut Collection<'b>, item: Rc<SystemType<'b>>) {
    if item.is_empty() {
        return;
    }
    match item.borrow() {
        SystemType::Collection(values) => {
            for value in values.iter() {
                uc.push_unique(Rc::clone(value));
            }
        },
        _ => {
            uc.push_unique(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use bson::spec::ElementType;
    use rawbson::DocBuf;
    use rawbson::elem::Element;
    use crate::rapath::engine::eval;
    use crate::rapath::stypes::{Collection, SystemNumber, SystemString, SystemType};
    use crate::utils::test_utils::parse_expression;

    #[test]
    fn test_union() {
        let bdoc = bson::doc!{"inner": [{"k": 2}, {"r": 7, "sub": [0, 11]}] };
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));

        let mut candidates = Vec::new();
        let base = Rc::new(SystemType::Collection(Collection::new_empty()));
        candidates.push(("1 | 1", Rc::new(SystemType::Number(SystemNumber::new_integer(1)))));
        candidates.push(("1 | 2", Rc::new(SystemType::Collection(Collection::from(vec![1, 2])))));
        candidates.push(("inner.r | inner.sub", Rc::new(SystemType::Collection(Collection::from(vec![7, 0, 11])))));

        for (input, expected) in candidates {
            let expr = parse_expression(input);
            let actual = eval(&expr, Rc::clone(&doc_base)).unwrap();
            let result = SystemType::equals(expected, actual).unwrap();
            assert!(result.is_truthy());
        }
    }
}