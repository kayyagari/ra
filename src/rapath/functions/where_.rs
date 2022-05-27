use std::borrow::Borrow;
use std::rc::Rc;

use rawbson::elem::ElementType;

use crate::errors::EvalError;
use crate::rapath::element_utils::to_systype;
use crate::rapath::engine::{eval_with_custom_comparison, ExecContext, simple_compare};
use crate::rapath::EvalResult;
use crate::rapath::expr::{Ast, Function, Operator};
use crate::rapath::stypes::{Collection, SystemType};

pub fn where_<'b>(ctx: &'b impl ExecContext<'b>, base: Rc<SystemType<'b>>, args: &'b Vec<Ast<'b>>) -> EvalResult<'b> {
    let arg_len = args.len();
    if arg_len == 0 {
        return Err(EvalError::from_str("missing argument for where function"));
    }

    if arg_len > 1 {
        return Err(EvalError::new(format!("incorrect number of arguments passed to where function. Expected 1, found {}", arg_len)));
    }

    match base.borrow() {
        SystemType::Element(e) => {
            let r = eval_with_custom_comparison(ctx, &args[0], Rc::clone(&base), Some(nested_compare))?;
            if !r.is_truthy() {
                return Ok(Rc::new(SystemType::Collection(Collection::new_empty())));
            }
            let r = to_systype(*e);
            Ok(Rc::new(r.unwrap()))
        },
        SystemType::Collection(c) => {
            if c.is_empty() {
                return Ok(base);
            }
            let mut r = Collection::new();
            let e = &args[0];
            for item in c.iter() {
                let item_result = eval_with_custom_comparison(ctx, &e, Rc::clone(item), Some(nested_compare))?;
                if item_result.is_truthy() {
                    r.push(Rc::clone(item));
                }
            }

            Ok(Rc::new(SystemType::Collection(r)))
        },
        st => {
            return Err(EvalError::new(format!("invalid base input given to the where function. Expected either an Element or a collection of Elements but found {}", st.get_type())));
        }
    }
}

fn nested_compare<'b>(lhs: Rc<SystemType<'b>>, rhs: Rc<SystemType<'b>>, op: &Operator) -> EvalResult<'b> {
    match lhs.borrow() {
        SystemType::Element(e) => {
            match e.element_type() {
                ElementType::EmbeddedDocument => {
                    let ld = e.as_document()?;
                    for item in ld.into_iter() {
                        let (key, e) = item?;
                        let le = to_systype(e);
                        if let Some(le) = le {
                            let le = Rc::new(le);
                            let result = nested_compare(le, Rc::clone(&rhs), op)?;
                            if result.is_truthy() {
                                return Ok(result);
                            }
                        }
                    }
                    Ok(Rc::new(SystemType::Boolean(false)))
                },
                t => {
                    // control will only reach here, if to_systype couldn't convert the remaining ElementTypes to the appropriate SystemType
                    Err(EvalError::new(format!("unexpected element type found in custom comparator of where function {:?}", t)))
                }
            }
        },
        SystemType::Collection(c) => {
            if !c.is_empty() {
                for item in c.iter() {
                    let item_result = nested_compare(Rc::clone(item), Rc::clone(&rhs), op)?;
                    if item_result.is_truthy() {
                        return Ok(item_result);
                    }
                }
            }

            Ok(Rc::new(SystemType::Boolean(false)))
        },
        _ => {
            simple_compare(lhs, rhs, op)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::rc::Rc;

    use rawbson::DocBuf;
    use rawbson::elem::{Element, ElementType};
    use crate::errors::EvalError;
    use crate::rapath::engine::{eval, ExecContext, UnresolvableExecContext};

    use crate::rapath::parser::parse;
    use crate::rapath::scanner::scan_tokens;
    use crate::rapath::stypes::{SystemType, SystemTypeType};
    use crate::utils::test_utils::{read_patient, to_docbuf};

    #[test]
    fn test_where() {
        let bdoc = bson::doc!{"inner": [{"k": 1}, {"k": 1}, {"k": 2}] };
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let tokens = scan_tokens("inner.where(k = 1)").unwrap();
        let e = parse(tokens).unwrap();
        let doc_base = SystemType::Element(doc_el);
        let ctx = UnresolvableExecContext::new(Rc::new(doc_base));
        let result = eval(&ctx, &e, ctx.root_resource());
        assert!(result.is_ok());
        let result = result.unwrap();
        let result = &*result.borrow();
        let result = match result {
            SystemType::Collection(c) => c,
            st => panic!("{}", format!("expected a collection, but found {}", st.get_type()))
        };
        assert_eq!(2, result.len());
        println!("{:?}", result);
    }

    #[test]
    fn test_where_in_primitive_array() -> Result<(), EvalError> {
        let mut p_json = read_patient();
        let p1 = to_docbuf(&p_json);
        let p1 = Element::new(ElementType::EmbeddedDocument, p1.as_bytes());
        let tokens = scan_tokens("name.where(given = 'Duck')").unwrap();
        let e = parse(tokens).unwrap();
        let doc_base = Rc::new(SystemType::Element(p1));
        let ctx = UnresolvableExecContext::new(Rc::clone(&doc_base));
        let result = eval(&ctx, &e, Rc::clone(&doc_base))?;
        assert!(result.is_truthy());

        let tokens = scan_tokens("name.where(given = 'Peacock')").unwrap();
        let e = parse(tokens).unwrap();
        let result = eval(&ctx, &e, Rc::clone(&doc_base))?;
        assert!(!result.is_truthy());

        Ok(())
    }
}