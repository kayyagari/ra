use std::borrow::Borrow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

use rawbson::elem::{Element, ElementType};

use crate::errors::EvalError;
use crate::rapath::element_utils::{eval_path, to_systype};
use crate::rapath::EvalResult;
use crate::rapath::functions::where_::where_;
use crate::rapath::expr::{Ast, CmpFunc, Operator};
use crate::rapath::expr::Ast::*;
use crate::rapath::expr::Operator::*;
use crate::rapath::stypes::{Collection, SystemNumber, SystemString, SystemType, SystemTypeType};

// pub struct ExecContext<'a> {
//     env_vars: &'a HashMap<String, String>
// }

    pub fn eval<'a, 'b>(ast: &'a Ast<'a>, base: Rc<SystemType<'b>>) -> EvalResult<'b> where 'a: 'b {
        eval_with_custom_comparison(ast, base, None)
    }

    pub fn eval_with_custom_comparison<'a, 'b>(ast: &'a Ast<'a>, base: Rc<SystemType<'b>>, cmp_func: Option<CmpFunc<'b>>) -> EvalResult<'b> where 'a: 'b {
        match ast {
            Binary {lhs, rhs, op} => {
                match op {
                    Equal | NotEqual |
                    Equivalent | NotEquivalent |
                    Greater | GreaterEqual |
                    Less | LessEqual => {
                        let lr = eval_with_custom_comparison(&lhs, Rc::clone(&base), cmp_func)?;
                        let rr = eval_with_custom_comparison(&rhs, Rc::clone(&base), cmp_func)?;
                        if let Some(cmp_func) = cmp_func {
                            return cmp_func(lr, rr, op);
                        }

                        simple_compare(lr, rr, op)
                    },
                    Plus => {
                        let lr = eval_with_custom_comparison(&lhs, Rc::clone(&base), cmp_func)?;
                        let rr = eval_with_custom_comparison(&rhs, Rc::clone(&base), cmp_func)?;
                        lr.add(&rr)
                    },
                    And => {
                        let lr = eval_with_custom_comparison(&lhs, Rc::clone(&base), cmp_func)?;
                        if lr.is_truthy() {
                            let rr = eval_with_custom_comparison(&rhs, Rc::clone(&base), cmp_func)?;
                            if rr.is_truthy() {
                                return Ok(rr);
                            }
                        }

                        Ok(Rc::new(SystemType::Boolean(false)))
                    },
                    Is => {
                        let lr = eval_with_custom_comparison(&lhs, Rc::clone(&base), cmp_func)?;
                        let rr = eval_with_custom_comparison(&rhs, Rc::clone(&base), cmp_func)?;
                        SystemType::is(lr, rr)
                    }
                    _ => {
                        Err(EvalError::new(format!("unsupported binary operation {:?}", op)))
                    }
                }
            },
            Literal {val} => {
                Ok(Rc::clone(val))
            },
            Path {name} => {
                eval_path(name, base)
            },
            SubExpr {lhs, rhs} => {
                let lb = eval(lhs, base)?;
                eval(rhs, Rc::clone(&lb))
            },
            Function {func} => {
                func.eval_func(base)
            },
            e => {
                Err(EvalError::new(format!("unsupported expression {}", e)))
            }
        }
    }

    #[inline]
    pub fn simple_compare<'b>(mut lhs: Rc<SystemType<'b>>, mut rhs: Rc<SystemType<'b>>, op: &Operator) -> EvalResult<'b> {
        match op {
            Equal => {
                SystemType::equals(lhs, rhs)
            },
            NotEqual => {
                SystemType::not_equals(lhs, rhs)
            },
            Equivalent => {
              SystemType::equiv(lhs, rhs)
            },
            NotEquivalent => {
              SystemType::not_equiv(lhs, rhs)
            },
            Greater => {
                SystemType::gt(lhs, rhs)
            },
            GreaterEqual => {
                SystemType::ge(lhs, rhs)
            },
            Less => {
              SystemType::lt(lhs, rhs)
            },
            LessEqual => {
              SystemType::le(lhs, rhs)
            },
            _ => {
                Err(EvalError::new(format!("unsupported comparison operation {:?}", op)))
            }
        }
    }

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use bson::spec::ElementType;
    use rawbson::DocBuf;
    use rawbson::elem::Element;

    use crate::rapath::parser::parse;
    use crate::rapath::scanner::scan_tokens;
    use crate::rapath::stypes::{SystemNumber, SystemTypeType};
    use crate::rapath::stypes::SystemType;
    use crate::utils::test_utils::parse_expression;

    #[test]
    fn test_doc_as_element() {
        let bdoc = bson::doc!{"inner": {"k": 1} };
        let raw = DocBuf::from_document(&bdoc);
        println!("{:?}", &raw);
        let inner = raw.get_document("inner").unwrap().unwrap();
        println!("{:?}", inner);

        let inner = raw.get("inner").unwrap().unwrap();
        println!("{:?}", &inner);

        let bdoc = bson::doc!{"k": 1};
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        println!("doc's root element {:?}", &doc_el);
        assert_eq!(1, doc_el.as_document().unwrap().get_i32("k").unwrap().unwrap());
    }

    #[test]
    fn test_path_expr() {
        let bdoc = bson::doc!{"inner": {"k": 1} };
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let tokens = scan_tokens("inner.k").unwrap();
        let e = parse(tokens).unwrap();
        let doc_base = SystemType::Element(doc_el);
        let result = eval(&e, Rc::new(doc_base));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Number, result.get_type());
        assert_eq!(1, result.as_i64().unwrap());
    }

    #[test]
    fn test_addition() {
        let tokens = scan_tokens("1+1").unwrap();
        let e = parse(tokens).unwrap();
        let dummy_base = SystemType::Boolean(true);
        let result = eval(&e, Rc::new(dummy_base));
        //println!("{:?}", result.as_ref().err().unwrap());
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Number, result.get_type());

        // FIXME the add() function needs to be improved
        assert_eq!(2.0, result.as_f64().unwrap());
    }

    #[test]
    fn test_equal() {
        let tokens = scan_tokens("1 = 1").unwrap();
        let e = parse(tokens).unwrap();
        let dummy_base = SystemType::Boolean(true);
        let result = eval(&e, Rc::new(dummy_base));
        //println!("{:?}", result.as_ref().err().unwrap());
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Boolean, result.get_type());
        assert!(result.as_bool().unwrap());
    }

    #[test]
    fn test_simple_comparison_non_singleton() {
        let bdoc = bson::doc!{"inner": [{"k": 1}, {"k": 1}, {"k": 2}, {"r": 7}] };
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());

        let e = parse_expression("inner.k = 1");
        let doc_base = Rc::new(SystemType::Element(doc_el));
        let result = eval(&e, Rc::clone(&doc_base));
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().starts_with("cannot compare, lhs"));

        let e = parse_expression("1 = inner.k");
        let doc_base = Rc::new(SystemType::Element(doc_el));
        let result = eval(&e, Rc::clone(&doc_base));
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().starts_with("cannot compare, rhs"));

        let e = parse_expression("inner.r = 7");
        let result = eval(&e, Rc::clone(&doc_base));
        assert!(result.is_ok());
        assert_eq!(true, result.unwrap().as_bool().unwrap());

        let e = parse_expression("1 = inner.r");
        let result = eval(&e, Rc::clone(&doc_base));
        assert!(result.is_ok());
        assert_eq!(false, result.unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_comparison() {
        let mut exprs = Vec::new();
        exprs.push(("1 > 0", true));
        exprs.push(("1 >= 1", true));
        exprs.push(("0 < 1", true));
        exprs.push(("1 <= 1", true));
        exprs.push(("2 <= 1", false));
        exprs.push(("10 > 5", true));
        exprs.push(("10 < 5", false));
        exprs.push(("10 >= 5", true));
        exprs.push(("10 <= 5", false));
        exprs.push(("10 <= 5.0", false));
        exprs.push(("'abc' > 'ABC'", true));
        exprs.push(("'abc' >= 'ABC'", true));
        exprs.push(("'abc' < 'ABC'", false));
        exprs.push(("'abc' <= 'ABC'", false));
        exprs.push(("@2018-03-01 > @2018-01-01", true));
        exprs.push(("@2018-03-01 < @2018-01-01", false));
        exprs.push(("@2018-03-01 >= @2018-01-01", true));
        exprs.push(("@2018-03-01 <= @2018-01-01", false));
        exprs.push(("@2018-03-01T10:30:00 > @2018-03-01T10:00:00", true));
        exprs.push(("@2018-03-01T10:30:00 >= @2018-03-01T10:00:00", true));
        exprs.push(("@2018-03-01T10:30:00 < @2018-03-01T10:00:00", false));
        exprs.push(("@2018-03-01T10:30:00 <= @2018-03-01T10:30:00.000", true));
        exprs.push(("@T10:30:00 > @T10:00:00", true));
        exprs.push(("@T10:30:00 >= @T10:00:00", true));
        exprs.push(("@T10:30:00 < @T10:00:00", false));
        exprs.push(("@T10:30:00 <= @T10:00:00", false));
        exprs.push(("@T10:30:00 > @T10:30:00.000", false));
        exprs.push(("@T10:30:00 < @T10:30:00.000", false));
        exprs.push(("@T10:30:00 >= @T10:30:00.000", true));
        exprs.push(("@T10:30:00 <= @T10:30:00.000", true));

        let base = Rc::new(SystemType::Collection(Collection::new_empty()));
        for (input, expected) in exprs {
            println!("{}", input);
            let e = parse_expression(input);
            let result = eval(&e, Rc::clone(&base)).unwrap();
            assert_eq!(expected, result.as_bool().unwrap());
        }

        let mut exprs_with_empty_result = Vec::new();
        exprs_with_empty_result.push("4 'm' > 4 'cm'");
        exprs_with_empty_result.push("4 'm' < 4 'cm'");
        exprs_with_empty_result.push("4 'm' >= 4 'cm'");
        exprs_with_empty_result.push("4 'm' <= 4 'cm'");
        exprs_with_empty_result.push("@2018-03 > @2018-03-01");
        exprs_with_empty_result.push("@2018-03 < @2018-03-01");
        exprs_with_empty_result.push("@2018-03 >= @2018-03-01");
        exprs_with_empty_result.push("@2018-03 <= @2018-03-01");
        exprs_with_empty_result.push("@2018-03-01T10 > @2018-03-01T10:30");
        exprs_with_empty_result.push("@2018-03-01T10 < @2018-03-01T10:30");
        exprs_with_empty_result.push("@2018-03-01T10 >= @2018-03-01T10:30");
        exprs_with_empty_result.push("@2018-03-01T10 <= @2018-03-01T10:30");
        exprs_with_empty_result.push("@T10 > @T10:30");
        exprs_with_empty_result.push("@T10 < @T10:30");
        exprs_with_empty_result.push("@T10 >= @T10:30");
        exprs_with_empty_result.push("@T10 <= @T10:30");
        for input in exprs_with_empty_result {
            println!("{}", input);
            let e = parse_expression(input);
            let result = eval(&e, Rc::clone(&base)).unwrap();
            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_is() {
        let bdoc = bson::doc!{"resource": {"resourceType": "Provider", "name": "k"}};
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));

        let mut exprs = Vec::new();
        exprs.push(("resource is Provider", true));
        exprs.push(("resource is Patient", false));

        for (input, expected) in exprs {
            let e = parse_expression(input);
            let result = eval(&e, Rc::clone(&doc_base)).unwrap();
            assert_eq!(expected, result.as_bool().unwrap());
        }

        let mut col = Collection::new();
        col.push(Rc::clone(&doc_base));
        let singleton_base = Rc::new(SystemType::Collection(col));
        let e = parse_expression("resource is Provider");
        let result = eval(&e, Rc::clone(&singleton_base)).unwrap();
        assert_eq!(true, result.as_bool().unwrap());

        let mut col = Collection::new();
        col.push(Rc::clone(&doc_base));
        col.push(Rc::clone(&doc_base));
        let result = eval(&e, Rc::new(SystemType::Collection(col)));
        assert!(result.is_err());

        let result = eval(&e, Rc::new(SystemType::Collection(Collection::new_empty()))).unwrap();
        assert!(result.is_empty());
    }
}