use std::borrow::Borrow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

use rawbson::elem::{Element, ElementType};

use crate::errors::EvalError;
use crate::rapath::element_utils::{get_attribute_to_cast_to, eval_path, to_systype};
use crate::rapath::EvalResult;
use crate::rapath::functions::where_::where_;
use crate::rapath::expr::{Ast, CmpFunc, Operator};
use crate::rapath::expr::Ast::*;
use crate::rapath::expr::Operator::*;
use crate::rapath::functions::cast_as::cast;
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
                    },
                    Xor => {
                        let lr = eval_with_custom_comparison(&lhs, Rc::clone(&base), cmp_func)?;
                        if lr.is_empty() {
                            return Ok(lr);
                        }
                        let rr = eval_with_custom_comparison(&rhs, Rc::clone(&base), cmp_func)?;
                        if rr.is_empty() {
                            return Ok(rr);
                        }
                        let lr = lr.is_truthy();
                        let rr = rr.is_truthy();
                        let result;
                        if (lr && !rr) || (!lr && rr) {
                            result = true;
                        }
                        else {
                            result = false;
                        }
                        Ok(Rc::new(SystemType::Boolean(result)))
                    },
                    Implies => {
                        let lr = eval_with_custom_comparison(&lhs, Rc::clone(&base), cmp_func)?;
                        let lr_true = lr.is_truthy();
                        let lr_empty = lr.is_empty();
                        if !lr_empty && !lr_true {
                            return Ok(Rc::new(SystemType::Boolean(true)));
                        }
                        let rr = eval_with_custom_comparison(&rhs, Rc::clone(&base), cmp_func)?;
                        let rr_empty = rr.is_empty();
                        if lr_empty && rr_empty {
                            return Ok(rr);
                        }
                        let rr_true = rr.is_truthy();

                        if lr_true {
                            if rr_true {
                                return Ok(Rc::new(SystemType::Boolean(true)));
                            }
                            else if rr_empty {
                                return Ok(rr); // empty
                            }
                            return Ok(Rc::new(SystemType::Boolean(false)));
                        }
                        else if lr_empty && rr_true {
                            return Ok(Rc::new(SystemType::Boolean(true)));
                        }
                        Ok(Rc::new(SystemType::Collection(Collection::new_empty())))
                    },
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
                eval_with_custom_comparison(rhs, Rc::clone(&lb), cmp_func)
            },
            Function {func} => {
                func.eval_func(base)
            },
            TypeCast {at_name, at_and_type_name, type_name} => {
                let el = get_attribute_to_cast_to(base, at_name, at_and_type_name)?;
                cast(el, type_name)
            },
            Variable {name} => {
                match name.as_str() {
                    "$this" => Ok(Rc::clone(&base)),
                    _ => Err(EvalError::new(format!("unknown variable {}", name)))
                }
            },
            EnvVariable {name} => {
                match name.as_str() {
                    "%resource" => Ok(Rc::clone(&base)),
                    _ => Err(EvalError::new(format!("unknown environment variable {}", name)))
                }
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

    #[test]
    fn test_as() {
        let bdoc = bson::doc! {"value": {"value": 161.42333812930528,
          "unit": "cm",
          "system": "http://unitsofmeasure.org",
          "code": "cm"}, "codeQuantity": {
          "value": 41.76996932711261,
          "unit": "kg",
          "system": "http://unitsofmeasure.org",
          "code": "kg"}, "name": "k"};
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));

        let mut exprs = Vec::new();
        exprs.push(("value as Quantity", true)); // attribute "value" exists
        exprs.push(("code as Quantity", true)); // attribute "code" doesn't exist but "codeQuantity" does
        exprs.push(("value as Quantity > 1 'cm'", true));
        exprs.push(("value as Quantity > 1 'cm' and 0 = 0", true)); // just to check if the parser is doing it right or not
        exprs.push(("code as Quantity < 1 'kg'", false));
        exprs.push(("code as Quantity < 1 'kg' and 1 = 1", false)); // just to check if the parser is doing it right or not

        for (input, expected) in exprs {
            let e = parse_expression(input);
            let result = eval(&e, Rc::clone(&doc_base)).unwrap();
            assert_eq!(expected, result.is_truthy());
        }
    }

    #[test]
    fn test_xor() {
        let bdoc = bson::doc!{"k": 2, "r": 7};
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));

        // outcome: 0 - false, 1 - true, -1 - empty
        let mut exprs = Vec::new();
        exprs.push(("1 = 1 xor k = 2", 0));
        exprs.push(("1 = 1 xor r != 7", 1));
        exprs.push(("1 = 1 xor empty_attribute", -1));

        exprs.push(("1 != 1 xor k = 2", 1));
        exprs.push(("1 != 1 xor r != 7", 0));
        exprs.push(("1 != 1 xor empty_attribute", -1));

        exprs.push(("empty_attribute xor k = 2", -1));
        exprs.push(("empty_attribute xor r != 7", -1));
        exprs.push(("empty_attribute xor empty_attribute", -1));

        for (input, expected) in exprs {
            let e = parse_expression(input);
            let r = eval(&e, Rc::clone(&doc_base)).unwrap();
            match expected {
                -1 => assert!(r.is_empty()),
                1 => assert_eq!(true, r.as_bool().unwrap()),
                0 => assert_eq!(false, r.as_bool().unwrap()),
                _ => {
                    assert!(false, "invalid input, unknown outcome")
                }
            }
        }
    }

    #[test]
    fn test_implies() {
        let bdoc = bson::doc!{"k": 2, "r": 7};
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));

        // outcome: 0 - false, 1 - true, -1 - empty
        let mut exprs = Vec::new();
        exprs.push(("r = 7 implies k = 2", 1));
        exprs.push(("r = 7 implies k != 2", 0));
        exprs.push(("r = 7 implies empty_attribute", -1));

        exprs.push(("r != 7 implies k = 2", 1));
        exprs.push(("r != 7 implies k != 2", 1));
        exprs.push(("r != 7 implies empty_attribute", 1));

        exprs.push(("empty_attribute implies k = 2", 1));
        exprs.push(("empty_attribute implies r != 7", -1));
        exprs.push(("empty_attribute implies empty_attribute", -1));

        for (input, expected) in exprs {
            println!("{}", input);
            let e = parse_expression(input);
            let r = eval(&e, Rc::clone(&doc_base)).unwrap();
            match expected {
                -1 => assert!(r.is_empty()),
                1 => assert_eq!(true, r.as_bool().unwrap()),
                0 => assert_eq!(false, r.as_bool().unwrap()),
                _ => {
                    assert!(false, "invalid input, unknown outcome")
                }
            }
        }
    }
}