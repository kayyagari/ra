use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

use rawbson::elem::{Element, ElementType};

use crate::errors::EvalError;
use crate::rapath::element_utils::{eval_path, to_systype};
use crate::rapath::functions::where_::where_;
use crate::rapath::expr::Ast;
use crate::rapath::expr::Ast::*;
use crate::rapath::expr::Operator::*;
use crate::rapath::stypes::{Collection, SystemNumber, SystemString, SystemType};

// pub struct ExecContext<'a> {
//     env_vars: &'a HashMap<String, String>
// }

type EvalFn<'a> = fn(base: &Rc<SystemType<'a>>, args: &'a Vec<Ast<'a>>) -> EvalResult<'a>;

pub struct FunctionDef<'a>(EvalFn<'a>);

pub type EvalResult<'a> = Result<Rc<SystemType<'a>>, EvalError>;

impl<'a> Ast<'a> {
    pub fn eval(&self, base: &Rc<SystemType<'a>>) -> EvalResult {
        match self {
            Binary {lhs, rhs, op} => {
                let lr = lhs.eval(base)?;
                let rr = rhs.eval(base)?;
              match op {
                  Plus => {
                      lr.add(&rr)
                  },
                  Equal => {
                      let r = lr == rr;
                      Ok(Rc::new(SystemType::Boolean(r)))
                  },
                  NotEqual => {
                      let r = lr != rr;
                      Ok(Rc::new(SystemType::Boolean(r)))
                  },
                  _ => {
                      Err(EvalError::from_str("unsupported binary operation"))
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
                let lb = lhs.eval(base)?;
                rhs.eval(&lb)
            },
            Function {name, args} => {
                // TODO replace with dynamic function execution
                //where_(base, &args)
                let f = FunctionDef(where_);
                f.0(base, args)
            },
            e => {
                Err(EvalError::new(format!("unsupported expression {}", e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use bson::spec::ElementType;
    use rawbson::DocBuf;
    use rawbson::elem::Element;

    use crate::rapath::parser::parse;
    use crate::rapath::scanner::scan_tokens;
    use crate::rapath::stypes::{SystemNumber, SystemTypeType};
    use crate::rapath::stypes::SystemType;

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
        let result = e.eval(&Rc::new(doc_base));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Number, result.get_type());
        assert_eq!(SystemType::Number(SystemNumber::new_integer(1)), *result);
    }

    #[test]
    fn test_addition() {
        let tokens = scan_tokens("1+1").unwrap();
        let e = parse(tokens).unwrap();
        let dummy_base = SystemType::Boolean(true);
        let result = e.eval(&Rc::new(dummy_base));
        //println!("{:?}", result.as_ref().err().unwrap());
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Number, result.get_type());
        println!("{:?}", result);
        assert_eq!(SystemType::Number(SystemNumber::new_integer(2)), *result);
    }

    #[test]
    fn test_equal() {
        let tokens = scan_tokens("1 = 1").unwrap();
        let e = parse(tokens).unwrap();
        let dummy_base = SystemType::Boolean(true);
        let result = e.eval(&Rc::new(dummy_base));
        //println!("{:?}", result.as_ref().err().unwrap());
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Boolean, result.get_type());
        println!("{:?}", result);
        assert_eq!(SystemType::Boolean(true), *result);
    }
}