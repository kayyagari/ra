use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

use rawbson::elem::Element;

use crate::errors::EvalError;
use crate::rapath::expr::Ast;
use crate::rapath::expr::Ast::*;
use crate::rapath::expr::Operator::*;
use crate::rapath::stypes::{Collection, SystemNumber, SystemType};

// pub struct ExecContext<'a> {
//     env_vars: &'a HashMap<String, String>
// }

pub type EvalResult<'a> = Result<SystemType<'a>, EvalError>;

impl<'a> Ast<'a> {
    pub fn eval(&self, base: &SystemType<'a>) -> Result<Rc<SystemType<'a>>, EvalError> {
        match self {
            Binary {lhs, rhs, op} => {
              match op {
                  Plus => {
                      let lr = lhs.eval(base)?;
                      let rr = rhs.eval(base)?;
                      lr.add(rr)
                  },
                  _ => {
                      Err(EvalError::from_str("unsupported binary operation"))
                  }
              }
            },
            Literal {val} => {
                Ok(val.clone())
            }
            _ => {
                Err(EvalError::from_str("unsupported expression"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
    fn test_addition() {
        let tokens = scan_tokens("1+1").unwrap();
        let e = parse(tokens).unwrap();
        let dummy_base = SystemType::Boolean(true);
        let result = e.eval(&dummy_base);
        //println!("{:?}", result.as_ref().err().unwrap());
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Number, result.get_type());
        println!("{:?}", result);
        assert_eq!(SystemType::Number(SystemNumber::new_integer(2)), *result);
    }
}