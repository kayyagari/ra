use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

use rawbson::elem::Element;

use crate::rapath::expr::Ast;
use crate::rapath::expr::Ast::*;
use crate::rapath::expr::Operator::*;
use crate::rapath::stypes::{Collection, SystemType, SystemNumber};

// pub struct ExecContext<'a> {
//     env_vars: &'a HashMap<String, String>
// }

pub type EvalResult<'a> = Result<SystemType<'a>, EvalError>;

#[derive(Debug)]
pub struct EvalError {
    msg: String
}
impl Error for EvalError{}
impl Display for EvalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.msg.as_str())
    }
}

impl EvalError {
    pub fn new(msg: String) -> Self {
        EvalError{msg}
    }

    pub fn from_str(msg: &str) -> Self {
        EvalError{msg: String::from(msg)}
    }
}

pub fn add<'a>(lhs: Rc<SystemType<'a>>, rhs: Rc<SystemType<'a>>) -> Result<Rc<SystemType<'a>>, EvalError> {
    match &*lhs {
        SystemType::String(s) => {
            let r = rhs.get_as_string().unwrap();
            let s = format!("{}{}", s, r);
            Ok(Rc::new(SystemType::String(s)))
        },
        SystemType::Number(n) => {
            let l = n.get_as_number().unwrap();
            let r = rhs.get_as_number().unwrap();
            let sd = l + r;
            let sd = SystemNumber::new_integer(sd);
            Ok(Rc::new(SystemType::Number(sd)))
        }
        _ => {
            Err(EvalError::from_str("unsupported data type"))
        }
    }
}

impl<'a> Ast<'a> {
    pub fn eval(&self, base: &SystemType<'a>) -> Result<Rc<SystemType<'a>>, EvalError> {
        match self {
            Binary {lhs, rhs, op} => {
              match op {
                  Plus => {
                      let lr = lhs.eval(base)?;
                      let rr = rhs.eval(base)?;
                      add(lr,rr)
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

    use crate::rapath::stypes::SystemTypeType;
    use crate::rapath::parser::parse;
    use crate::rapath::scanner::scan_tokens;
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
        //assert_eq!(SystemTypeType::Number, result.get_type());
        println!("{:?}", result);
    }
}