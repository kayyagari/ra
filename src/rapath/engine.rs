use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::rc::Rc;

use rawbson::elem::{Element, ElementType};

use crate::errors::EvalError;
use crate::rapath::element_utils::to_systype;
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
            },
            Path {name} => {
                match base {
                    SystemType::Element(e) => {
                        match e.element_type() {
                            ElementType::EmbeddedDocument => {
                                let doc = e.as_document();
                                if doc.is_err() {
                                    return Err(EvalError::new(format!("failed to convert the base element as a document while evaluating the path {}", name)));
                                }
                                let doc = doc.unwrap();
                                let path_val = doc.get(name.as_str());
                                if path_val.is_err() {
                                    return Err(EvalError::new(format!("failed to get the value of path {} from the base document", name)));
                                }
                                let path_val = path_val.unwrap();

                                if path_val.is_none() {
                                    return Ok(Rc::new(SystemType::Collection(Collection::new_empty())));
                                }
                                let path_val = path_val.unwrap();

                                let st = to_systype(path_val);
                                if let None = st {
                                    return Err(EvalError::new(format!("could not convert the result of path {} to a known SystemType", name)));
                                }

                                Ok(Rc::new(st.unwrap()))
                            },
                            ElementType::Array => {
                                let array = e.as_array();
                                if array.is_err() {
                                    return Err(EvalError::new(format!("failed to convert the base element as an array while evaluating the path {}", name)));
                                }
                                let array = array.unwrap();
                                let array = array.into_iter();
                                let mut collection = Collection::new();
                                for item in array {
                                    if let Ok(item_el) = item {
                                        if item_el.element_type() == ElementType::EmbeddedDocument {
                                            let doc = item_el.as_document();
                                            if doc.is_err() {
                                                break;
                                            }
                                            let doc = doc.unwrap();
                                            let name_el = doc.get(name.as_str());
                                            if let Ok(name_el) = name_el {
                                                if let Some(name_el) = name_el {
                                                    let st = to_systype(name_el);
                                                    if let Some(st) = st {
                                                        collection.push(st);
                                                    }
                                                }
                                            }
                                        }
                                        else {
                                            break;
                                        }
                                    }
                                }
                                Ok(Rc::new(SystemType::Collection(collection)))
                            },
                            _ => {
                                return Err(EvalError::new(format!("invalid target element for path {}. Target must be an object or an array of objects", name)));
                            }
                        }
                    },
                    _ => {
                        return Err(EvalError::new(format!("invalid SystemType for path {}. It must be either an element or a collection of elements", name)));
                    }
                }
            },
            SubExpr {lhs, rhs} => {
                let lb = lhs.eval(base)?;
                rhs.eval(&*lb)
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
    fn test_path_expr() {
        let bdoc = bson::doc!{"inner": {"k": 1} };
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let tokens = scan_tokens("inner.k").unwrap();
        let e = parse(tokens).unwrap();
        let doc_base = SystemType::Element(doc_el);
        let result = e.eval(&doc_base);
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
        let result = e.eval(&dummy_base);
        //println!("{:?}", result.as_ref().err().unwrap());
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Number, result.get_type());
        println!("{:?}", result);
        assert_eq!(SystemType::Number(SystemNumber::new_integer(2)), *result);
    }
}