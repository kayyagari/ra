use std::borrow::Borrow;
use std::rc::Rc;
use rawbson::elem::{Element, ElementType};
use crate::errors::EvalError;
use crate::rapath::EvalResult;
use crate::rapath::stypes::{SystemNumber, SystemString, SystemType, Collection};
use log::error;
use rawbson::RawError;

pub fn to_systype(el: Element) -> Option<SystemType> {
    match el.element_type() {
        ElementType::EmbeddedDocument => {
            return Some(SystemType::Element(el));
        },
        ElementType::Array => {
            let array = el.as_array();
            if array.is_err() {
                error!("failed to convert element as an array {}", array.err().unwrap());
                return None;
            }
            let array = array.unwrap();
            let array = array.into_iter();
            let mut collection = Collection::new();
            for item in array {
                if let Ok(item) = item {
                    let stype = to_systype(item);
                    if stype.is_some() {
                        collection.push(Rc::new(stype.unwrap()));
                    }
                }
            }

            return Some(SystemType::Collection(collection));
        },
        ElementType::Int64 => {
            let i = el.as_i64();
            if let Ok(i) = i {
                return Some(SystemType::Number(SystemNumber::new_integer(i)));
            }
        },
        ElementType::Int32 => {
            let i = el.as_i32();
            if let Ok(i) = i {
                return Some(SystemType::Number(SystemNumber::new_integer(i as i64)));
            }
        },
        ElementType::Double => {
            let f = el.as_f64();
            if let Ok(f) = f {
                return Some(SystemType::Number(SystemNumber::new_decimal(f)));
            }
        },
        ElementType::String => {
            let s = el.as_str();
            if let Ok(s) = s {
                return Some(SystemType::String(SystemString::from_slice(s)));
            }
        },
        ElementType::Boolean => {
            let b = el.as_bool();
            if let Ok(b) = b {
                return Some(SystemType::Boolean(b));
            }
        },
        // ElementType::DateTime => {
        //     todo!("conversion of date time from Element is yet to be supported");
        //     return None;
        // },
        _ => {
            return None;
        }
    }

    None
}

pub fn eval_path<'a, 'b>(name: &'a String, base: Rc<SystemType<'b>>) -> EvalResult<'b> {
    match &*base.borrow() {
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
                                            collection.push(Rc::new(st));
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
        SystemType::Collection(c) => {
            if c.is_empty() {
                return Ok(base);
            }
            let mut r = Collection::new();
            for item in c.iter() {
                if let SystemType::Element(e) = item.borrow() {
                    let item_result = eval_path(name, Rc::clone(item))?;
                    if !item_result.is_empty() {
                        r.push(item_result);
                    }
                }
                else {
                    return Err(EvalError::new(format!("a collection of SystemType Element are expected for path evaluation, but instead found one {}", item.get_type())));
                }
            }

            Ok(Rc::new(SystemType::Collection(r)))
        },
        _ => {
            return Err(EvalError::new(format!("invalid SystemType for path {}. It must be either an element or a collection of elements", name)));
        }
    }
}

pub fn eq(lhs: &Element, rhs: &Element) -> Result<bool, RawError>  {
    let ltype = lhs.element_type();
    let rtype = rhs.element_type();
    if ltype != rtype {
        return Ok(false);
    }

    match ltype {
        ElementType::String => {
            let l = lhs.as_str()?;
            let r = rhs.as_str()?;
            Ok(l == r)
        },
        ElementType::Boolean => {
            let l = lhs.as_bool()?;
            let r = rhs.as_bool()?;
            Ok(l == r)
        },
        ElementType::Int32 => {
            let l = lhs.as_i32()?;
            let r = rhs.as_i32()?;
            Ok(l == r)
        },
        ElementType::Int64 => {
            let l = lhs.as_i64()?;
            let r = rhs.as_i64()?;
            Ok(l == r)
        },
        ElementType::Double => {
            let l = lhs.as_f64()?;
            let r = rhs.as_f64()?;
            Ok(l == r)
        },
        ElementType::Null => {
            let l = lhs.as_null()?;
            let r = rhs.as_null()?;
            Ok(l == r)
        },
        ElementType::DateTime => {
            let l = lhs.as_datetime()?;
            let r = rhs.as_datetime()?;
            Ok(l == r)
        },
        ElementType::Timestamp => {
            let l = lhs.as_timestamp()?;
            let r = rhs.as_timestamp()?;
            Ok(l == r)
        },
        ElementType::EmbeddedDocument => {
            let ld = lhs.as_document()?;
            let rd = rhs.as_document()?;
            for item in ld.into_iter() {
                let (key, e) = item?;
                let re = rd.get(key)?;
                if let None = re {
                    return Ok(false);
                }
                let b = eq(&e, re.as_ref().unwrap())?;
                if !b {
                    return Ok(false);
                }
            }
            Ok(true)
        },
        ElementType::Array => {
            let la = lhs.as_array()?;
            let ra = rhs.as_array()?;

            let mut riter = ra.into_iter();
            for litem in  la.into_iter() {
                let le = litem?;
                if let Some(re) = riter.next() {
                    let re = re?;
                    let b = eq(&le, &re)?;
                    if !b {
                        return Ok(false);
                    }
                }
                else {
                    // right array is shorter
                    return Ok(false);
                }
            }

            if let Some(e) = riter.next() {
                // left array is shorter
                return Ok(false);
            }

            Ok(true)
        },
        _ => {
            return Err(RawError::UnexpectedType);
        }
    }
}
