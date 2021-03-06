use std::borrow::Borrow;
use std::rc::Rc;
use rawbson::elem::{Element, ElementType};
use crate::errors::{EvalError, RaError};
use crate::rapath::EvalResult;
use crate::rapath::stypes::{SystemNumber, SystemString, SystemType, Collection, SystemTypeType};
use log::{debug, error};
use rawbson::{Doc, RawError};
use crate::dtypes::DataType;

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
                        if let SystemType::Collection(inner_col) = item_result.borrow() {
                            for inner_item in inner_col.iter() {
                                r.push(Rc::clone(inner_item));
                            }
                        }
                        else {
                            r.push(item_result);
                        }
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

pub fn equals(lhs: &Element, rhs: &Element) -> Result<bool, RawError>  {
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
                let b = equals(&e, re.as_ref().unwrap())?;
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
                    let b = equals(&le, &re)?;
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

pub fn is_resource_of_type(e: &Element, name: &str) -> bool {
    match e.element_type() {
        ElementType::EmbeddedDocument => {
            let doc = e.as_document();
            if let Ok(doc) = doc {
                let rt = doc.get("resourceType");
                if let Ok(rt) = rt {
                    if let Some(rt) = rt {
                        if let Ok(rt) = rt.as_str() {
                            return rt == name;
                        }
                    }
                }
            }
        },
        _ => {}
    }

    false
}

pub fn get_attribute_to_cast_to<'b>(base: Rc<SystemType<'b>>, at_name: &str, at_and_type_name: &str) -> EvalResult<'b> {
    match base.borrow() {
        SystemType::Element(e) => {
            match e.element_type() {
                ElementType::EmbeddedDocument => {
                    let doc = e.as_document()?;
                    let at = doc.get(at_name)?;
                    let mut value= None;
                    if let Some(at) = at {
                        value = to_systype(at);
                    }
                    else {
                        let at = doc.get(at_and_type_name)?;
                        if let Some(at) = at {
                            value = to_systype(at);
                        }
                    }

                    if let None = value {
                        debug!("neither {} nor {} found in the attributes of the base element", at_name, at_and_type_name);
                        return Ok(Rc::new(SystemType::Collection(Collection::new_empty())));
                    }
                    return Ok(Rc::new(value.unwrap()));
                },
                et => {
                    return Err(EvalError::new(format!("unsupported element type for casting the target value {:?}", et)));
                }
            }
        },
        SystemType::Collection(c) => {
            if !c.is_empty() {
                let mut tmp = Collection::new();
                for item in c.iter() {
                    let v = get_attribute_to_cast_to(Rc::clone(item), at_name, at_and_type_name)?;
                    if !v.is_empty() {
                        tmp.push(v);
                    }
                }
                return Ok(Rc::new(SystemType::Collection(tmp)));
            }
        },
        _ => {}
    }

    Ok(base)
}

/// gathers all the string values, including from the nested elements
// l = long
// s = short
pub fn gather_string_values<'l, 's>(el: &'l Element<'s>, dt: Option<DataType>, values: &mut Vec<&'s str>) -> Result<(), EvalError> {
    match el.element_type() {
        ElementType::EmbeddedDocument => {
            let doc = el.as_document()?;
            for item in doc {
                if let Ok((key, item)) = item {
                    if let Some(dt) = dt {
                        if dt == DataType::HUMANNAME || dt == DataType::ADDRESS {
                            if key == "period" || key == "use" {
                                continue;
                            }
                        }
                    }
                    _gather_string_values(&item, values)?;
                }
            }
        },
        _ => {
            _gather_string_values(el, values)?;
        }
    }

    Ok(())
}

fn _gather_string_values<'l, 's>(el: &'l Element<'s>, values: &mut Vec<&'s str>) -> Result<(), EvalError> {
    match el.element_type() {
        ElementType::EmbeddedDocument => {
            let doc = el.as_document()?;
            for item in doc {
                if let Ok((_, item)) = item {
                    _gather_string_values(&item, values);
                }
            }
        },
        ElementType::Array => {
            let arr = el.as_array()?;
            for item in arr {
                if let Ok(item) = item {
                    _gather_string_values(&item, values);
                }
            }
        },
        ElementType::String => {
            let v = el.as_str()?;
            values.push(v);
        },
        _ => {}
    }

    Ok(())
}

pub fn gather_system_and_code<'i>(el: &'i Element) -> Result<(Option<&'i str>, Option<&'i str>), EvalError> {
    let mut system = None;
    let mut code = None;

    if el.element_type() == ElementType::EmbeddedDocument {
        let doc = el.as_document()?;
        system = get_str_val(doc, "system");
        code = get_str_val(doc, "code");
        if let None = code {
            code = get_str_val(doc, "value");
        }
    }

    Ok((system, code))
}

fn get_str_val<'i>(doc: &'i Doc, name: &str) -> Option<&'i str> {
    let el = doc.get_str(name);
    if let Ok(el) = el {
        if let Some(el) = el {
            return Some(el);
        }
    }
    None
}
#[cfg(test)]
mod tests {
    use bson::doc;
    use rawbson::DocBuf;
    use super::*;

    #[test]
    fn test_gather_string_values() {
        let doc = doc!{"use":"official","family":"A","given":["Kanth"], "number": 2};
        let doc = DocBuf::from_document(&doc);
        let el = Element::new(ElementType::EmbeddedDocument, doc.as_bytes());

        let mut candidates = Vec::new();
        candidates.push((Some(DataType::HUMANNAME), vec!["A", "Kanth"]));
        candidates.push((Some(DataType::ADDRESS), vec!["A", "Kanth"]));
        candidates.push((None, vec!["official", "A", "Kanth"]));

        for (dt, expected_values) in candidates {
            let mut actual_values = Vec::new();
            gather_string_values(&el, dt, &mut actual_values).unwrap(); // unwrapping to catch error
            assert_eq!(expected_values.len(), actual_values.len());
            for e in expected_values {
                let mut found = false;
                for a in &actual_values {
                    if e == *a {
                        found = true;
                        break;
                    }
                }
                assert!(found);
            }
        }
    }
}