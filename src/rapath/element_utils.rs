use rawbson::elem::{Element, ElementType};
use crate::rapath::stypes::{SystemNumber, SystemString, SystemType};

pub fn to_systype(el: Element) -> Option<SystemType> {
    match el.element_type() {
        ElementType::EmbeddedDocument | ElementType::Array => {
            return Some(SystemType::Element(el));
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