use std::borrow::Borrow;
use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::EvalResult;
use crate::rapath::stypes::{SystemQuantity, SystemType};

pub fn cast<'b>(mut st: Rc<SystemType<'b>>, type_name: &str) -> EvalResult<'b> {
    if st.is_empty() {
        return Ok(st);
    }
    if let SystemType::Collection(c) = st.borrow() {
        let tmp = c.get_if_singleton();
        if let None = tmp {
            return Err(EvalError::new(String::from("cannot cast multi-valued collection")));
        }
        st = tmp.unwrap();
    }

    match type_name {
        "Quantity" => {
            if let SystemType::Element(e) = st.borrow() {
                let doc = e.as_document()?;
                let val = doc.get("value")?;
                if let None = val {
                    return Err(EvalError::from_str("missing value attribute in Quantity element"));
                }
                let unit = doc.get("unit")?;
                if let None = unit {
                    return Err(EvalError::from_str("missing unit attribute in Quantity element"));
                }
                let val = val.unwrap().as_f64()?;
                let unit = unit.unwrap().as_str()?;
                // TODO this copying needs to be eliminated, but that requires refactoring scanner and parser
                let sq = SystemQuantity::new(val, String::from(unit));
                return Ok(Rc::new(SystemType::Quantity(sq)));
            }
            else {
                return Err(EvalError::new(format!("cannot convert {} to Quantity", st.get_type())));
            }
        },
        "DateTime" => {
            // TODO requires refactoring the date and time parsing functions present in scanner
            if let SystemType::String(e) = st.borrow() {
                todo!()
            }
        },

        _ => {}
    }

    Ok(st)
}