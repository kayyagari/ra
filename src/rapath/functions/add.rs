use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::stypes::{SystemNumber, SystemType};

impl<'a> SystemType<'a> {
    pub fn add(&self, rhs: Rc<SystemType<'a>>) -> Result<Rc<SystemType<'a>>, EvalError> {
        match self {
            SystemType::String(s) => {
                let r = rhs.get_as_string().unwrap();
                let s = format!("{}{}", s, r);
                Ok(Rc::new(SystemType::String(s)))
            },
            SystemType::Number(n) => {
                let l = n.as_i64();
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
}