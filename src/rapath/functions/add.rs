use std::rc::Rc;

use crate::errors::EvalError;
use crate::rapath::engine::EvalResult;
use crate::rapath::stypes::{SystemNumber, SystemString, SystemType};

impl<'a> SystemType<'a> {
    pub fn add(&self, rhs: &Rc<SystemType<'a>>) -> EvalResult<'a> {
        match self {
            SystemType::String(s) => {
                let r = rhs.as_string()?;
                let s = format!("{}{}", s.as_str(), r);
                Ok(Rc::new(SystemType::String(SystemString::new(s))))
            },
            SystemType::Number(n) => {
                let l = n.as_i64();
                let r = rhs.as_i64()?;
                let sd = l + r;
                let sd = SystemNumber::new_integer(sd);
                Ok(Rc::new(SystemType::Number(sd)))
            }
            st => {
                Err(EvalError::new(format!("addition is not supported on type {}", st.get_type())))
            }
        }
    }
}