use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::EvalResult;
use crate::rapath::stypes::{SystemNumber, SystemString, SystemType};

impl<'a> SystemType<'a> {
    pub fn gt(&self, rhs: &Rc<SystemType<'a>>) -> EvalResult<'a> {
        match self {
            SystemType::String(s) => {
                let r = rhs.as_string()?;
                let b = s.as_str() > r;
                Ok(Rc::new(SystemType::Boolean(b)))
            },
            SystemType::Number(n) => {
                let l = n.as_i64();
                let r = rhs.as_i64()?;
                let b = l > r;
                Ok(Rc::new(SystemType::Boolean(b)))
            },
            st => {
                Err(EvalError::new(format!("greatethan is not supported on type {}", st.get_type())))
            }
        }
    }
}
