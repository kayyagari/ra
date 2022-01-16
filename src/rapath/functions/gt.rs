use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::EvalResult;
use crate::rapath::stypes::{SystemNumber, SystemString, SystemType};

impl<'a> SystemType<'a> {
    pub fn gt(&self, rhs: &Rc<SystemType<'a>>) -> EvalResult<'a> {
        match self {
            SystemType::String(s) => {
                let r = rhs.as_string()?;
                let b = s.as_str().as_bytes().gt(r.as_bytes());
                Ok(Rc::new(SystemType::Boolean(b)))
            },
            SystemType::Number(n) => {
                let l = n.as_i64();
                let r = rhs.as_i64()?;
                let b = l > r;
                Ok(Rc::new(SystemType::Boolean(b)))
            },
            st => {
                Err(EvalError::new(format!("greaterthan is not supported on type {}", st.get_type())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use crate::rapath::engine::eval;
    use crate::rapath::parser::parse;
    use crate::rapath::scanner::scan_tokens;
    use crate::rapath::stypes::{SystemNumber, SystemTypeType};
    use crate::rapath::stypes::SystemType;

    #[test]
    fn test_greaterthan() {
        let tokens = scan_tokens("'abc' > 'xyz'").unwrap();
        let e = parse(tokens).unwrap();
        let dummy_base = SystemType::Boolean(true);
        let result = eval(&e, Rc::new(dummy_base));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(SystemTypeType::Boolean, result.get_type());
        assert_eq!(SystemType::Boolean(false), *result);
    }
}