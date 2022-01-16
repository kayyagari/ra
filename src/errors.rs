use std::error::Error;
use std::fmt::{Display, Formatter};
use rawbson::RawError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RaError {
    #[error("{0}")]
    DbError(String),
}

#[cfg(test)]
mod tests {
    use crate::errors::RaError;

    #[test]
    fn test_error() {
        let re = RaError::DbError(String::from("this is the message"));
        println!("{:?}", re);
    }
}

#[derive(Debug)]
pub struct ParseError {
    msg: String
}

impl Error for ParseError{}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.msg.as_str())
    }
}

impl ParseError {
    pub fn new(msg: String) -> Self {
        ParseError{msg}
    }

    pub fn from_str(msg: &str) -> Self {
        ParseError{msg: String::from(msg)}
    }
}

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

impl<'a> From<RawError> for EvalError {
    fn from(err: RawError) -> Self {
        EvalError::new(err.to_string())
    }
}

impl<'a> From<rawbson::de::Error> for EvalError {
    fn from(err: rawbson::de::Error) -> Self {
        EvalError::new(err.to_string())
    }
}

#[derive(Debug)]
pub struct ScanError {
    pub errors: Vec<String>
}

impl Error for ScanError {
    fn description(&self) -> &str {
        "filter parsing error"
    }
}

impl Display for ScanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for e in &self.errors {
            f.write_str(e.as_str())?;
        }
        Ok(())
    }
}
