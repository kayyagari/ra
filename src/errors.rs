use std::error::Error;
use std::fmt::{Display, Formatter};
use bson::document::ValueAccessError;
use rawbson::RawError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RaError {
    #[error("{0}")]
    DbError(String),
    #[error("{0}")]
    InvalidValueError(String),
    // #[error("{0}")]
    // SystemError(String),
    #[error("{0}")]
    SchemaParsingError(String),
    #[error("")]
    SchemaValidationError
}

impl RaError {
    pub fn invalid_err<S: AsRef<str>>(msg: S) -> Self {
        Self::InvalidValueError(String::from(msg.as_ref()))
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
    // TODO change to accept &str and String then remove from_str()
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

impl From<ValueAccessError> for RaError {
    fn from(e: ValueAccessError) -> Self {
        match e {
            ValueAccessError::NotPresent => RaError::invalid_err("missing attribute"),
            ValueAccessError::UnexpectedType => RaError::invalid_err("invalid conversion attempt on attribute value"),
            _ => RaError::invalid_err(e.to_string())
        }
    }
}

impl From<rocksdb::Error> for RaError {
    fn from(e: rocksdb::Error) -> Self {
        RaError::DbError(e.to_string())
    }
}

impl From<bson::ser::Error> for RaError {
    fn from(e: bson::ser::Error) -> Self {
        RaError::invalid_err(e.to_string())
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

#[cfg(test)]
mod tests {
    use crate::errors::RaError;

    #[test]
    fn test_error() {
        let re = RaError::DbError(String::from("this is the message"));
        println!("{:?}", re);
    }
}
