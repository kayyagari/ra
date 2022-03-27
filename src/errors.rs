use std::error::Error;
use std::fmt::{Display, Formatter};

use bson::document::ValueAccessError;
use rawbson::RawError;
use serde::{Serialize, Deserialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RaError {
    #[error("{0}")]
    DbError(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    // #[error("{0}")]
    // SystemError(String),
    #[error("{0}")]
    SchemaParsingError(String),
    #[error("{0}")]
    SearchParamParsingError(String),
    #[error("")]
    SchemaValidationError
}

impl RaError {
    pub fn bad_req<S: AsRef<str>>(msg: S) -> Self {
        Self::BadRequest(String::from(msg.as_ref()))
    }

    fn to_string(&self) -> String {
        use RaError::*;
        let s = match self {
            DbError(s) => s.as_str(),
            BadRequest(s) => s.as_str(),
            NotFound(s) => s.as_str(),
            SchemaParsingError(s) => s.as_str(),
            SearchParamParsingError(s) => s.as_str(),
            SchemaValidationError => "schema validation error"
        };

        String::from(s)
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

#[derive(Debug)]
pub struct ScanError {
    pub errors: Vec<String>
}

impl Error for ScanError {
    fn description(&self) -> &str {
        "filter parsing error"
    }
}

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
            ValueAccessError::NotPresent => RaError::bad_req("missing attribute"),
            ValueAccessError::UnexpectedType => RaError::bad_req("invalid conversion attempt on attribute value"),
            _ => RaError::bad_req(e.to_string())
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
        RaError::bad_req(e.to_string())
    }
}

impl From<ScanError> for RaError {
    fn from(e: ScanError) -> Self {
        RaError::bad_req(e.to_string())
    }
}

impl From<ParseError> for RaError {
    fn from(e: ParseError) -> Self {
        RaError::bad_req(e.to_string())
    }
}

impl From<EvalError> for RaError {
    fn from(e: EvalError) -> Self {
        RaError::bad_req(e.to_string())
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IssueSeverity {
    Fatal,
    Error,
    Warning,
    Information
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[allow(non_camel_case_types)]
#[rustfmt::skip]
pub enum IssueType {
    Invalid,
        Structure, Required, Value, Invariant,
    Security,
        Login, Unknown, Expired, Forbidden, Suppressed,
    Processing,
        Not_supported, Duplicate, Multiple_matches, Not_found, Deleted,
        Too_long, Code_invalid, Extension, Too_costly, Business_rule, Conflict,
    Transient,
        Lock_error, No_store, Exception, Timeout, Incomplete, Throttled,
    Informational
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
