use crate::res_schema::{PropertyDef};
use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::filter_scanner::ComparisonOperator;
use crate::dtypes::DataType;
use chrono::{DateTime, Utc};
use crate::errors::RaError;

pub struct SearchExpr {
    name: String,
    attribute: &'static PropertyDef,
    op: ComparisonOperator,
    val: DataType,
    modifier: Modifier
}

#[derive(Debug)]
pub struct Reference {
    res_type: String,
    id: Option<String>,
    url: Option<String>,
    version: Option<u64>
}

#[derive(Debug)]
pub struct Quantity {
    number: String,
    system: Option<String>,
    code: Option<String>
}

#[derive(Debug)]
pub struct Token {
    system: Option<String>,
    code: Option<String>
}

#[derive(Debug)]
pub struct Uri {
    value: String,
    is_url: bool
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SearchParamType {
    Number,
    Date,
    String,
    Token,
    Reference,
    Composite,
    Quantity,
    Uri,
    Special
}

impl SearchParamType {
    pub fn from(name: &str) -> Result<SearchParamType, RaError> {
        use SearchParamType::*;
        match name {
            "number" => Ok(Number),
            "date" => Ok(Date),
            "string" => Ok(String),
            "token" => Ok(Token),
            "reference" => Ok(Reference),
            "composite" => Ok(Composite),
            "quantity" => Ok(Quantity),
            "uri" => Ok(Uri),
            "special" => Ok(Special),
            _ => Err(RaError::SchemaParsingError(format!("unknown search parameter type {}", name)))
        }
    }
}

#[allow(non_camel_case_types)]
pub enum Modifier {
    Text,
    Not,
    Above,
    Below,
    In,
    NotIn,
    OfType,
    Missing,
    Exact,
    Contains,
    Identifier,
    ResType(String), // e.g :patient used to define the type of reference (subject:patient=<id>)
    None
}

#[derive(Debug)]
pub struct FilterError {
    msg: String
}

impl Error for FilterError{}

impl Display for FilterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.msg.as_str())
    }
}

impl SearchExpr {
    // pub fn new(name: String, rdef: &ResourceDef) -> Result<SearchExpr, FilterError> {
    //
    // }

    // pub fn evaluate(&self, el: Option<Element>) -> Result<bool, EvalError> {
    //
    //     match self.attribute.dtype {
    //         DataType::INTEGER => {
    //             // el.unwrap().as_i64()
    //         }
    //     }
    //     true
    // }
}
