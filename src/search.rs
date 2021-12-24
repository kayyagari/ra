use crate::res_schema::{PropertyDef};
use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::scanner::ComparisonOperator;
use crate::dtypes::DataType;
use chrono::{DateTime, Utc};

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

#[allow(non_camel_case_types)]
pub enum SearchParamType {
    NUMBER(f64),
    DATE(DateTime<Utc>),
    STRING(String),
    TOKEN(Token),
    REFERENCE(Reference),
    COMPOSITE(String), // still unclear on how to parse the composite value
    QUANTITY(Quantity),
    URI(Uri),
    SPECIAL
}

#[allow(non_camel_case_types)]
pub enum Modifier {
    TEXT,
    NOT,
    ABOVE,
    BELOW,
    IN,
    NOT_IN,
    OF_TYPE,
    MISSING,
    EXACT,
    CONTAINS,
    IDENTIFIER,
    RES_TYPE(String), // e.g :Patient used to define the type of reference (subject:Patient=<ID>)
    NONE
}

#[derive(Debug)]
pub struct FilterError {
    msg: String
}

#[derive(Debug)]
pub struct EvalError {
    pub msg: String
}

impl Error for FilterError{}
impl Error for EvalError{}

impl Display for FilterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.msg.as_str())
    }
}

impl Display for EvalError {
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
