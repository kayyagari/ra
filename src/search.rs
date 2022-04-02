use crate::res_schema::PropertyDef;
use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::dtypes::DataType;
use chrono::{DateTime, Utc};
use crate::errors::{ParseError, RaError};
use crate::search::filter_scanner::Token;

mod filter_scanner;
mod filter_parser;
pub mod executor;

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
pub struct TokenParam {
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
            _ => Err(RaError::SearchParamParsingError(format!("unknown search parameter type {}", name)))
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ComparisonOperator {
    AP, // A value in the set is approximately the same as this value.
        // Note that the recommended value for the approximation is 10% of
        // the stated value (or for a date, 10% of the gap between now and the date),
        // but systems may choose other values where appropriate
    CO, // An item in the set contains this value
    EB, // The value ends before the specified value
    EQ, // an item in the set has an equal value
    EW, // An item in the set ends with this value
    GE, // A value in the set is greater or equal to the given value
    GT, // A value in the set is greater than the given value
    IN, // True if one of the concepts is in the nominated value set by URI, either a relative, literal or logical vs
    LE, // A value in the set is less or equal to the given value
    LT, // A value in the set is less than the given value
    NE, // An item in the set has an unequal value
    NI, // True if none of the concepts are in the nominated value set by URI, either a relative, literal or logical vs
    PO, // True if a (implied) date period in the set overlaps with the implied period in the value
    PR, // The set is empty or not (value is false or true)
    RE, // True if one of the references in set points to the given URL
    SA, // The value starts after the specified value
    SB, // True if the value is subsumed by a concept in the set
    SS, // True if the value subsumes a concept in the set
    SW // An item in the set starts with this value
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FilterType {
    Simple,
    Conditional,
    And,
    Not,
    Or
}

#[derive(Debug)]
pub enum Filter {
    StringFilter {
        identifier: String,
        operator: ComparisonOperator,
        value: String
    },
    ConditionalFilter {
        identifier: String,
        id_path: String,
        operator: ComparisonOperator,
        value: String,
        condition: Box<Filter>
    },
    AndFilter {
        children: Vec<Box<Filter>>
    },
    OrFilter {
        children: Vec<Box<Filter>>
    },
    NotFilter {
        child: Box<Filter>
    }
}

impl Filter {
    fn get_type(&self) -> FilterType {
        use Filter::*;
        use FilterType::*;
        match self {
            StringFilter {..} => Simple,
            ConditionalFilter {..} => Conditional,
            AndFilter {..} => And,
            OrFilter {..} => Or,
            NotFilter {..} => Not
        }
    }

    fn to_string(&self) -> String {
        use Filter::*;
        match self {
            StringFilter {identifier, operator, value} => format!("({} {:?} {})", identifier, operator, value),
            ConditionalFilter {identifier, condition,
                     id_path, operator,
                     value} => format!("({}[{}]{} {:?} {})", identifier, condition.to_string(), id_path, operator, value),
            AndFilter {children} => {
                let mut s = String::from("(");
                let size = children.len() - 1;
                for (i, ch) in children.iter().enumerate() {
                    s.push_str(ch.to_string().as_str());
                    if size > 0 && i < size {
                        s.push_str(" AND ");
                    }
                }
                s.push_str(")");
                s
            },
            OrFilter {children} => {
                let mut s = String::from("(");
                let size = children.len() - 1;
                for (i, ch) in children.iter().enumerate() {
                    s.push_str(ch.to_string().as_str());
                    if size > 1 && i < size {
                        s.push_str(" OR ");
                    }
                }
                s.push_str(")");
                s
            },
            NotFilter {child} => format!("NOT{}", child.to_string())
        }
    }
}

pub fn parse_filter(filter: &str) -> Result<Filter, ParseError> {
    let mut tokens = filter_scanner::scan_tokens(filter)?;
    filter_parser::parse(tokens)
}