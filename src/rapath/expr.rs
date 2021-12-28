use std::fmt::{Display, Formatter};

use chrono::{DateTime, NaiveTime, Utc};

use crate::rapath::scanner::{Token};
use crate::search::EvalError;
use crate::parser::ParseError;
use crate::rapath::scanner::Token::*;
use crate::rapath::expr::N::Decimal;
use rawbson::elem::Element;

#[derive(Debug)]
pub enum Ast<'a> {
    Path {
      name: String
    },
    SubExpr {
        lhs: Box<Ast<'a>>,
        rhs: Box<Ast<'a>>
    },
    Binary {
        lhs: Box<Ast<'a>>,
        op: Operator,
        rhs: Box<Ast<'a>>
    },
    Function {
        name: String,
        args: Vec<Ast<'a>>
    },
    Index {
        idx: u32
    },
    Literal {
        val: SystemType<'a>
    },
    EnvVariable {
        val: SystemType<'a>
    }
}

#[derive(Debug)]
pub enum Operator {
    Plus, Minus,
    Ampersand,
    Slash, Star,
    Equal, NotEqual,
    Equivalent, NotEquivalent,
    Greater, GreaterEqual,
    Less, LessEqual,
    In, Contains,
    And, Or, Xor,
    Union, Div, Mod, Is, As, Implies
}

#[derive(Debug)]
pub enum SystemType<'a> {
    Boolean(bool),
    String(String),
    Number(SystemNumber),
    DateTime(SystemDateTime),
    Time(SystemTime),
    Quantity(SystemQuantity),
    Element(Element<'a>),
    Collection(Collection<SystemType<'a>>)
}

#[derive(Debug)]
pub struct SystemNumber {
    val: N
}

#[derive(Debug)]
enum N {
    Integer(i64),
    UnsignedInt(u64),
    PositiveInt(u64),
    Decimal(f64)
}

#[derive(Debug)]
pub struct SystemDateTime {
    pub val: DateTime<Utc>
}

#[derive(Debug)]
pub struct SystemTime {
    pub val: NaiveTime
}

#[derive(Debug)]
pub struct SystemConstant {
    pub val: String
}

#[derive(Debug)]
pub struct SystemQuantity {
    pub val: f64,
    pub unit: String
}

#[derive(Debug)]
pub struct Collection<T> {
    pub val: Vec<T>
}

impl<T> Collection<T> {
    pub fn new() -> Self {
        Collection{val: vec![]}
    }
}

impl SystemNumber {
    pub fn new(n: f64) -> Self {
        SystemNumber {val: Decimal(n)}
    }

    pub fn from(s: &String) -> Result<SystemNumber, ParseError> {
        let n = s.parse::<f64>();
        if let Err(e) = n {
            return Err(ParseError{msg: format!("{}", e)});
        }
        let n = n.unwrap();
        Ok(SystemNumber {val: Decimal(n)})
    }

    pub fn to_negative_val(&mut self) {
        //self.val.negate();
    }
}
