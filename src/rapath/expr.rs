use std::fmt::{Display, Formatter, Write};

use chrono::{DateTime, NaiveTime, Utc};

use crate::rapath::scanner::{Token};
use crate::search::EvalError;
use crate::errors::ParseError;
use crate::rapath::scanner::Token::*;
use rawbson::elem::Element;
use std::rc::Rc;
use crate::rapath::stypes::SystemType;

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
        val: Rc<SystemType<'a>>
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
