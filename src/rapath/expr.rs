use std::fmt::{Display, Formatter, Write};

use chrono::{DateTime, NaiveTime, Utc};

use crate::rapath::scanner::{Token};
use crate::errors::{EvalError, ParseError};
use crate::rapath::scanner::Token::*;
use rawbson::elem::Element;
use std::rc::Rc;
use crate::rapath::stypes::SystemType;

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
        func: EvalFn<'a>,
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

pub type EvalFn<'a> = fn(base: &Rc<SystemType<'a>>, args: &'a Vec<Ast<'a>>) -> Result<Rc<SystemType<'a>>, EvalError>;

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

impl<'a> Display for Ast<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use self::Ast::*;
        let s = match self {
            Path{..}=> "Path",
            SubExpr{..} => "SubExpr",
            Binary{..} => "Binary",
            Function{..} => "Function",
            Index{..} => "Index",
            Literal{..} => "Literal",
            EnvVariable{..} => "EnvVariable"
        };

        f.write_str(s)
    }
}