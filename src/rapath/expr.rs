use std::fmt::{Display, format, Formatter, Write};

use chrono::{DateTime, NaiveTime, Utc};

use crate::rapath::scanner::{Token};
use crate::errors::{EvalError, ParseError};
use crate::rapath::scanner::Token::*;
use rawbson::elem::Element;
use std::rc::Rc;
use crate::rapath::EvalResult;
use crate::rapath::functions::*;
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
        // name: String,
        func: Function<'a>,
        // func: EvalFn<'a>,
        // func: Box<dyn EvalFunc<'a>>,
        // args: Rc<Vec<Ast<'a>>>
    },
    ArrayIndex {
        left: Box<Ast<'a>>, // not calling it LHS
        index: usize
    },
    Literal {
        val: Rc<SystemType<'a>>
    },
    Variable {
        name: String
    },
    EnvVariable {
        name: String
    },
    TypeCast {
        at_name: String, // e.g value
        at_and_type_name: String, // e.g valueCodeableConcept
        type_name: String // e.g CodeableConcept
    }
}

// TODO use a function pointer instead of using the Function enum
//pub type EvalFn<'b> = fn(base: Rc<SystemType<'b>>, args: &'b Rc<Vec<Ast<'b>>>) -> EvalResult<'b>;
pub type CmpFunc<'b> = fn(lhs: Rc<SystemType<'b>>, rhs: Rc<SystemType<'b>>, op: &Operator) -> EvalResult<'b>;

// this enum exists because I couldn't get to make EvalFn<'b> work
// the error was "but data from `base` flows into `ast` here" in engine.rs in the match arm of Ast::Function
// it would be nice to get the function pointer based code work
pub enum Function<'a> {
    NameAndArgs(String, Vec<Ast<'a>>)
}

impl<'a, 'b> Function<'a> where 'a: 'b {
    pub fn eval_func(&'a self, base: Rc<SystemType<'b>>) -> EvalResult<'b> {
        match self {
            Function::NameAndArgs(name, args) => {
                match name.as_str() {
                    "where" => {
                        where_(base, args)
                    },
                    "empty" => {
                        empty(base, args)
                    },
                    "exists" => {
                        exists(base, args)
                    },
                    _ => {
                        Err(EvalError::new(format!("unknown function name {}", name)))
                    }
                }
            }
        }
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

impl<'a, 'b> Display for Ast<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use self::Ast::*;
        let s = match self {
            Path{..}=> "Path",
            SubExpr{..} => "SubExpr",
            Binary{..} => "Binary",
            Function{..} => "Function",
            ArrayIndex {..} => "ArrayIndex",
            Literal{..} => "Literal",
            EnvVariable{..} => "EnvVariable",
            TypeCast {..} => "TypeCast",
            Variable {..} => "Variable"
        };

        f.write_str(s)
    }
}