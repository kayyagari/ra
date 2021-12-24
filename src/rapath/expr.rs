use std::fmt::{Display, Formatter};

use chrono::{DateTime, NaiveTime, Utc};

use crate::rapath::scanner::{Token, TokenType};
use crate::search::EvalError;
use crate::parser::ParseError;
use crate::rapath::scanner::TokenType::*;

pub trait Expr  {
    // fn eval(&self) -> Result<Box<dyn Expr>, EvalError>;
    // fn apply(&self, op: TokenType, rhs: &Box<dyn Expr>) -> Result<Box<dyn Expr>, EvalError> {
    //     Err(EvalError{msg: String::from("unsupported operation")})
    // }
    //
    // fn get_number(&self) -> f64 {
    //     panic!(String::from("unsupported operation"));
    // }

    fn to_string(&self) -> String {
        String::from("to be implemented")
    }

    fn is_leaf(&self) -> bool {
        false
    }
}

pub struct SystemBoolean {
    pub val: bool
}

pub struct SystemString {
    pub val: String
}

pub struct SystemInteger {
    pub val: i64
}

pub struct SystemDecimal {
    pub val: f64
}

pub struct SystemDateTime {
    pub val: DateTime<Utc>
}

pub struct SystemTime {
    pub val: NaiveTime
}

pub struct SystemConstant {
    pub val: String
}

pub struct SystemQuantity {
    pub val: f64,
    pub unit: String
}

pub struct Collection<T> {
    pub val: Vec<T>
}

pub struct PathExpr {
    pub val: String,
    pub function: Option<Box<dyn Expr>>
}

pub struct FunctionExpr {
    pub ctx_path: String,
    pub name: String,
    pub params: Vec<Box<dyn Expr>>
}

pub struct BinaryExpr {
    pub left: Box<dyn Expr>,
    pub right: Box<dyn Expr>,
    pub op: TokenType
}

impl<T> Collection<T> {
    pub fn new() -> Self {
        Collection{val: vec![]}
    }
}

impl SystemDecimal {
    pub fn new(n: f64) -> Self {
        SystemDecimal {val: n}
    }

    pub fn from(s: &String) -> Result<SystemDecimal, ParseError> {
        let n = s.parse::<f64>();
        if let Err(e) = n {
            return Err(ParseError{msg: format!("{}", e)});
        }
        Ok(SystemDecimal {val: n.unwrap()})
    }
}

impl Expr for SystemBoolean {
    // fn eval(&self) -> Result<Box<dyn Expr>, EvalError> {
    //     Ok(Box::new(Self{val: self.val}))
    // }
    //
    // fn apply(&self, op: TokenType, rhs: &Box<dyn Expr>) -> Result<Box<dyn Expr>, EvalError> {
    //     let mut result = false;
    //     match op {
    //         NOT_EQUAL => {
    //             result = !self.val;
    //         },
    //         EQUAL => {
    //             result = (self.val == rhs.val);
    //         }
    //     }
    //
    //     Ok(Box::new(SystemBoolean{val: result}))
    // }
    //
    // fn get_number(&self) -> f64 {
    //     todo!()
    // }

    fn to_string(&self) -> String {
        format!("{}", self.val)
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl Expr for SystemString {
    fn to_string(&self) -> String {
        format!("{}", self.val)
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl Expr for SystemInteger {
    fn to_string(&self) -> String {
        format!("{}", self.val)
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl<T> Expr for Collection<T> {
    fn to_string(&self) -> String {
        format!("{}", self.val.len())
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl Expr for SystemDecimal {
    // fn eval(&self) -> Result<Box<dyn Expr>, EvalError> {
    //     Ok(Box::new(Self{val: self.val}))
    // }
    //
    // fn apply(&self, op: TokenType, rhs: &Box<dyn Expr>) -> Result<Box<dyn Expr>, EvalError> {
    //     let result: Box<dyn Expr>;
    //     let other = rhs.get_number();
    //     match op {
    //         TokenType::PLUS => {
    //             result = Box::new(SystemDecimal {val: self.val + other});
    //         },
    //         _ => {
    //             return Err(EvalError{msg: String::from("unsupported operator")});
    //         }
    //     }
    //
    //     Ok(result)
    // }
    //
    // fn get_number(&self) -> f64 {
    //     self.val
    // }

    fn to_string(&self) -> String {
        format!("{}", self.val)
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl Expr for SystemDateTime {
    fn to_string(&self) -> String {
        format!("{}", self.val)
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl Expr for SystemTime {
    fn to_string(&self) -> String {
        format!("{}", self.val)
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl Expr for SystemQuantity {
    fn to_string(&self) -> String {
        format!("{}", self.val)
    }

    fn is_leaf(&self) -> bool {
        true
    }
}

impl Expr for BinaryExpr {
    // fn eval(&self) -> Result<Box<dyn Expr>, EvalError> {
    //     let left_leaf = self.left.is_leaf();
    //     let right_leaf = self.right.is_leaf();
    //
    //     if left_leaf && right_leaf {
    //         return self.left.apply(self.op, &self.right);
    //     }
    //     else if left_leaf {
    //         return self.left.apply(self.op, &self.right.eval()?);
    //     }
    //
    //     self.left.eval()?.apply(self.op, &self.right)
    // }
}

impl Expr for PathExpr {

}

impl Expr for FunctionExpr {

}

impl Expr for SystemConstant {

}