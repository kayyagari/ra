use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::stypes::SystemType;

pub mod scanner;
pub mod parser;
pub mod stypes;
pub mod expr;
pub mod engine;
mod functions;
mod element_utils;
mod operations;

pub type EvalResult<'a> = Result<Rc<SystemType<'a>>, EvalError>;
