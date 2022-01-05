use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::stypes::SystemType;

mod scanner;
mod parser;
mod stypes;
mod expr;
mod engine;
mod functions;
mod element_utils;

pub type EvalResult<'a> = Result<Rc<SystemType<'a>>, EvalError>;
