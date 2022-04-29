use std::collections::HashMap;
use rocksdb::DBIterator;
use crate::barn::{Barn, CF_INDEX};
use crate::search::index_scanners::string::StringIndexScanner;
use crate::errors::EvalError;
use crate::search::ComparisonOperator;

pub mod string;
pub mod and_or;
pub mod not;
pub mod reference;

pub type SelectedResourceKey = Result<Option<[u8; 24]>, EvalError>;

pub trait IndexScanner {
    fn next(&mut self) -> SelectedResourceKey;
    fn collect_all(&mut self) -> HashMap<[u8; 24], bool>;
}
