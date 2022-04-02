use std::collections::HashMap;
use rocksdb::DBIterator;
use crate::barn::{Barn, CF_INDEX};
use crate::barn::index_scanners::string::StringIndexScanner;
use crate::errors::EvalError;
use crate::search::ComparisonOperator;

mod string;

pub type SelectedResourceKey = Result<Option<[u8; 24]>, EvalError>;

pub trait IndexScanner {
    fn next(&mut self) -> SelectedResourceKey;
    fn collect_all(&mut self) -> HashMap<[u8; 24], bool>;
}

impl Barn {
    pub fn new_string_index_scanner<'f, 'd: 'f>(&'d self, search_param_hash: &[u8], input: &'f [u8], op: &'f ComparisonOperator) -> StringIndexScanner<'f, 'd> {
        let cf = self.db.cf_handle(CF_INDEX).unwrap();
        let itr = self.db.prefix_iterator_cf(cf, search_param_hash);
        StringIndexScanner::new(input, itr, op)
    }
}
