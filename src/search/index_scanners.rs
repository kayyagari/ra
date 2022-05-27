use std::collections::HashMap;
use rocksdb::DBIterator;
use crate::barn::{Barn, CF_INDEX};
use crate::search::index_scanners::string::StringIndexScanner;
use crate::errors::EvalError;
use crate::res_schema::SchemaDef;
use crate::search::ComparisonOperator;

pub mod string;
pub mod and_or;
pub mod not;
pub mod reference;

pub type SelectedResourceKey = Result<Option<[u8; 24]>, EvalError>;

pub trait IndexScanner<'f> {
    fn next(&mut self) -> SelectedResourceKey;
    fn collect_all(&mut self) -> HashMap<[u8; 24], bool>;
    fn chained_search(&mut self, res_pks: &mut HashMap<[u8; 24], [u8; 24]>, sd: &SchemaDef, db: &'f Barn) -> Result<HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>>, EvalError> {
        Ok(HashMap::new())
    }
}

pub type ChainedSearchCmpFunc = fn(idx_row_key: &[u8], ) -> Result<bool, EvalError>;