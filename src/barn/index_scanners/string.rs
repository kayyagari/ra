use std::collections::HashMap;
use log::warn;
use rocksdb::DBIterator;
use crate::barn::index_scanners::{IndexScanner, SelectedResourceKey};
use crate::search::ComparisonOperator;
use crate::search::ComparisonOperator::*;

pub struct StringIndexScanner<'f, 'd: 'f> {
    input: &'f [u8],
    itr: DBIterator<'d>,
    op: &'f ComparisonOperator,
    eof: bool
}

impl<'f, 'd: 'f> StringIndexScanner<'f, 'd> {
    pub fn new(input: &'f [u8], itr: DBIterator<'d>, op: &'f ComparisonOperator) -> Self {
        StringIndexScanner {input, itr, op, eof: false}
    }
}

impl<'f, 'd: 'f> IndexScanner for StringIndexScanner<'f, 'd> {
    fn next(&mut self) -> SelectedResourceKey {
        let mut res_key = None;

        if !self.eof {
            loop {
                let row = self.itr.next();
                if let None = row {
                    break;
                }
                let row = row.unwrap();
                let pos = row.0.len() - 24;
                let hasVal = row.0[4] == 1;
                let mut norm_val_in_key = None;
                if hasVal {
                    norm_val_in_key = Some(&row.0[5..pos]);
                }
                let r = self.cmp_value(norm_val_in_key, row.1.as_ref());
                if r {
                    let mut tmp: [u8; 24] = [0; 24];
                    tmp.copy_from_slice(&row.0[pos..]);
                    res_key = Some(tmp);
                    break;
                }
                if self.eof {
                    break;
                }
            }
        }

        Ok(res_key)
    }

    fn collect_all(&mut self) -> HashMap<[u8; 24], bool> {
        let mut res_keys = HashMap::new();
        if self.eof {
            return res_keys;
        }

         loop {
             let row = self.itr.next();
             if let None = row {
                 self.eof = true;
                 break;
             }
             let row = row.unwrap();
             let pos = row.0.len() - 24;
             let hasVal = row.0[4] == 1;
             let mut norm_val_in_key = None;
             if hasVal {
                 norm_val_in_key = Some(&row.0[5..pos]);
             }
             let r = self.cmp_value( norm_val_in_key, row.1.as_ref());
             if r {
                 let mut tmp: [u8; 24] = [0; 24];
                 tmp.copy_from_slice(&row.0[pos..]);
                 res_keys.insert(tmp, true);
             }
             if self.eof {
                 break;
             }
        }

        res_keys
    }
}

impl<'f, 'd: 'f> StringIndexScanner<'f, 'd> {
    fn cmp_value(&mut self, k: Option<&[u8]>, v: &[u8]) -> bool {
        let mut result = false;
        match self.op {
            AP => {
                warn!("{:?} operation is not supported on string search", self.op);
            },
            CO => {
                if let Some(k) = k {
                    // TODO add a function to search for a sub-slice
                    //result = k.contains_slice(self.input);
                }
            },
            EB => {},
            EQ => {
                if k.is_some() {
                    if self.input == v {
                        result = true;
                    }
                }
            },
            EW => {},
            GE => {},
            GT => {},
            IN => {},
            LE => {},
            LT => {},
            NE => {
                if k.is_some() {
                    if self.input != v {
                        result = true;
                    }
                }
                else {
                    result = true;
                }
            },
            NI => {},
            PO => {},
            PR => {},
            RE => {},
            SA => {},
            SB => {},
            SS => {},
            SW => {}
            // _ => {
            //     panic!("{:?} operation is not supported yet for filtering", self.op);
            // }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use crate::search;
    use crate::search::executor::to_index_scanner;
    use crate::utils::test_utils::TestContainer;
    use super::*;

    #[test]
    fn test_scan_equality() -> Result<(), Error> {
        let tc = TestContainer::new();
        let (db, sd) = tc.setup_db_with_example_patient()?;
        let filter = search::parse_filter("name eq \"James\"")?;
        let rd = sd.resources.get("Patient").unwrap();
        let mut idx_scanner = to_index_scanner(&filter, &rd, &sd, &db)?;
        let key = idx_scanner.collect_all();
        assert_eq!(1, key.len());
        Ok(())
    }
}