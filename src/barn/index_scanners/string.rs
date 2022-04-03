use std::collections::HashMap;
use log::warn;
use rocksdb::DBIterator;
use crate::barn::index_scanners::{IndexScanner, SelectedResourceKey};
use crate::search::ComparisonOperator;
use crate::search::ComparisonOperator::*;

pub struct StringIndexScanner<'f, 'd: 'f> {
    input: Vec<u8>,
    itr: DBIterator<'d>,
    index_prefix: &'f [u8],
    op: &'f ComparisonOperator,
    eof: bool
}

impl<'f, 'd: 'f> StringIndexScanner<'f, 'd> {
    pub fn new(input: &'f str, itr: DBIterator<'d>, op: &'f ComparisonOperator, index_prefix: &'f [u8]) -> Self {
        // this conversion to Vec<u8> is necessary to keep the parser schema free
        // and support the ":exact" modifier
        // note: a UTF-8 string will NOT always produce byte-arrays of same lengths for upper and lower cases
        let norm_val;
        if op == &EQ {
            norm_val = input.as_bytes().to_vec();
        }
        else {
            norm_val = input.to_lowercase().as_bytes().to_vec();
        }

        StringIndexScanner {input: norm_val, itr, op, index_prefix, eof: false}
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
                let row_prefix = &row.0[..4];
                if row_prefix != self.index_prefix {
                    self.eof = true;
                    break;
                }

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
             let row_prefix = &row.0[..4];
             if row_prefix != self.index_prefix {
                 self.eof = true;
                 break;
             }

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
        let input = self.input.as_slice();
        match self.op {
            CO => {
                if let Some(k) = k {
                    result = contains_slice(k, input);
                }
            },
            EQ => {
                if k.is_some() {
                    if input == v {
                        result = true;
                    }
                }
            },
            EW => {
                if let Some(k) = k {
                    result = k.ends_with(input);
                }
            },
            GE => {
                if let Some(k) = k {
                    result = k >= input;
                }
            },
            GT => {
                if let Some(k) = k {
                    result = k > input;
                }
            },
            LE => {
                if let Some(k) = k {
                    result = k <= input;
                }
            },
            LT => {
                if let Some(k) = k {
                    result = k < input;
                    //println!("{} < {} = {}", String::from_utf8(k.to_vec()).unwrap(), String::from_utf8(input.to_vec()).unwrap(), result);
                }
            },
            NE => {
                if k.is_some() {
                    if input != v {
                        result = true;
                    }
                }
                else {
                    result = true;
                }
            },
            SW => {
                if let Some(k) = k {
                    result = k.starts_with(input);
                }
            },
            _ => {
                warn!("{:?} operator is not supported on strings", self.op);
            }
        }

        result
    }
}

fn contains_slice(src: &[u8], item: &[u8]) -> bool {
    let sub_slice_len = item.len();
    if sub_slice_len != 0 && sub_slice_len < src.len() {
        let windows = src.windows(sub_slice_len);
        for w in windows {
            if w == item {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use crate::search;
    use crate::search::executor::to_index_scanner;
    use crate::utils::test_utils::TestContainer;
    use super::*;

    #[test]
    fn test_string_search_using_scanner() -> Result<(), Error> {
        let tc = TestContainer::new();
        let (db, sd) = tc.setup_db_with_example_patient()?;
        let mut candidates = vec![];
        candidates.push(("name eq \"James\"", 1));
        candidates.push(("name eq \"james\"", 0));
        candidates.push(("name sw \"Jam\"", 1));
        candidates.push(("name sw \"jAM\"", 1));
        candidates.push(("name ew \"es\"", 1));
        candidates.push(("name ew \"ES\"", 1));
        candidates.push(("name co \"et\"", 1));
        candidates.push(("name co \"ET\"", 1));
        candidates.push(("family ge \"Windsor\"", 1));
        candidates.push(("family lt \"ChalMers\"", 0));
        candidates.push(("family le \"chalmers\"", 1));
        candidates.push(("family ne \"chalmers\"", 1));
        candidates.push(("family gt \"windsor\"", 0));
        candidates.push(("family gt \"Windsor\"", 0));

        for (input, expected) in candidates {
            println!("{}", input);
            let filter = search::parse_filter(input)?;
            let rd = sd.resources.get("Patient").unwrap();
            let mut idx_scanner = to_index_scanner(&filter, &rd, &sd, &db)?;
            let key = idx_scanner.collect_all();
            assert_eq!(expected, key.len());
        }
        Ok(())
    }
}