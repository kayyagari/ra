use std::collections::HashMap;
use rocksdb::DBIterator;
use crate::search::{ComparisonOperator, Modifier};
use crate::search::index_scanners::IndexScanner;
use crate::utils::u32_from_le_bytes;

pub struct TokenIndexScanner<'f, 'd: 'f> {
    system: Option<&'f [u8]>,
    code: Option<&'f [u8]>,
    itr: DBIterator<'d>,
    index_prefix: &'f [u8],
    modifier: Modifier<'f>
}

impl<'f, 'd: 'f> TokenIndexScanner<'f, 'd> {
    pub fn new(input: &'f str, itr: DBIterator<'d>, index_prefix: &'f [u8], modifier: Modifier<'f>) -> Self {
        let mut system = None;
        let mut code = None;
        let mut parts = input.rsplitn(2, "|");
        if let Some(c) = parts.next() {
            if !c.is_empty() {
                code = Some(c.as_bytes());
            }
        }

        if let Some(s) = parts.next() {
            if !s.is_empty() {
                system = Some(s.as_bytes());
            }
        }

        TokenIndexScanner{system, code, itr, index_prefix, modifier}
    }

    fn compare(&self, stored_system: Option<&[u8]>, stored_code: Option<&[u8]>) -> bool {
        match self.modifier {
            Modifier::Text => {
                // TODO do a text search, expensive, for now it will return false
                false
            },
            Modifier::None => {
                let mut sys_match = false;
                if let Some(given_system) = self.system {
                    if let Some(stored_system) = stored_system {
                        sys_match = given_system == stored_system;
                    }
                }
                else {
                    sys_match = true;
                }

                let mut code_match = false;
                if let Some(given_code) = self.code {
                    if let Some(stored_code) = stored_code {
                        code_match = given_code == stored_code;
                    }
                }
                else {
                    code_match = true;
                }

                sys_match && code_match
            }
            _ => {
                false
            }
        }
    }
}

impl<'f, 'd: 'f> IndexScanner<'f> for TokenIndexScanner<'f, 'd> {
    fn collect_all(&mut self) -> HashMap<[u8; 24], bool> {
        let mut res_keys = HashMap::new();
        loop {
            let row = self.itr.next();
            if let None = row {
                break;
            }
            let row = row.unwrap();
            let row_prefix = &row.0[..4];
            if row_prefix != self.index_prefix {
                break;
            }

            let pos = row.0.len() - 24;
            let has_val = row.0[4] == 1;
            let mut stored_system = None;
            let mut stored_code = None;
            if has_val {
                let sys_len = u32_from_le_bytes(&row.0[5..9]);
                let code_len_start_pos = 9 + sys_len as usize;
                if sys_len > 0 {
                    stored_system = Some(&row.0[9..code_len_start_pos]);
                }
                let code_len_end_pos = code_len_start_pos +4;
                let code_len = u32_from_le_bytes(&row.0[code_len_start_pos..code_len_end_pos]) as usize;
                if code_len > 0 {
                    stored_code = Some(&row.0[code_len_end_pos..code_len_end_pos+code_len]);
                }
            }
            let r = self.compare( stored_system, stored_code);
            if r {
                let mut tmp: [u8; 24] = [0; 24];
                tmp.copy_from_slice(&row.0[pos..]);
                res_keys.insert(tmp, true);
            }
        }

        res_keys
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
    fn test_token_search() -> Result<(), Error> {
        let tc = TestContainer::new();
        let (db, sd) = tc.setup_db_with_example_patient()?;
        let mut candidates = vec![];
        candidates.push(("identifier eq \"urn:oid:1.2.36.146.595.217.0.1|12345\"", 1));
        candidates.push(("identifier eq \"|12345\"", 1));
        candidates.push(("identifier eq \"12345\"", 1));
        candidates.push(("identifier eq \"urn:oid:1.2.36.146.595.217.0.1|\"", 1));
        candidates.push(("identifier eq \"urn:oid:1.2.36.146.595.217.0.1\"", 0)); // absence of | makes the value be treated as code
        let rd = sd.resources.get("Patient").unwrap();

        for (input, expected) in candidates {
            println!("{}", input);
            let filter = search::parse_filter(input)?;
            let mut idx_scanner = to_index_scanner(&filter, &rd, &sd, &db)?;
            let key = idx_scanner.collect_all();
            assert_eq!(expected, key.len());
        }

        Ok(())
    }
}