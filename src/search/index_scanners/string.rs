use std::collections::HashMap;
use log::warn;
use rocket::form::validate::Contains;
use rocksdb::DBIterator;
use crate::barn::Barn;
use crate::errors::EvalError;
use crate::res_schema::SchemaDef;
use crate::search::index_scanners::{IndexScanner, SelectedResourceKey};
use crate::search::{ComparisonOperator, Modifier};
use crate::search::ComparisonOperator::*;

pub struct StringIndexScanner<'f, 'd: 'f> {
    value: Vec<u8>,
    itr: DBIterator<'d>,
    index_prefix: &'f [u8],
    op: &'f ComparisonOperator,
    eof: bool,
    modifier: Modifier<'f>,
    values: Vec<Vec<u8>>
}

impl<'f, 'd: 'f> StringIndexScanner<'f, 'd> {
    pub fn new(input: &'f str, itr: DBIterator<'d>, mut op: &'f ComparisonOperator, index_prefix: &'f [u8], modifier: Modifier<'f>) -> Self {
        // conversion to Vec<u8> is necessary to keep the parser schema free
        // and support the ":exact" modifier
        // note: a UTF-8 string will NOT always produce byte-arrays of same lengths for upper and lower cases
        let mut values = Vec::new();
        if input.contains(',') {
            let tmp = split_delimited_values(input, ',');
            for s in tmp {
                values.push(normalize(&s, &modifier));
            }
        }

        let mut norm_val= Vec::new();
        if values.len() == 1 {
            norm_val = values.swap_remove(0);
        }
        else if values.is_empty() {
            norm_val = normalize(input, &modifier);
        }
        else {
            op = &ComparisonOperator::IN;
        }

        StringIndexScanner { value: norm_val, itr, op, index_prefix, eof: false, modifier, values}
    }
}

impl<'f, 'd: 'f> IndexScanner<'f> for StringIndexScanner<'f, 'd> {
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

    fn chained_search(&mut self, res_pks: &mut HashMap<[u8; 24], [u8; 24]>, sd: &SchemaDef, db: &'f Barn) -> Result<HashMap<[u8; 4], HashMap<[u8; 24], [u8; 24]>>, EvalError> {
        let mut keys: HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>> = HashMap::new();
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
            if res_pks.is_empty() {
                break;
            }
            let pos = row.0.len() - 24;
            let this_pk = &row.0[pos..];
            let ref_to_res_pk = res_pks.remove(this_pk);
            if let Some(ref_to_res_pk) = ref_to_res_pk {
                let hasVal = row.0[4] == 1;
                let mut norm_val_in_key = None;
                if hasVal {
                    norm_val_in_key = Some(&row.0[5..pos]);
                }
                let r = self.cmp_value( norm_val_in_key, row.1.as_ref());
                if r {
                    let this_res_type= &row.0[pos..pos+4];
                    if !keys.contains_key(this_res_type) {
                        let this_res_type_sized = this_res_type.try_into().unwrap();
                        keys.insert(this_res_type_sized, HashMap::new());
                    }
                    let this_pk_sized = this_pk.try_into().unwrap();
                    keys.get_mut(this_res_type).unwrap().insert(this_pk_sized, ref_to_res_pk);
                }
            }
        }

        Ok(keys)
    }
}

impl<'f, 'd: 'f> StringIndexScanner<'f, 'd> {
    fn cmp_value(&mut self, k: Option<&[u8]>, v: &[u8]) -> bool {
        let mut result = false;
        let input = self.value.as_slice();
        match self.op {
            CO => {
                if let Some(k) = k {
                    result = contains_slice(k, input);
                }
            },
            EQ => {
                if let Some(k) = k {
                    if self.modifier == Modifier::Exact {
                        result = input == v;
                    }
                    else {
                        result = input == k;
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
            IN => {
                if let Some(k) = k {
                    for given in &self.values {
                        if self.modifier == Modifier::Exact {
                            result = given.as_slice() == v;
                        }
                        else {
                            result = given.as_slice() == k;
                        }

                        if result {
                            break;
                        }
                    }
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

fn split_delimited_values(value: &str, needle: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut ci = value.char_indices();
    let mut prev = ' ';
    let mut part = String::new();
    loop {
        match ci.next() {
            Some((pos, c)) => {
                if c == needle && prev != '\\' {
                    if !part.is_empty() {
                        parts.push(part.clone());
                        part.clear();
                    }
                }
                else if c == '\\' && prev != '\\' {
                    // skip pushing escape secquence
                }
                else {
                    part.push(c);
                }
                prev = c;
            },
            None => {
                if !part.is_empty() {
                    parts.push(part);
                }
                break;
            }
        }
    }

    parts
}

fn normalize(value: &str, modifier: &Modifier) -> Vec<u8> {
    if modifier == &Modifier::Exact {
        return value.as_bytes().to_vec();
    }

    value.to_lowercase().as_bytes().to_vec()
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
        candidates.push(("name eq \"Ai9\"", 1)); // a value formed after applying NFKD on unicode string "Åi₉"
        candidates.push(("name eq \"james\"", 1));
        candidates.push(("name:exact eq \"james\"", 0));
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
        candidates.push(("name eq \"not-James,james\"", 1));
        candidates.push(("name:exact eq \"not-James,james\"", 0));

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

    #[test]
    fn test_split_delimited_values() {
        let mut candidates = Vec::new();
        candidates.push(("a,b\\,c", ',', vec!["a", "b,c"]));
        candidates.push(("a,b\\,c,", ',', vec!["a", "b,c"]));
        candidates.push(("a$b\\$c", '$', vec!["a", "b$c"]));
        candidates.push(("a$b\\$c$", '$', vec!["a", "b$c"]));
        candidates.push(("a|b\\|c", '|', vec!["a", "b|c"]));
        candidates.push(("a|b\\|c|", '|', vec!["a", "b|c"]));
        candidates.push(("a|b\\|c||", '|', vec!["a", "b|c"]));
        for (input, delimiter, expected) in candidates {
            let actual = split_delimited_values(input, delimiter);
            assert_eq!(expected, actual);
        }
    }
}