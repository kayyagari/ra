use std::collections::HashMap;
use crate::search::ComparisonOperator;
use crate::search::index_scanners::{IndexScanner, SelectedResourceKey};

pub struct AndOrIndexScanner<'f> {
    and: bool,
    children: Vec<Box<dyn IndexScanner + 'f>>
}

impl<'f> AndOrIndexScanner<'f> {
    pub fn new_and(children: Vec<Box<dyn IndexScanner + 'f>>) -> Self {
        AndOrIndexScanner{and: true, children}
    }

    pub fn new_or(children: Vec<Box<dyn IndexScanner + 'f>>) -> Self {
        AndOrIndexScanner{and: false, children}
    }
}

impl<'f> IndexScanner for AndOrIndexScanner<'f> {
    fn next(&mut self) -> SelectedResourceKey {
        todo!()
    }

    fn collect_all(&mut self) -> HashMap<[u8; 24], bool> {
        let mut keys;
        if self.and {
            keys = HashMap::new();
            let mut min_idx = 0;
            let mut min_len: usize = usize::MAX;
            let total_len = self.children.len();
            let mut map_holder = Vec::with_capacity(total_len);
            for i in 0..total_len {
                let tmp = self.children[i].collect_all();
                let tmp_len = tmp.len();
                if tmp_len < min_len {
                    min_len = tmp_len;
                    min_idx = i;
                }
                map_holder.push(tmp);
            }

            let bar = map_holder.swap_remove(min_idx);
            let mut to_be_retained = HashMap::with_capacity(bar.len());
            for (k, v) in bar {
                let mut keep = true;
                for i in 0..total_len-1 {
                    if !map_holder[i].contains_key(&k) {
                        keep = false;
                        break;
                    }
                }
                to_be_retained.insert(k, v);
            }
            keys = to_be_retained;
        }
        else { // OR
            keys = self.children[0].collect_all();
            for i in 1..self.children.len() {
                let tmp = self.children[i].collect_all();
                for (k, v) in tmp {
                    keys.insert(k, v);
                }
            }
        }

        keys
    }
}