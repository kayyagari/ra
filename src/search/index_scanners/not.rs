use std::collections::HashMap;
use crate::barn::Barn;
use crate::ResourceDef;
use crate::search::index_scanners::{IndexScanner, SelectedResourceKey};

pub struct NotIndexScanner<'f> {
    child: Box<dyn IndexScanner<'f> + 'f>,
    db : &'f Barn,
    rd: &'f ResourceDef
}

impl<'f> NotIndexScanner<'f> {
    pub fn new(child: Box<dyn IndexScanner<'f> + 'f>, rd: &'f ResourceDef, db : &'f Barn) -> Self {
        NotIndexScanner{child, rd, db}
    }
}

impl<'f> IndexScanner<'f> for NotIndexScanner<'f> {
    fn collect_all(&mut self) -> HashMap<[u8; 24], bool> {
        todo!()
    }
}