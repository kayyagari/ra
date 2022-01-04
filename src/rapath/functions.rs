mod add;
pub mod where_;

use std::collections::HashMap;
use std::rc::Rc;
use crate::rapath::engine::EvalResult;
use crate::rapath::expr::Ast;
use crate::rapath::stypes::SystemType;

// pub fn initFnDef<'a>() {
//     let FUNCTIONS1: HashMap<&'static str, FunctionDef<'static>> = {
//         let mut fns = HashMap::new();
//         fns.insert("where", FunctionDef(where_::where_));
//         fns
//     };
// }
