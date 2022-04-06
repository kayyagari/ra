use std::borrow::Borrow;
use std::rc::Rc;
use crate::errors::EvalError;
use crate::rapath::EvalResult;
use crate::rapath::expr::Ast;
use crate::rapath::functions::where_;
use crate::rapath::stypes::{Collection, SystemType};

pub fn array_index(base: Rc<SystemType>, index: usize) -> EvalResult {
    match base.borrow() {
        SystemType::Collection(c) => {
            let size = c.len();
            if size == 0 || index >= size {
                return Ok(Rc::new(SystemType::Collection(Collection::new_empty())));
            }
            Ok(c.get(index).unwrap())
        },
        t => Err(EvalError::new(format!("{} is not an array type", t.get_type())))
    }
}

#[cfg(test)]
mod tests {
    use bson::spec::ElementType;
    use rawbson::DocBuf;
    use rawbson::elem::Element;
    use crate::rapath::engine::{eval, ExecContext, UnresolvableExecContext};
    use crate::rapath::stypes::SystemNumber;
    use crate::utils::test_utils::parse_expression;
    use super::*;

    #[test]
    fn test_array_index() {
        let bdoc = bson::doc!{"inner": [{"k": 2}, {"r": 7, "sub": [0, 11]}] };
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));
        let ctx = UnresolvableExecContext::new(doc_base);

        let mut exprs = Vec::new();
        exprs.push(("inner[0].k", SystemType::Number(SystemNumber::new_integer(2))));
        exprs.push(("inner[1].r", SystemType::Number(SystemNumber::new_integer(7))));
        exprs.push(("inner[2]", SystemType::Collection(Collection::new_empty())));
        exprs.push(("inner[2].k", SystemType::Collection(Collection::new_empty())));
        exprs.push(("inner[1].sub[1]", SystemType::Number(SystemNumber::new_integer(11))));
        for (input, expected) in exprs {
            let e = parse_expression(input);
            let actual = eval(&ctx, &e, ctx.root_resource()).unwrap();
            let expected = Rc::new(expected);
            let result = SystemType::equals(Rc::clone(&expected), actual).unwrap();
            assert_eq!(expected.is_truthy(), result.is_truthy());
        }
    }
}