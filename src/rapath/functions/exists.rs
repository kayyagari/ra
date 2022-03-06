use std::borrow::Borrow;
use std::rc::Rc;
use crate::rapath::EvalResult;
use crate::rapath::expr::Ast;
use crate::rapath::functions::where_;
use crate::rapath::stypes::SystemType;

pub fn exists<'b>(mut base: Rc<SystemType<'b>>, args: &'b Vec<Ast<'b>>) -> EvalResult<'b> {
    if !args.is_empty() {
        base = where_(base, args)?
    }

    match base.borrow() {
        SystemType::Collection(c) => {
            Ok(Rc::new(SystemType::Boolean(!c.is_empty())))
        },
        _ => Ok(Rc::new(SystemType::Boolean(true)))
    }
}

#[cfg(test)]
mod tests {
    use bson::spec::ElementType;
    use rawbson::DocBuf;
    use rawbson::elem::Element;
    use crate::rapath::engine::eval;
    use crate::utils::test_utils::parse_expression;
    use super::*;

    #[test]
    fn test_exsts() {
        let bdoc = bson::doc!{"inner": [{"k": 1}, {"k": 1}, {"k": 2}, {"r": 7}] };
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));

        let mut exprs = Vec::new();
        exprs.push(("exists()", true));
        exprs.push(("exists(inner.r = 7)", true));
        exprs.push(("exists(inner.k != 1)", true));
        exprs.push(("exists(inner.k < 1)", false));
        for (input, expected) in exprs {
            let e = parse_expression(input);
            let result = eval(&e, Rc::clone(&doc_base)).unwrap();
            assert_eq!(expected, result.as_bool().unwrap());
        }
    }
}