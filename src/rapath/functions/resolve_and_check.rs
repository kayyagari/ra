use std::borrow::Borrow;
use std::rc::Rc;
use bson::spec::ElementType;
use rawbson::elem::Element;
use crate::errors::EvalError;
use crate::rapath::engine::{eval, ExecContext};
use crate::rapath::{element_utils, EvalResult};
use crate::rapath::expr::Ast;
use crate::rapath::stypes::{Collection, SystemType};

pub fn resolve_and_check<'b>(ctx: &'b impl ExecContext<'b>, base: Rc<SystemType<'b>>, args: &'b Vec<Ast<'b>>) -> EvalResult<'b> {
    if base.is_empty() {
        return Ok(base);
    }

    let arg_len = args.len();
    if arg_len == 0 {
        return Err(EvalError::from_str("missing argument for resolve_and_check function"));
    }

    if arg_len > 1 {
        return Err(EvalError::new(format!("incorrect number of arguments passed to resolve_and_check function. Expected 1, found {}", arg_len)));
    }

    let type_name_result = eval(ctx, &args[0], Rc::clone(&base))?;
    let type_name;
    if let SystemType::String(t) = type_name_result.borrow() {
        type_name = t.as_str();
    }
    else {
        return Err(EvalError::new(format!("argument passed to resolve_and_check function returned value of type {} but a String is expected", type_name_result.get_type())));
    }

    match base.borrow() {
        SystemType::Collection(col) => {
            let mut c = Collection::new();
            for item in col.iter() {
                let target = check_target(ctx, Rc::clone(item), type_name)?;
                if !target.is_empty() {
                    c.push(target);
                }
            }
            Ok(Rc::new(SystemType::Collection(c)))
        },
        _ => check_target(ctx, Rc::clone(&base), type_name)
    }
}

fn check_target<'b>(ctx: &'b impl ExecContext<'b>, base: Rc<SystemType<'b>>, type_name: &str) -> EvalResult<'b> {
    match base.borrow() {
        SystemType::Element(el) => {
            let doc = el.as_document()?;
            let reference = doc.get_str("reference")?;
            if let Some(reference) = reference {
                let target = ctx.resolve(reference)?;
                let el = Element::new(ElementType::EmbeddedDocument, target.as_slice());
                let r = element_utils::is_resource_of_type(&el, type_name);
                return Ok(Rc::new(SystemType::Boolean(r)));
            }
            Ok(Rc::new(SystemType::Collection(Collection::new_empty())))
        },
        SystemType::String(s) => {
            let target = ctx.resolve(s.as_str())?;
            let el = Element::new(ElementType::EmbeddedDocument, target.as_slice());
            let r = element_utils::is_resource_of_type(&el, type_name);
            Ok(Rc::new(SystemType::Boolean(r)))
        },
        _ => Err(EvalError::new(format!("unsupported type {} received for resolving reference", base.get_type())))
    }
}

// this function is tested through the test_bundle_transaction in base.rs