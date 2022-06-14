use std::borrow::Borrow;
use std::rc::Rc;
use serde_json::Value;
use crate::errors::EvalError;
use crate::rapath::EvalResult;
use crate::rapath::stypes::{Collection, SystemQuantity, SystemType};

pub fn cast<'b>(mut st: Rc<SystemType<'b>>, type_name: &str) -> EvalResult<'b> {
    if st.is_empty() {
        return Ok(st);
    }
    if let SystemType::Collection(c) = st.borrow() {
        if c.len() == 1 {
            st = convert(c.get(0).unwrap(), type_name)?;
        }
        else {
            let mut tmp = Collection::new();
            for item in c.iter() {
                let converted_val = convert(Rc::clone(item), type_name)?;
                tmp.push(converted_val);
            }
            st = Rc::new(SystemType::Collection(tmp));
        }
    }
    else {
        st = convert(st, type_name)?;
    }

    Ok(st)
}

fn convert<'b>(st: Rc<SystemType<'b>>, type_name: &str) -> EvalResult<'b> {
    match type_name {
        "Quantity" => {
            if let SystemType::Element(e) = st.borrow() {
                let doc = e.as_document()?;
                let val = doc.get("value")?;
                if let None = val {
                    return Err(EvalError::from_str("missing value attribute in Quantity element"));
                }
                let unit = doc.get("unit")?;
                if let None = unit {
                    return Err(EvalError::from_str("missing unit attribute in Quantity element"));
                }
                let val = val.unwrap().as_f64()?;
                let unit = unit.unwrap().as_str()?;
                // TODO this copying needs to be eliminated, but that requires refactoring scanner and parser
                let sq = SystemQuantity::new(val, String::from(unit));
                return Ok(Rc::new(SystemType::Quantity(sq)));
            }
            else {
                return Err(EvalError::new(format!("cannot convert {} to Quantity", st.get_type())));
            }
        },
        "DateTime" => {
            // TODO requires refactoring the date and time parsing functions present in scanner
            if let SystemType::String(e) = st.borrow() {
                todo!()
            }
        },

        _ => {}
    }
    Ok(st)
}

#[cfg(test)]
mod tests {
    use bson::spec::ElementType;
    use rawbson::DocBuf;
    use rawbson::elem::Element;
    use crate::rapath::engine::{eval, ExecContext, UnresolvableExecContext};
    use crate::utils::test_utils::parse_expression;
    use super::*;

    #[test]
    fn test_as() {
        let bdoc = bson::doc! {"value": {"value": 161.42333812930528,
          "unit": "cm",
          "system": "http://unitsofmeasure.org",
          "code": "cm"}, "codeQuantity": {
          "value": 41.76996932711261,
          "unit": "kg",
          "system": "http://unitsofmeasure.org",
          "code": "kg"}, "name": "k"};
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));
        let ctx = UnresolvableExecContext::new(doc_base);

        let mut exprs = Vec::new();
        exprs.push(("value as Quantity", true)); // attribute "value" exists
        exprs.push(("code as Quantity", true)); // attribute "code" doesn't exist but "codeQuantity" does
        exprs.push(("value as Quantity > 1 'cm'", true));
        exprs.push(("value as Quantity > 1 'cm' and 0 = 0", true)); // just to check if the parser is doing it right or not
        exprs.push(("code as Quantity < 1 'kg'", false));
        exprs.push(("code as Quantity < 1 'kg' and 1 = 1", false)); // just to check if the parser is doing it right or not

        for (input, expected) in exprs {
            let e = parse_expression(input);
            let result = eval(&ctx, &e, ctx.root_resource()).unwrap();
            assert_eq!(expected, result.is_truthy());
        }
    }

    #[test]
    fn test_as_with_collection() {
        let bdoc = bson::doc! {"resourceType":"Observation","id":"fc6fce3e-cb07-44ad-9a94-912b0947dddc","status":"final","category":[{"coding":[{"system":"http://terminology.hl7.org/CodeSystem/observation-category","code":"vital-signs","display":"vital-signs"}]}],"code":{"coding":[{"system":"http://loinc.org","code":"55284-4","display":"Blood Pressure"}],"text":"Blood Pressure"},"subject":{"reference":"urn:uuid:c2b40f49-4c27-419b-ad34-5c7bcc07781a"},"encounter":{"reference":"urn:uuid:23c284fd-009d-4ac7-a68a-958c0f5308d5"},"effectiveDateTime":"2010-09-08T00:56:05-04:00","issued":"2010-09-08T00:56:05.925-04:00","component":[{"code":{"coding":[{"system":"http://loinc.org","code":"8462-4","display":"Diastolic Blood Pressure"}],"text":"Diastolic Blood Pressure"},"valueQuantity":{"value":83.99636147115997,"unit":"mm[Hg]","system":"http://unitsofmeasure.org","code":"mm[Hg]"}},{"code":{"coding":[{"system":"http://loinc.org","code":"8480-6","display":"Systolic Blood Pressure"}],"text":"Systolic Blood Pressure"},"valueQuantity":{"value":129.38209711438796,"unit":"mm[Hg]","system":"http://unitsofmeasure.org","code":"mm[Hg]"}}]};
        let raw = DocBuf::from_document(&bdoc);
        let doc_el = Element::new(ElementType::EmbeddedDocument, raw.as_bytes());
        let doc_base = Rc::new(SystemType::Element(doc_el));
        let ctx = UnresolvableExecContext::new(doc_base);

        let e = parse_expression("component.value as CodeableConcept");
        let result = eval(&ctx, &e, ctx.root_resource()).unwrap();
        assert_eq!(true, result.is_empty());
    }
}