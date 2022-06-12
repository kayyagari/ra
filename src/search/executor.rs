use std::collections::VecDeque;
use std::io::Cursor;
use std::process::id;
use std::rc::Rc;
use bson::Document;
use ksuid::Ksuid;
use lazy_static::lazy_static;
use log::debug;
use regex::Regex;
use crate::api::base::{OperationOutcome, RaResponse, SearchQuery};
use crate::api::bundle::{SearchEntry, SearchSet};
use crate::barn::Barn;
use crate::res_schema::{SchemaDef, SearchParamDef, SearchParamExpr};
use crate::ResourceDef;
use crate::search::{Filter, Modifier, SearchParamType};
use crate::search::index_scanners::{IndexScanner, reference};
use crate::errors::{EvalError, IssueType, RaError};
use crate::rapath::parser::parse;
use crate::rapath::scanner::scan_tokens;
use crate::search::ComparisonOperator;
use crate::search::index_scanners::and_or::AndOrIndexScanner;
use crate::search::index_scanners::not::NotIndexScanner;
use crate::search::index_scanners::reference::{ChainedParam, ReferenceChainIndexScanner};
use crate::search::index_scanners::string::StringIndexScanner;

lazy_static! {
    static ref HTTP_RE: Regex = Regex::new(r"(?i)^((http|https)://)").unwrap();
}
pub fn execute_search_query(filter: &Filter, sq: &SearchQuery, rd: &ResourceDef, db: &Barn, sd: &SchemaDef) -> Result<RaResponse, RaError> {
    let mut idx = to_index_scanner(filter, rd, sd, db)?;
    let keys = idx.collect_all();
    let mut ss = SearchSet::new();
    let mut count = 0;
    for (ref k, _) in keys {
        let res = db.get_resource_by_pk(k)?;
        if let Some(res) = res {
            let mut cursor = Cursor::new(res.as_ref());
            let doc = Document::from_reader(&mut cursor);
            if let Err(e) = doc {
                let msg = format!("error while deserializing the document data fetched from database ({})", e.to_string());
                let oo = OperationOutcome::new_error(IssueType::Exception, msg);
                return Err(RaError::Custom{code: 500, outcome: oo});
            }
            ss.add(doc.unwrap());
            count += 1;
            if count >= sq.count {
                break;
            }
        }
    }
    Ok(RaResponse::SearchResult(ss))
}

pub fn to_index_scanner<'f, 'd: 'f>(filter: &'f Filter, rd: &'f ResourceDef, sd: &'f SchemaDef, db: &'d Barn) -> Result<Box<dyn IndexScanner<'f> + 'f>, EvalError> {
    match filter {
        Filter::SimpleFilter {identifier, value,  operator} => {
            let (name, modifier, path) = parse_attribute_name(identifier);
            return create_index_scanner(name, value, operator, modifier, path, rd, sd, db);
        },
        Filter::AndFilter {children} => {
            let mut scanners = Vec::with_capacity(children.len());
            for c in children {
                let cs = to_index_scanner(c, rd, sd, db)?;
                scanners.push(cs);
            }

            let and = AndOrIndexScanner::new_and(scanners);
            return Ok(Box::new(and));
        },
        Filter::OrFilter {children} => {
            let mut scanners = Vec::with_capacity(children.len());
            for c in children {
                let cs = to_index_scanner(c, rd, sd, db)?;
                scanners.push(cs);
            }

            let or = AndOrIndexScanner::new_or(scanners);
            return Ok(Box::new(or));
        },
        Filter::NotFilter {child} => {
            let cs = to_index_scanner(child, rd, sd, db)?;
            let ns = NotIndexScanner::new(cs, rd, db);
            return Ok(Box::new(ns));
        }
        _ => {
        }
    }

    Err(EvalError::new(format!("unsupported filter type {:?}", filter.get_type())))
}

pub fn find_search_param_expr<'f>(name: &'f str, rd: &'f ResourceDef, sd: &'f SchemaDef) -> Result<(&'f SearchParamDef, &'f SearchParamExpr), EvalError> {
    let spd_and_expr = sd.get_search_param_expr_for_res(name, &rd.name);
    if let None = spd_and_expr {
        return Err(EvalError::new(format!("there is no search parameter defined with code {} on {}", name, rd.name)));
    }
    let (spd, sp_expr) = spd_and_expr.unwrap();

    if let None = sp_expr {
        return Err(EvalError::new(format!("cannot search on a non-indexed field, there is no FHIRPath expression for the search parameter defined with code {} on {}", name, rd.name)));
    }

    Ok((spd, sp_expr.unwrap()))
}

pub fn create_index_scanner<'f>(name: &'f str, value: &'f str, operator: &'f ComparisonOperator, modifier: Modifier<'f>, path: Option<&'f str>, rd: &'f ResourceDef, sd: &'f SchemaDef, db: &'f Barn) -> Result<Box<dyn IndexScanner<'f> + 'f>, EvalError> {
    let (spd, sp_expr) = find_search_param_expr(name, rd, sd)?;
    let idx_scanner: Box<dyn IndexScanner>;
    match spd.param_type {
        SearchParamType::String => {
            let itr = db.new_index_iter(&sp_expr.hash);
            let tmp = StringIndexScanner::new(value, itr, operator, &sp_expr.hash, modifier);
            idx_scanner = Box::new(tmp);
        },
        SearchParamType::Reference => {
            let itr = db.new_index_iter(&sp_expr.hash);
            if modifier == Modifier::Identifier {
                if let Some(path) = path {
                    return Err(EvalError::new(format!("chaining is not supporrted when identifier is used as the modifier")));
                }
                // do identifier search
                let (system, code) = parse_identifier(value);
                let mut rpath_expr = String::with_capacity(100);
                if let Some(system) = system {
                    rpath_expr.push_str(format!("identifier.where(system = '{}'", system).as_str());
                    if let Some(code) = code {
                        rpath_expr.push_str(format!(" and value = '{}')", code).as_str());
                    }
                    else {
                        rpath_expr.push_str(")");
                    }
                }
                else if let Some(code) = code {
                    rpath_expr.push_str(format!("identifier.where(value = '{}')", code).as_str());
                }

                debug!("using rapath expression {}", &rpath_expr);
                let tokens = scan_tokens(&rpath_expr);
                if let Err(e) = tokens {
                    return Err(EvalError::new(format!("failed to tokenize the FHIRPath expression: {}", e)));
                }
                let rpath_expr = parse(tokens.unwrap());
                if let Err(e) = rpath_expr {
                    return Err(EvalError::new(format!("failed to parse the FHIRPath expression: {}", e)));
                }
                let tmp = reference::new_reference_id_scanner(rpath_expr.unwrap(), db, &sp_expr.hash);
                return Ok(Box::new(tmp));
            }

            let (mut ref_type, ref_id, version_num) = parse_ref_val("", value)?;
            if let Modifier::Custom(s) = modifier {
                if let Some(rt) = ref_type {
                    // check that the given ResourceType in modifier is same as the one present in
                    // the value e.g subject:Patient = Patient/1
                    // if not, throw an error
                    if rt != s {
                        return Err(EvalError::new(format!("mismatched resourceType names in modifier({}) and reference({})", s, rt)));
                    }
                }
                else {
                    let rd = sd.get_res_def_by_name(s);
                    if let Err(e) = rd {
                        return Err(EvalError::new(format!("unknown resourceType {}", s)));
                    }
                    ref_type = Some(&rd.unwrap().name);
                }
            }

            let mut ref_type_hash = None;
            if let Some(rt) = ref_type {
                let rd = sd.get_res_def_by_name(rt);
                if let Err(e) = rd {
                    return Err(EvalError::new(format!("unknown resourceType {}", rt)));
                }
                ref_type_hash = Some(rd.unwrap().hash);
            }

            if let Some(path) = path {
                // do chained search
                let chain = parse_chain(path, value, operator);
                let tmp = reference::new_reference_chain_scanner(Rc::new(chain), ref_type_hash, db, sd, &sp_expr.hash);
                return Ok(Box::new(tmp));
            }

            // do normal reference search
            if let None = ref_id {
                return Err(EvalError::new(format!("missing reference ID in reference {}", value)));
            }
            let ref_id_val = Ksuid::from_base62(ref_id.unwrap());
            if let Err(e) = ref_id_val {
                return Err(EvalError::new(format!("invalid reference ID {}", ref_id.unwrap())));
            }
            let tmp = reference::new_reference_scanner(ref_id_val.unwrap(), ref_type_hash, itr, &sp_expr.hash, modifier);
            idx_scanner = Box::new(tmp);
        },
        _ => {
            return Err(EvalError::new(format!("unsupported search parameter type {:?}", spd.param_type)));
        }
    }

    return Ok(idx_scanner);
}

fn parse_attribute_name(name: &str) -> (&str, Modifier, Option<&str>) {
    let mut parts = name.splitn(2, ".");
    let mut at_name = parts.next().unwrap();
    let mut modifier = Modifier::None;
    let mut at_name_parts = at_name.splitn(2,":");
    if let Some(first) = at_name_parts.next() {
        at_name = first;
    }

    if let Some(m) = at_name_parts.next() {
        modifier = Modifier::from(m);
    }

    let chain = parts.next();
    (at_name, modifier, chain)
}

fn parse_ref_val<'f>(base_url: &str, mut ref_val: &'f str) -> Result<(Option<&'f str>, Option<&'f str>, Option<u32>), EvalError> {
    let without_base = ref_val.strip_prefix(base_url);
    if let None = without_base {
        let starts_with_http = HTTP_RE.is_match(ref_val);
        if starts_with_http {
            return Err(EvalError::from_str("searching for canonical references is not supported"))
        }
    }
    else {
        ref_val = without_base.unwrap();
    }

    let mut parts = ref_val.splitn(2, "/");
    let first = parts.next();
    let second = parts.next();

    if let None = first {
        return Err(EvalError::new(format!("invalid reference value {}", ref_val)));
    }
    let mut first = first.unwrap();
    let mut ref_type = None;
    let mut ref_id = None;
    let mut version = None;
    if let Some(id) = second {
        ref_type = Some(first); // in this case, first part holds the name of the type of the resouce e.g Patient/<ksuid>
        // look for version
        let mut parts = id.split_terminator("/");

        ref_id = parts.next();

        if let Some(history) = parts.next() {
            if history == "_history" {
                if let Some(v) = parts.next() {
                    let v_num = v.parse::<u32>();
                    if let Err(e) = v_num {
                        return Err(EvalError::new(format!("invalid version number {} in reference value {}", v, ref_val)));
                    }
                    version = Some(v_num.unwrap());
                }
            }
        }
    }
    else {
        if !first.is_empty() {
            ref_id = Some(first);
        }
    }

    Ok((ref_type, ref_id, version))
}

fn parse_chain<'f>(at_path: &'f str, value: &'f str, operator: &'f ComparisonOperator) -> ChainedParam<'f> {
    let mut parts = at_path.split(".").peekable();
    let mut links = VecDeque::new();
    loop {
        match parts.next() {
            Some(s) => {
                let mut name_mod = s.splitn(2, ":");
                let name = name_mod.next().unwrap();
                let mut modifier = Modifier::None;
                if let Some(m) = name_mod.next() {
                    modifier = Modifier::from(m);
                }
                let mut opt_val = None;
                if let None = parts.peek() { // last attribute in the chain
                    opt_val = Some(value);
                }

                links.push_back(ChainedParam::new(name, modifier, opt_val, operator));
            },
            None => break
        }
    }

    let mut root: Option<ChainedParam> = None;
    loop {
        match links.pop_back() {
            Some(mut cp) => {
                if let None = root {
                    root = Some(cp);
                }
                else {
                    cp.add_child(root.unwrap());
                    root = Some(cp);
                }
            },
            None => break
        }
    }
    root.unwrap()
}

/// Parses the given identifier token and returns (<system>, <code>) tuple
fn parse_identifier(value: &str) -> (Option<&str>, Option<&str>) {
    let separator = value.find("|");
    if let None = separator {
        return (None, Some(value));
    }

    let separator = separator.unwrap();
    if separator == 0 {
        let mut code = None;
        if value.len() > 1 {
            code = Some(&value[1..]);
        }
        return (None, code);
    }
    let mut id_parts = value.split_terminator("|");
    let system = Some(id_parts.next().unwrap());
    let mut code = None;
    if let Some(c) = id_parts.next() {
        code = Some(c);
    }

    (system, code)
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use crate::search::executor::{parse_attribute_name, parse_ref_val};
    use crate::search::Modifier;

    #[test]
    fn test_parse_attribute_name() {
        let mut candidates = Vec::new();
        candidates.push(("subject:Patient.name", ("subject", Modifier::Custom("Patient"), Some("name"))));
        candidates.push(("general-practitioner.name", ("general-practitioner", Modifier::None, Some("name"))));
        candidates.push(("name:exact", ("name", Modifier::Exact, None)));
        candidates.push(("name", ("name", Modifier::None, None)));
        for (attribute_name, (expected_name, expected_mod, expected_path)) in candidates {
            let (actual_name, actual_mod, actual_path) = parse_attribute_name(attribute_name);
            assert_eq!(expected_name, actual_name);
            assert_eq!(expected_mod, actual_mod);
            assert_eq!(expected_path, actual_path);
        }
    }

    #[test]
    fn test_parse_reference_value() -> Result<(), Error>{
        let base_url = "http://ra.org/fhir/";
        let mut candidates = Vec::new();
        candidates.push(("Observation/123/_history/234234", Some("Observation"), Some("123"), Some(234234)));
        let rv = format!("{}Observation/123/_history/234234", base_url);
        candidates.push((rv.as_str(), Some("Observation"), Some("123"), Some(234234)));
        candidates.push(("Observation/123", Some("Observation"), Some("123"), None));
        candidates.push(("Observation", None, Some("Observation"), None));
        candidates.push(("", None, None, None));

        for (ref_value, expected_ref_type, expected_ref_id, expected_version) in candidates {
            println!("{}", ref_value);
            let (actual_ref_type, actual_ref_id, actual_version) = parse_ref_val(base_url, ref_value)?;
            assert_eq!(expected_ref_type, actual_ref_type);
            assert_eq!(expected_ref_id, actual_ref_id);
            assert_eq!(expected_version, actual_version);
        }

        let r = parse_ref_val(base_url, "http://hl7.org/fhir/ValueSet/example|3.0");
        assert!(r.is_err());
        Ok(())
    }
}