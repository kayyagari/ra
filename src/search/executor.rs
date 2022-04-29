use std::io::Cursor;
use std::process::id;
use bson::Document;
use ksuid::Ksuid;
use crate::api::base::{OperationOutcome, RaResponse, SearchQuery};
use crate::api::bundle::{SearchEntry, SearchSet};
use crate::barn::Barn;
use crate::res_schema::SchemaDef;
use crate::ResourceDef;
use crate::search::{Filter, Modifier, SearchParamType};
use crate::search::index_scanners::{IndexScanner, reference};
use crate::errors::{EvalError, IssueType, RaError};
use crate::search::ComparisonOperator;
use crate::search::index_scanners::and_or::AndOrIndexScanner;
use crate::search::index_scanners::not::NotIndexScanner;
use crate::search::index_scanners::string::StringIndexScanner;

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

pub fn to_index_scanner<'f, 'd: 'f>(filter: &'f Filter, rd: &'f ResourceDef, sd: &'f SchemaDef, db: &'d Barn) -> Result<Box<dyn IndexScanner + 'f>, EvalError> {
    match filter {
        Filter::SimpleFilter {identifier, value,  operator} => {
            let (name, modifier, path) = parse_attribute_name(identifier);
            let spd_and_expr = sd.get_search_param_expr_for_res(name, &rd.name);
            if let None = spd_and_expr {
                return Err(EvalError::new(format!("there is no search parameter defined with code {} on {}", identifier, rd.name)));
            }
            let (spd, sp_expr) = spd_and_expr.unwrap();

            if let None = sp_expr {
                return Err(EvalError::new(format!("cannot search on a non-indexed field, there is no FHIRPATH expression for the search parameter defined with code {} on {}", identifier, rd.name)));
            }

            let sp_expr = sp_expr.unwrap();
            let idx_scanner: Box<IndexScanner>;
            match spd.param_type {
                SearchParamType::String => {
                    let itr = db.new_index_iter(&sp_expr.hash);
                    let tmp = StringIndexScanner::new(value, itr, operator, &sp_expr.hash, modifier);
                    idx_scanner = Box::new(tmp);
                },
                SearchParamType::Reference => {
                    let (ref_id, mut ref_type) = parse_ref_val(value)?;
                    let itr = db.new_index_iter(&sp_expr.hash);
                    if let Modifier::Custom(ref s) = modifier {
                        if s == "identifier" {
                            if let Some(path) = path {
                                return Err(EvalError::new(format!("chaining is not supporrted when identifier is used as the modifier")));
                            }
                            // do identifier search
                        }
                        else {
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
                    }

                    // do normal reference search
                    let tmp = reference::new(ref_id, ref_type_hash, itr, &sp_expr.hash, modifier);
                    idx_scanner = Box::new(tmp);
                },
                _ => {
                    return Err(EvalError::new(format!("unsupported search parameter type {:?}", spd.param_type)));
                }
            }

            return Ok(idx_scanner);
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

fn parse_ref_val(ref_val: &str) -> Result<(Ksuid, Option<&str>), EvalError> {
    let mut parts = ref_val.splitn(2, "/");
    let first = parts.next();
    let second = parts.next();

    if let None = first {
        return Err(EvalError::new(format!("invalid reference value {}", ref_val)));
    }
    let mut first = first.unwrap();
    let mut ref_type = None;
    if let Some(id) = second {
        ref_type = Some(first); // in this case, first part holds the name of the type of the resouce e.g Patient/<ksuid>
        first = id;
    }

    let id = Ksuid::from_base62(first);
    if let Err(e) = id {
        return Err(EvalError::new(format!("invalid reference ID {}", first)));
    }

    Ok((id.unwrap(), ref_type))
}

#[cfg(test)]
mod tests {
    use crate::search::executor::parse_attribute_name;
    use crate::search::Modifier;

    #[test]
    fn test_parse_attribute_name() {
        let mut candidates = Vec::new();
        candidates.push(("subject:Patient.name", ("subject", Modifier::Custom("Patient".to_string()), Some("name"))));
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
}