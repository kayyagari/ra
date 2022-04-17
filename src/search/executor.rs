use std::io::Cursor;
use bson::Document;
use crate::api::base::{OperationOutcome, RaResponse, SearchQuery};
use crate::api::bundle::{SearchEntry, SearchSet};
use crate::barn::Barn;
use crate::res_schema::SchemaDef;
use crate::ResourceDef;
use crate::search::{Filter, Modifier, SearchParamType};
use crate::search::index_scanners::IndexScanner;
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
            let (name, modifier) = parse_attribute_name(identifier);
            let spd_and_expr = sd.get_search_param_expr_for_res(name, &rd.name);
            if let None = spd_and_expr {
                return Err(EvalError::new(format!("there is no search parameter defined with code {} on {}", identifier, rd.name)));
            }
            let (spd, sp_expr) = spd_and_expr.unwrap();

            if let None = sp_expr {
                return Err(EvalError::new(format!("cannot search on a non-indexed field, there is no FHIRPATH expression for the search parameter defined with code {} on {}", identifier, rd.name)));
            }

            let sp_expr = sp_expr.unwrap();
            let idx_scanner;
            match spd.param_type {
                SearchParamType::String => {
                    let itr = db.new_index_iter(&sp_expr.hash);
                    idx_scanner = StringIndexScanner::new(value, itr, operator, &sp_expr.hash, modifier)
                },
                _ => {
                    return Err(EvalError::new(format!("unsupported search parameter type {:?}", spd.param_type)));
                }
            }

            return Ok(Box::new(idx_scanner));
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

fn parse_attribute_name(name: &str) -> (&str, Modifier) {
    let mut parts = name.splitn(2, ":");
    let at_name = parts.next().unwrap();
    let mut modifier = Modifier::None;
    if let Some(m) = parts.next() {
        modifier = Modifier::from(m);
    }

    (at_name, modifier)
}
