use std::borrow::Borrow;
use std::rc::Rc;
use bson::{Bson, bson, Document};
use bson::spec::ElementType;
use chrono::Utc;
use ksuid::Ksuid;
use log::{debug, trace};
use rawbson::elem::Element;
use rocksdb::WriteBatch;
use crate::barn::{Barn, CF_INDEX, ResolvableContext};
use crate::errors::{EvalError, RaError};
use crate::rapath::element_utils;
use crate::rapath::engine::eval;
use crate::rapath::parser::{parse, parse_with_schema};
use crate::rapath::scanner::scan_tokens;
use crate::rapath::stypes::{SystemString, SystemType};
use crate::res_schema::{SchemaDef, SearchParamDef, SearchParamExpr};
use crate::ResourceDef;
use crate::search::SearchParamType;
use crate::utils::bson_utils;

impl Barn {
    pub fn insert_batch(&self, ksid: &Ksuid, res_def: &ResourceDef, mut data: Document, wb: &mut WriteBatch, sd: &SchemaDef, skip_indexing: bool) -> Result<(Document, Vec<u8>, [u8; 24]), RaError> {
        let res_id = ksid.to_base62();
        debug!("inserting a {} with ID {}", &res_def.name, &res_id);
        data.insert("id", Bson::from(res_id));

        // update metadata
        let mut meta = data.get_mut("meta");
        if let None = meta {
            data.insert("meta", bson!({}));
            meta = data.get_mut("meta");
        }
        // TODO is the below check needed??
        // else if let Some(m) = meta {
        //     if m.element_type() != ElementType::EmbeddedDocument {
        //
        //     }
        // }
        let mut meta = meta.unwrap().as_document_mut().unwrap();
        meta.insert("versionId", Bson::from(1));
        // this has to be inserted as a string otherwise when serialized to JSON
        // dates are formatted in extended-JSON format
        meta.insert("lastUpdated", Bson::from(Utc::now().format(bson_utils::DATE_FORMAT).to_string()));

        // TODO move this block to update and replace calls
        // check version history
        // let history_pk = res_def.new_history_prefix_id(ksid.as_bytes());
        // let history_count_rec = self.db.get(&history_pk);
        // let mut history_count = 1; // history number always points to the current version (and it always starts with 1)
        // if history_count_rec.is_ok() {
        //     let history_count_rec = history_count_rec.unwrap().unwrap();
        //     history_count = utils::u32_from_le_bytes(history_count_rec.as_bytes());
        // }

        let mut vec_bytes = Vec::new();
        data.to_writer(&mut vec_bytes);

        let pk = res_def.new_id(ksid.as_bytes());
        wb.put(&pk, vec_bytes.as_slice());
        if !skip_indexing {
            self.index_searchparams(wb, &pk, &vec_bytes, res_def, sd)?;
        }

        Ok((data, vec_bytes, pk))
    }

    pub fn index_searchparams(&self, wb: &mut WriteBatch, pk: &[u8; 24], res_data: &Vec<u8>, rd: &ResourceDef, sd: &SchemaDef) -> Result<(), RaError> {
        let base = Element::new(ElementType::EmbeddedDocument, res_data.as_ref());
        let base = Rc::new(SystemType::Element(base));
        let search_params = sd.get_search_params_of(&rd.name);
        if let None = search_params {
            debug!("no search parameters found for the resource {}", &rd.name);
            return Ok(());
        }
        let search_params = search_params.unwrap();
        //println!("{:?}", search_params.iter().map(|e| e.0.to_string()).collect::<Vec<String>>());
        let cf = self.db.cf_handle(CF_INDEX).unwrap();
        let wrapped_sd = Some(sd);
        for (code, param_id) in search_params {
            let spd = sd.get_search_param(*param_id).unwrap();
            let expr = spd.expressions.get(&rd.name);
            let expr = expr.unwrap().as_ref().unwrap();
            //debug!("evaluating expression {} of search param {}", expr.expr, code);
            let tokens = scan_tokens(expr.expr.as_str()).unwrap(); // the expression was already validated at the time of building schema
            let ast = parse_with_schema(tokens, wrapped_sd).unwrap();
            let ctx = ResolvableContext::new(Rc::clone(&base), self, sd);
            let result = eval(&ctx, &ast, Rc::clone(&base))?;

            let mut rows: Vec<Option<(Vec<u8>, Vec<u8>)>> = Vec::new();
            format_index_rows(result, spd, expr, sd, pk, &mut rows)?;
            for row in rows {
                if let Some((k, v)) = row {
                    wb.put_cf(cf, k.as_slice(), v.as_slice());
                }
            }
        }

        Ok(())
    }
}

fn format_index_rows(expr_result: Rc<SystemType>, spd: &SearchParamDef, expr: &SearchParamExpr, sd: &SchemaDef, pk: &[u8; 24], rows: &mut Vec<Option<(Vec<u8>, Vec<u8>)>>) -> Result<(), RaError> {
    match expr_result.borrow() {
        SystemType::Collection(c) => {
            if c.is_empty() {
                let r = format_index_row(expr_result, spd, expr, sd, pk)?;
                rows.push(r);
            }
            else {
                for item in c.iter() {
                    format_index_rows(Rc::clone(item), spd, expr, sd, pk, rows)?;
                }
            }
        },
        SystemType::Element(e) => {
            if spd.param_type == SearchParamType::String {
                let mut strings = Vec::new();
                element_utils::gather_string_values(e, &mut strings)?;
                for s in strings {
                    let str_result = Rc::new(SystemType::String(SystemString::from_slice(s)));
                    let r = format_index_row(str_result, spd, expr, sd, pk)?;
                    rows.push(r);
                }
            }
            else {
                let r = format_index_row(expr_result, spd, expr, sd, pk)?;
                rows.push(r);
            }
        },
        _ => {
            let r = format_index_row(expr_result, spd, expr, sd, pk)?;
            rows.push(r);
        }
    }

    Ok(())
}

fn format_index_row(expr_result: Rc<SystemType>, spd: &SearchParamDef, expr: &SearchParamExpr, sd: &SchemaDef, pk: &[u8; 24]) -> Result<Option<(Vec<u8>, Vec<u8>)>, RaError> {
    let mut key: Vec<u8> = Vec::new();
    let mut value: Vec<u8> = Vec::new();
    key.extend_from_slice(&expr.hash); // the index number

    if !expr_result.is_truthy() {
        key.push(0); // NULL value flag
        key.extend_from_slice(pk);
        return Ok(Some((key, value)));
    }

    let expr_result = expr_result.borrow();
    match spd.param_type {
        SearchParamType::String => {
            if let SystemType::String(s) = expr_result {
                key.push(1);
                key.extend_from_slice(s.as_str().to_lowercase().as_bytes()); // key always holds the lowercase value
                value.extend_from_slice(s.as_str().as_bytes()); // value will contain the string as is
            }
        },
        SearchParamType::Number => {
            if let SystemType::Number(n) = expr_result {
                key.push(1);
                key.extend_from_slice(&n.as_f64().to_le_bytes()); // store the number always as a float
                // value need not be stored
            }
        },
        SearchParamType::Date => {
            if let SystemType::DateTime(sd) = expr_result {
                key.push(1);
                key.extend_from_slice(&sd.millis().to_le_bytes());
            }
        },
        // SearchParamType::Quantity => {
        //     if let SystemType::Quantity(sq) = expr_result {
        //     }
        // },
        // SearchParamType::Token => {
        // },
        SearchParamType::Reference => {
            if let SystemType::Element(e) = expr_result {
                let ref_id_and_version = get_reference_val_from(e, sd)?;
                if let Some((ref_id, version)) = ref_id_and_version {
                    key.push(1);
                    key.extend_from_slice(&ref_id);

                    if let Some(version) = version {
                        value.extend_from_slice(&version.to_le_bytes());
                    }
                }
            }
        },
        // SearchParamType::Composite => {
        // },
        // SearchParamType::Uri => {
        // },
        // SearchParamType::Speacial => {
        // }
        _ => {}
    }

    key.extend_from_slice(pk);
    Ok(Some((key, value)))
}

fn get_reference_val_from(el: &Element, sd: &SchemaDef) -> Result<Option<([u8; 24], Option<u32>)>, RaError> {
    if let Ok(el) = el.as_document() {
        if let Ok(target) = el.get_str("reference") {
            if let Some(target) = target {
                let parts: Vec<&str> = target.split("/").collect();
                if parts.len() == 2 {
                    let to = sd.resources.get(parts[0]).unwrap();
                    // split again to extract version
                    let mut parts = parts[1].splitn(2, "|");
                    if let Some(res_id) = parts.next() {
                        let to_id = Ksuid::from_base62(res_id);
                        if let Err(e) = to_id {
                            return Err(RaError::BadRequest(format!("invalid resource ID in reference {} ({})", target, e.to_string())));
                        }
                        let to_id = to_id.unwrap();
                        let mut ref_id: [u8; 24] = [0; 24];
                        ref_id[..4].copy_from_slice(&to.hash);
                        ref_id[4..].copy_from_slice(to_id.as_bytes());

                        let mut version = None;
                        if let Some(v) = parts.next() {
                            let tmp = v.parse::<u32>();
                            if let Err(e) = tmp {
                                return Err(RaError::BadRequest(format!("invalid version number in reference {} ({})", target, e.to_string())));
                            }
                            version = Some(tmp.unwrap());
                        }
                        return Ok(Some((ref_id, version)));
                    }
                }
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::fs::File;
    use std::path::PathBuf;
    use bson::doc;
    use rawbson::DocBuf;
    use serde_json::Value;
    use crate::api::base::ApiBase;
    use crate::configure_log4rs;
    use crate::rapath::stypes::SystemString;
    use crate::res_schema::{parse_res_def, parse_search_param};
    use crate::utils::test_utils::{read_bundle, read_patient, TestContainer};
    use super::*;

    #[test]
    fn test_format_index_row() {
        let f = File::open("test_data/fhir.schema-4.0.json").unwrap();
        let v: Value = serde_json::from_reader(f).unwrap();
        let mut sd = parse_res_def(&v).unwrap();

        let doc = doc!{"id": "id", "base":["Patient"],"code":"family","expression":"Patient.name.family","type":"string"};
        let spd = parse_search_param(&doc, &sd).unwrap();
        let spd_id = spd.id;
        sd.add_search_param(spd);
        let spd = sd.search_params.get(&spd_id).unwrap();

        let str_val = "This is A string";
        let pk = Ksuid::generate();
        let pk = pk.as_bytes();
        let pk = sd.resources.get("Patient").unwrap().new_id(pk);
        let expr = spd.expressions.get("Patient").unwrap().as_ref().unwrap();
        let expr_result = Rc::new(SystemType::String(SystemString::from_slice(str_val)));
        let (k, v) = format_index_row(expr_result, &spd, expr, &sd, &pk).unwrap().unwrap();
        assert_eq!(&expr.hash, &k[..4]);
        assert_eq!(1u8, k[4]);
        let str_bytes = str_val.to_lowercase();
        let str_bytes = str_bytes.as_bytes();
        let str_end_pos = str_bytes.len() + 5;
        assert_eq!(str_bytes, &k[5..str_end_pos]);
        assert_eq!(pk, &k[str_end_pos..]);
        assert_eq!(str_val.as_bytes(), v.as_slice());
    }

    #[test]
    fn test_insert() -> Result<(), anyhow::Error> {
        //configure_log4rs();
        let tc = TestContainer::new();
        let api_base = tc.setup_api_base_with_example_patient();
        let (spd, expr) = api_base.schema.get_search_param_expr_for_res("family", "Patient").unwrap();
        let expr = expr.unwrap();
        let patient_schema = api_base.schema.resources.get("Patient").unwrap();

        let db = &api_base.db.db;
        let cf = db.cf_handle(CF_INDEX).unwrap();
        let mut itr = db.prefix_iterator_cf(cf, expr.hash);
        // for row in itr {
        //     let pos = row.0.len() - 24;
        //     let hasVal = row.0[4] == 1;
        //     let mut norm_val_in_key = None;
        //     let mut v = Cow::from("");
        //     if hasVal {
        //         norm_val_in_key = Some(&row.0[5..pos]);
        //         v = String::from_utf8_lossy(norm_val_in_key.unwrap());
        //     }
        //
        //     println!("{:?} {}", &row.0[..4], v);
        // }
        let (k, v) = itr.next().unwrap();
        assert_eq!(8, v.len()); // Chalmers
        assert_eq!(37, k.len());

        let (spd, expr) = api_base.schema.get_search_param_expr_for_res("patient", "Encounter").unwrap();
        let expr = expr.unwrap();
        let mut itr = db.prefix_iterator_cf(cf, expr.hash);
        let (k, v) = itr.next().unwrap();
        assert_ne!(expr.hash, &k[0..4]);

        let bundle = read_bundle();
        api_base.bundle(bundle).unwrap();
        let mut itr = db.prefix_iterator_cf(cf, expr.hash);
        let (k, v) = itr.next().unwrap();
        assert_eq!(expr.hash, &k[0..4]);
        assert_eq!(53, k.len());

        Ok(())
    }
}