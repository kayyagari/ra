use std::borrow::Borrow;
use std::rc::Rc;
use bson::{Bson, bson, Document};
use bson::spec::ElementType;
use chrono::Utc;
use ksuid::Ksuid;
use log::debug;
use rawbson::elem::Element;
use rocksdb::WriteBatch;
use crate::barn::{Barn, CF_INDEX};
use crate::errors::RaError;
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
    pub fn insert_batch(&self, ksid: &Ksuid, res_def: &ResourceDef, mut data: Document, wb: &mut WriteBatch, sd: &SchemaDef) -> Result<Document, RaError> {
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
        self.index_searchparams(wb, &pk, &vec_bytes, res_def, sd);

        // handle references
        for (ref_prop, _) in &res_def.ref_props {
            let ref_node = data.get(ref_prop.as_str());
            if let Some(ref_node) = ref_node {
                match ref_node.element_type() {
                    ElementType::Array => {
                        let ref_arr = ref_node.as_array();
                        if let Some(ref_arr) = ref_arr {
                            for item in ref_arr.iter() {
                                self.insert_ref(ref_prop, ksid.as_bytes(), item, res_def, wb, sd);
                            }
                        }
                    },
                    _ => {
                        self.insert_ref(ref_prop, ksid.as_bytes(), ref_node, res_def, wb, sd);
                    }
                }
            }
        }
        Ok(data)
    }

    fn insert_ref<S: AsRef<str>>(&self, ref_at_name: S, from_id: &[u8], item: &Bson, from: &ResourceDef, wb: &mut WriteBatch, sd: &SchemaDef) -> Result<(), RaError> {
        if let Some(item) = item.as_document() {
            if let Some(target) = item.get("reference") {
                if let Some(target) = target.as_str() {
                    let parts: Vec<&str> = target.split("/").collect();
                    if parts.len() == 2 {
                        let to = sd.resources.get(parts[0]);
                        if let None = to {
                            return Err(RaError::bad_req(format!("resource not found with the name {} in the reference {}", parts[0], target)));
                        }
                        let to_id = Ksuid::from_base62(parts[1]);
                        if let Err(e) = to_id {
                            return Err(RaError::bad_req(format!("reference ID {} is in invalid format", parts[1])));
                        }
                        let to = to.unwrap();
                        let to_id = to_id.unwrap();
                        let empty_val: &[u8;0] = &[0;0];
                        let fwd_id = from.new_ref_fwd_id(ref_at_name, from_id, to, to_id.as_bytes());
                        wb.put(fwd_id, empty_val);

                        let rev_id = from.new_ref_rev_id(to_id.as_bytes(), to, from_id);
                        wb.put(rev_id, empty_val);
                    }
                    else {
                        return Err(RaError::bad_req(format!("invalid format of reference {}, should be <Resource-name>/<ID>", target)));
                    }
                }
                else {
                    // TODO is this necessary to log or will the validation ensure that this case is covered?
                }
            }
        }
        Ok(())
    }

    fn index_searchparams(&self, wb: &mut WriteBatch, pk: &[u8; 24], res_data: &Vec<u8>, rd: &ResourceDef, sd: &SchemaDef) -> Result<(), RaError> {
        let base = Element::new(ElementType::EmbeddedDocument, res_data.as_ref());
        let base = Rc::new(SystemType::Element(base));
        let search_params = sd.get_search_params_of(&rd.name);
        if let None = search_params {
            debug!("no search parameters found for the resource {}", &rd.name);
            return Ok(());
        }
        let search_params = search_params.unwrap();
        let cf = self.db.cf_handle(CF_INDEX).unwrap();
        let wrapped_sd = Some(sd);
        for (code, param_id) in search_params {
            let spd = sd.get_search_param(*param_id).unwrap();
            let expr = spd.expressions.get(&rd.name);
            let expr = expr.unwrap().as_ref().unwrap();
            let tokens = scan_tokens(expr.expr.as_str()).unwrap(); // the expression was already validated at the time of building schema
            let ast = parse_with_schema(tokens, wrapped_sd).unwrap();
            let result = eval(&ast, Rc::clone(&base))?;

            let mut rows: Vec<Option<(Vec<u8>, Vec<u8>)>> = Vec::new();
            format_index_rows(result, spd, expr, pk, &mut rows);
            for row in rows {
                if let Some((k, v)) = row {
                    wb.put_cf(cf, k.as_slice(), v.as_slice());
                }
            }
        }

        Ok(())
    }
}

fn format_index_rows(expr_result: Rc<SystemType>, spd: &SearchParamDef, expr: &SearchParamExpr, pk: &[u8; 24], rows: &mut Vec<Option<(Vec<u8>, Vec<u8>)>>) -> Result<(), RaError> {
    match expr_result.borrow() {
        SystemType::Collection(c) => {
            if c.is_empty() {
                let r = format_index_row(expr_result, spd, expr, pk);
                rows.push(r);
            }
            else {
                for item in c.iter() {
                    format_index_rows(Rc::clone(item), spd, expr, pk, rows)?;
                }
            }
        },
        SystemType::Element(e) => {
            if spd.param_type == SearchParamType::String {
                let mut strings = Vec::new();
                element_utils::gather_string_values(e, &mut strings)?;
                for s in strings {
                    let str_result = Rc::new(SystemType::String(SystemString::from_slice(s)));
                    let r = format_index_row(str_result, spd, expr, pk);
                    rows.push(r);
                }
            }
            else {
                let r = format_index_row(expr_result, spd, expr, pk);
                rows.push(r);
            }
        },
        _ => {
            let r = format_index_row(expr_result, spd, expr, pk);
            rows.push(r);
        }
    }

    Ok(())
}

fn format_index_row(expr_result: Rc<SystemType>, spd: &SearchParamDef, expr: &SearchParamExpr, pk: &[u8; 24]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut key: Vec<u8> = Vec::new();
    let mut value: Vec<u8> = Vec::new();
    key.extend_from_slice(&expr.hash); // the index number

    if !expr_result.is_truthy() {
        key.push(0); // NULL value flag
        key.extend_from_slice(pk);
        return Some((key, value));
    }
    else {
        key.push(1); // non-NULL value flag
    }

    let expr_result = expr_result.borrow();
    match spd.param_type {
        SearchParamType::String => {
            if let SystemType::String(s) = expr_result {
                key.extend_from_slice(s.as_str().to_lowercase().as_bytes()); // key always holds the lowercase value
                value.extend_from_slice(s.as_str().as_bytes()); // value will contain the string as is
            }
        },
        SearchParamType::Number => {
            if let SystemType::Number(n) = expr_result {
                key.extend_from_slice(&n.as_f64().to_le_bytes()); // store the number always as a float
                // value need not be stored
            }
        },
        SearchParamType::Date => {
            if let SystemType::DateTime(sd) = expr_result {
                key.extend_from_slice(&sd.millis().to_le_bytes());
            }
        },
        // SearchParamType::Quantity => {
        //     if let SystemType::Quantity(sq) = expr_result {
        //     }
        // },
        // SearchParamType::Token => {
        // },
        // SearchParamType::Reference => {
        // },
        // SearchParamType::Composite => {
        // },
        // SearchParamType::Uri => {
        // },
        // SearchParamType::Speacial => {
        // }
        _ => {}
    }

    key.extend_from_slice(pk);
    Some((key, value))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;
    use bson::doc;
    use rawbson::DocBuf;
    use serde_json::Value;
    use crate::rapath::stypes::SystemString;
    use crate::res_schema::{parse_res_def, parse_search_param};
    use crate::utils::test_utils::read_patient;
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
        let (k, v) = format_index_row(expr_result, &spd, expr, &pk).unwrap();
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
        let path = PathBuf::from("/tmp/insert_test_insert");
        std::fs::remove_dir_all(&path);
        let barn = Barn::open_with_default_schema(&path)?;
        let sd = barn.build_schema_def()?;
        let search_params = sd.get_search_params_of(&String::from("Patient")).unwrap();
        // for (code, id) in search_params {
        //     println!("{} {}", code, id);
        // }
        let spd = sd.search_params.get(search_params.get("family").unwrap()).unwrap();
        let expr = spd.expressions.get("Patient").unwrap().as_ref().unwrap();
        let patient_schema = sd.resources.get("Patient").unwrap();
        let data = read_patient();
        let data = bson::to_document(&data).unwrap();

        let mut data = barn.insert(patient_schema, data, &sd)?;
        let inserted_data = DocBuf::from_document(&data);
        let inserted_data = Element::new(ElementType::EmbeddedDocument, inserted_data.as_bytes());

        let cf = barn.db.cf_handle(CF_INDEX).unwrap();
        let mut itr = barn.db.prefix_iterator_cf(cf, expr.hash);
        let (k, v) = itr.next().unwrap();
        assert_eq!(6, v.len()); // Donald
        assert_eq!(35, k.len());

        std::fs::remove_dir_all(&path);
        Ok(())
    }
}