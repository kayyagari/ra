use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::time::Instant;

use bson::{Bson, Document};
use bson::spec::ElementType;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use ksuid::Ksuid;
use log::{debug, error, info, trace, warn};
use rawbson::de::BsonDeserializer;
use rawbson::DocBuf;
use rawbson::elem::Element;
use rocksdb::{DB, DBCompressionType, DBIteratorWithThreadMode, Env, IngestExternalFileOptions, IteratorMode, Options, ReadOptions};
use serde_json::Value;
use thiserror::private::PathAsDisplay;

use crate::errors::{EvalError, RaError};
use crate::rapath::engine::eval;
use crate::rapath::expr::Ast;
use crate::rapath::stypes::SystemType;
use crate::res_schema::ResourceDef;
use crate::utils;

pub struct Barn {
    env: Env,
    db: DB,
    opts: Options,
}

impl Barn {
    pub fn open(db_path: &PathBuf) -> Result<Barn, RaError> {
        let mut opts = Self::default_db_options();
        Barn::_open(db_path, &mut opts)
    }

    fn default_db_options() -> Options {
        let mut res_db_opts = Options::default();
        res_db_opts.create_if_missing(true);
        res_db_opts.create_missing_column_families(true);
        res_db_opts.set_compression_type(DBCompressionType::Snappy);
        res_db_opts.set_use_direct_io_for_flush_and_compaction(true);
        res_db_opts.set_writable_file_max_buffer_size(100 * 1024 * 1024); // 100 MB
        //res_db_opts.increase_parallelism(cpu_count);

        //res_db_opts.set_use_direct_reads(true);
        //res_db_opts.set_compaction_readahead_size(5 * 1024 * 1024);

        res_db_opts
    }

    fn _open(db_path: &PathBuf, res_db_opts: &mut Options) -> Result<Barn, RaError> {
        if !db_path.exists() {
            let r = fs::create_dir_all(&db_path);
            if let Err(e) = r {
                let msg = format!("unable to create the db environment directory {}", db_path.as_display());
                warn!("{}", &msg);
                return Err(RaError::DbError(msg));
            }
        }

        let env = Env::default().unwrap();
        info!("opened db environment");
        res_db_opts.set_env(&env);
        let mut res_db = DB::open(res_db_opts, &db_path).unwrap();
        let b = Barn {
            env,
            db: res_db,
            opts: res_db_opts.clone()
        };

        Ok(b)
    }

    pub fn insert(&self, res_def: &ResourceDef, data: &mut Document) -> Result<(), RaError> {
        let ksid = Ksuid::generate();
        let pk = res_def.new_prefix_id(ksid.as_bytes());

        let res_id = ksid.to_base62();
        debug!("inserting a {} with ID {}", &res_def.name, &res_id);
        let res_id = Bson::from(res_id);
        data.remove("id");
        data.insert("id", res_id);

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

        let put_result = self.db.put(&pk, vec_bytes.as_slice());
        if let Err(e) = put_result {
            let msg = format!("unable to insert the record {}", e);
            warn!("{}", &msg);
            return Err(RaError::DbError(msg));
        }

        // handle references
        for ref_prop in &res_def.ref_props {
            let ref_node = data.get(ref_prop.as_str());
            if ref_node.is_some() {

            }
        }
        Ok(())
    }

    // pub fn get(&self, id: u64, res_name: String) -> Result<Document, RaError> {
    // }

    pub fn search<'a>(&self, res_def: &ResourceDef, filter: &'a Ast<'a>) -> Result<Vec<Value>, EvalError> {
        //let read_opts = ReadOptions::default();
        let mut results = Vec::new();

        let mut count = 0;
        let start = Instant::now();
        let mut inner = self.db.prefix_iterator(&res_def.hash);
        for (k, v) in inner {
            count += 1;
            let e = Element::new(ElementType::EmbeddedDocument, v.as_ref());
            let st = Rc::new(SystemType::Element(e));
            let pick = eval(&filter, st)?;
            if pick.is_truthy() {
                let de = BsonDeserializer::from_rawbson(e);
                let val: Value = rawbson::de::from_doc(e.as_document().unwrap())?;
                results.push(val);
            }
        }
        let elapsed = start.elapsed().as_secs();
        println!("time took to search through {} records {}", count, elapsed);
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use crate::res_schema::parse_res_def;
    use crate::test_utils::{parse_expression, read_patient, to_docbuf};
    use super::*;

    #[test]
    fn test_search() -> Result<(), anyhow::Error> {
        let f = File::open("test_data/fhir.schema-4.0.json")?;
        let v: Value = serde_json::from_reader(f)?;
        let s = parse_res_def(&v)?;
        let patient_schema = s.resources.get("Patient").unwrap();

        let path = PathBuf::from("/tmp/testdb");
        std::fs::remove_dir_all(&path);
        let barn = Barn::open(&path)?;
        let data = read_patient();
        let data = bson::to_bson(&data).unwrap();
        let mut data = data.as_document().unwrap().to_owned();
        barn.insert(patient_schema, &mut data)?;
        data.remove("id");
        let inserted_data = DocBuf::from_document(&data);
        let inserted_data = Element::new(ElementType::EmbeddedDocument, inserted_data.as_bytes());

        let filter = parse_expression("name.where(given = 'Peacock')");
        let results = barn.search(patient_schema, &filter)?;
        assert_eq!(0, results.len());

        let filter = parse_expression("name.where(given = 'Duck')");
        let results = barn.search(patient_schema, &filter)?;
        assert_eq!(1, results.len());

        let mut fetched_data = results.into_iter().next().unwrap();
        fetched_data.as_object_mut().unwrap().remove("id");
        let fetched_data = to_docbuf(&fetched_data);
        let fetched_data = Element::new(ElementType::EmbeddedDocument, fetched_data.as_bytes());

        assert_eq!(SystemType::Element(inserted_data), SystemType::Element(fetched_data));

        Ok(())
    }
}
