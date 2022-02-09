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
use rawbson::elem::ElementType;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use ksuid::Ksuid;
use lazy_static::lazy_static;
use log::{debug, error, info, trace, warn};
use rawbson::de::BsonDeserializer;
use rawbson::DocBuf;
use rawbson::elem::Element;
use rocksdb::{DB, DBCompressionType, Env, Options, WriteBatch};
use serde_json::Value;
use thiserror::private::PathAsDisplay;
use crate::api::bundle::SearchSet;

use crate::errors::{EvalError, RaError};
use crate::rapath::engine::eval;
use crate::rapath::expr::Ast;
use crate::rapath::stypes::SystemType;
use crate::res_schema::{parse_res_def, ResourceDef, SchemaDef};
use crate::resources::{get_default_schema_bytes, parse_compressed_json};
use crate::utils;
use crate::utils::{get_crc_hash, prefix_id};

mod insert;

const RA_METADATA_KEY_PREFIX: &str = "_____RA_METADATA_KEY_PREFIX_____";

lazy_static! {
 static ref SCHEMA_ID: [u8; 24] = {
        let schema_id = Ksuid::from_base62("246MsJFiHFB6TxLOmZhJlwPAM1k").unwrap();
        let prefix = get_crc_hash(RA_METADATA_KEY_PREFIX);
        prefix_id(&prefix, schema_id.as_bytes())
    };
}

pub struct Barn {
    env: Env,
    db: DB,
    opts: Options
}

impl Barn {
    pub fn open(db_path: &PathBuf) -> Result<Barn, RaError> {
        let mut opts = Self::default_db_options();
        Barn::_open(db_path, &mut opts)
    }

    pub fn open_with_default_schema(db_path: &PathBuf) -> Result<Barn, RaError> {
        let b = Barn::open(db_path)?;
        b.store_schema(get_default_schema_bytes())?;
        Ok(b)
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
                let msg = format!("unable to create the database environment directory {}", db_path.as_display());
                warn!("{}", &msg);
                return Err(RaError::DbError(msg));
            }
        }

        let env = Env::default().unwrap();
        info!("opened database environment");
        res_db_opts.set_env(&env);
        let mut res_db = DB::open(res_db_opts, &db_path).unwrap();

        let b = Barn {
            env,
            db: res_db,
            opts: res_db_opts.clone()
        };

        Ok(b)
    }

    pub fn store_schema(&self, data: &[u8]) -> Result<(), RaError> {
        info!("storing schema");
        let result = self.db.put(&*SCHEMA_ID, data);
        if let Err(e) = result {
            let msg = format!("failed to store schema in database {}", e);
            warn!("{}", &msg);
            return Err(RaError::DbError(msg));
        }

        Ok(())
    }

    pub fn read_schema(&self) -> Result<Value, RaError> {
        info!("reading schema from database");
        let schema_data = self.db.get(&*SCHEMA_ID);
        if let Err(e) = schema_data {
            let msg = "unable to read the schema data from database";
            warn!("{}", msg);
            return Err(RaError::DbError(String::from(msg)));
        }

        let schema_data = schema_data.unwrap();

        if let Some(schema_data) = schema_data {
            let value = parse_compressed_json(schema_data.as_slice())?;
            return Ok(value);
        }

        Err(RaError::DbError(String::from("schema entry exists but there is no data")))
    }

    pub fn insert(&self, res_def: &ResourceDef, mut data: Document, sd: &SchemaDef) -> Result<Document, RaError> {
        let ksid = Ksuid::generate();
        let mut wb = WriteBatch::default();
        let doc = self.insert_batch(&ksid, res_def, data, &mut wb, sd)?;
        let result = self.db.write(wb);
        if let Err(e) = result {
            let msg = format!("unable to insert the record {}", e);
            warn!("{}", &msg);
            return Err(RaError::DbError(msg));
        }

        Ok(doc)
    }



    // pub fn get(&self, id: u64, res_name: String) -> Result<Document, RaError> {
    // }

    pub fn search<'a>(&self, res_def: &ResourceDef, filter: &'a Ast<'a>) -> Result<SearchSet, EvalError> {
        let mut results = SearchSet::new();

        let mut count = 0;
        //let start = Instant::now();
        let prefix = &res_def.hash;
        let mut inner = self.db.prefix_iterator(prefix);
        for (k, v) in inner {
            if !k.starts_with(prefix) {
                break;
            }
            count += 1;
            let e = Element::new(ElementType::EmbeddedDocument, v.as_ref());
            let st = Rc::new(SystemType::Element(e));
            let pick = eval(&filter, st)?;
            if pick.is_truthy() && count < 20 {
                let de = BsonDeserializer::from_rawbson(e);
                let val: Document = rawbson::de::from_doc(e.as_document().unwrap())?;
                results.add(val);
            }
        }
        //let elapsed = start.elapsed().as_secs();
        //println!("time took to search through {} records {}", count, elapsed);
        Ok(results)
    }

    pub fn save_batch(&self, wb: WriteBatch) -> Result<(), RaError> {
        debug!("saving batch");
        self.db.write(wb)?;
        Ok(())
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
        let path = PathBuf::from("/tmp/testdb");
        std::fs::remove_dir_all(&path);
        let barn = Barn::open_with_default_schema(&path)?;
        let sd = parse_res_def(&barn.read_schema()?)?;
        let patient_schema = sd.resources.get("Patient").unwrap();
        let data = read_patient();
        let data = bson::to_document(&data).unwrap();
        let mut data = barn.insert(patient_schema, data, &sd)?;
        data.remove("id");
        let inserted_data = DocBuf::from_document(&data);
        let inserted_data = Element::new(ElementType::EmbeddedDocument, inserted_data.as_bytes());

        let filter = parse_expression("name.where(given = 'Peacock')");
        let results = barn.search(patient_schema, &filter)?;
        assert_eq!(0, results.len());

        let filter = parse_expression("name.where(given = 'Duck')");
        let results = barn.search(patient_schema, &filter)?;
        assert_eq!(1, results.len());

        let mut fetched_data = results.entries.into_iter().next().unwrap().resource;
        fetched_data.remove("id");
        let fetched_data = DocBuf::from_document(&fetched_data);
        let fetched_data = Element::new(ElementType::EmbeddedDocument, fetched_data.as_bytes());

        assert_eq!(SystemType::Element(inserted_data), SystemType::Element(fetched_data));

        Ok(())
    }
}
