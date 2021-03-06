use std::borrow::Borrow;
use std::cell::{Ref, RefCell};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::time::Instant;

use bson::{Bson, bson, Document};
use rawbson::elem::ElementType;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use ksuid::Ksuid;
use lazy_static::lazy_static;
use log::{debug, error, info, trace, warn};
use rawbson::de::BsonDeserializer;
use rawbson::{Doc, DocBuf};
use rawbson::elem::Element;
use rocksdb::{DB, DBCompressionType, DBIterator, DBPinnableSlice, Env, Options, WriteBatch};
use serde_json::Value;
use thiserror::private::PathAsDisplay;
use crate::api::bundle::SearchSet;

use crate::errors::{EvalError, RaError};
use crate::rapath::engine::{eval, ExecContext, UnresolvableExecContext};
use crate::rapath::EvalResult;
use crate::rapath::expr::Ast;
use crate::rapath::stypes::SystemType;
use crate::res_schema::{parse_res_def, parse_search_param, ResourceDef, SchemaDef};
use crate::utils::resources::{get_default_schema_bytes, get_default_search_param_bytes, parse_compressed_json};
use crate::utils;
use crate::utils::{bson_utils, get_crc_hash, prefix_id};

mod insert;

const RA_METADATA_KEY_PREFIX: &str = "_____RA_METADATA_KEY_PREFIX_____";
pub(crate) const CF_INDEX: &str = "index";

lazy_static! {
 static ref SCHEMA_ID: [u8; 24] = {
        let schema_id = Ksuid::from_base62("246MsJFiHFB6TxLOmZhJlwPAM1k").unwrap();
        let prefix = get_crc_hash(RA_METADATA_KEY_PREFIX);
        prefix_id(&prefix, schema_id.as_bytes())
    };

 static ref SEARCH_PARAM_RESOURCE_KEY_PREFIX: [u8; 4] = get_crc_hash("SearchParameter");
}

pub struct Barn {
    env: Env,
    db: DB,
    opts: Options
}

/// used only for internal testing purpose
/// this iterator copies the memory before returning an item
pub struct ResourceIterator<'d> {
    inner: DBIterator<'d>,
    prefix: &'d[u8]
}

pub struct IndexIterator<'d> {
    inner: DBIterator<'d>,
    prefix: &'d[u8]
}

pub struct IndexRow<'a> {
    key: &'a [u8],
    norm_val: Option<&'a [u8]>,
    value: &'a [u8]
}

pub struct ResolvableContext<'b> {
    root: Rc<SystemType<'b>>,
    db: &'b Barn,
    sd: &'b SchemaDef
}

impl<'b> ExecContext<'b> for ResolvableContext<'b> {
    fn root_resource(&self) -> Rc<SystemType<'b>> {
        Rc::clone(&self.root)
    }

    fn resolve(&'b self, relative_url: &str) -> Result<Vec<u8>, EvalError> {
        self.db.resolve(relative_url, self.sd)
    }
}

impl<'b> ResolvableContext<'b> {
    pub fn new(root: Rc<SystemType<'b>>, db: &'b Barn, sd: &'b SchemaDef) -> Self {
        ResolvableContext{root, db, sd}
    }
}

impl Barn {
    pub fn open(db_path: &PathBuf) -> Result<Barn, RaError> {
        let mut opts = Self::default_db_options();
        Barn::_open(db_path, &mut opts)
    }

    pub fn open_with_default_schema(db_path: &PathBuf) -> Result<Barn, RaError> {
        let b = Barn::open(db_path)?;
        b.store_schema(get_default_schema_bytes())?;
        b.store_default_search_params()?;
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
        let mut res_db = DB::open_cf(res_db_opts, &db_path, &[CF_INDEX]).unwrap();
        let b = Barn {
            env,
            db: res_db,
            opts: res_db_opts.clone()
        };

        Ok(b)
    }

    pub fn store_schema(&self, data: &[u8]) -> Result<(), RaError> {
        let s_val = self.db.get(&*SCHEMA_ID)?;
        if s_val.is_none() {
            info!("storing default bundled schema");
            let result = self.db.put(&*SCHEMA_ID, data);
            if let Err(e) = result {
                let msg = format!("failed to store schema in database {}", e);
                warn!("{}", &msg);
                return Err(RaError::DbError(msg));
            }
        }

        Ok(())
    }

    fn store_default_search_params(&self) -> Result<(), RaError> {
        let prefix = &*SEARCH_PARAM_RESOURCE_KEY_PREFIX;
        let mut itr = self.db.prefix_iterator(prefix);
        if itr.next().is_none() {
            info!("storing default search parameter resources");
            let data = get_default_search_param_bytes();
            let params = parse_compressed_json(data)?;
            let params = params.get("entry").unwrap().as_array().unwrap();
            let mut wb = WriteBatch::default();
            for p in params {
                let p = p.get("resource").unwrap();
                let doc = bson::to_document(p)?;
                let _ = self.insert_search_param_batch(prefix, doc, &mut wb)?;
            }

            let result = self.db.write(wb);
            if let Err(e) = result {
                let msg = format!("unable to insert default search parameter resources {}", e);
                warn!("{}", &msg);
                return Err(RaError::DbError(msg));
            }
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

    fn get_search_params(&self) -> ResourceIterator {
        let prefix = &*SEARCH_PARAM_RESOURCE_KEY_PREFIX;
        let inner: rocksdb::DBIterator = self.db.prefix_iterator(prefix);
        ResourceIterator{inner, prefix}
    }

    pub fn build_schema_def(&self) -> Result<SchemaDef, RaError> {
        let schema = self.read_schema()?;
        let mut schema = parse_res_def(&schema)?;
        let iter = self.get_search_params();
        for doc in iter {
            let spd = parse_search_param(&doc, &schema)?;
            schema.add_search_param(spd);
        }
        Ok(schema)
    }

    pub fn get_resource_iter<'d>(&'d self, rd: &'d ResourceDef) -> ResourceIterator {
        let prefix = &rd.hash;
        let inner: rocksdb::DBIterator = self.db.prefix_iterator(prefix);
        ResourceIterator{inner, prefix}
    }

    pub fn insert(&self, res_def: &ResourceDef, mut data: Document, sd: &SchemaDef, skip_indexing: bool) -> Result<Document, RaError> {
        let ksid = Ksuid::generate();
        let mut wb = WriteBatch::default();
        let (doc, _, _) = self.insert_batch(&ksid, res_def, data, &mut wb, sd, skip_indexing)?;
        let result = self.db.write(wb);
        if let Err(e) = result {
            let msg = format!("unable to insert the record {}", e);
            warn!("{}", &msg);
            return Err(RaError::DbError(msg));
        }

        Ok(doc)
    }



    pub fn get_resource_by_pk(&self, pk: &[u8; 24]) -> Result<Option<DBPinnableSlice>, RaError> {
        let res = self.db.get_pinned(pk);
        if let Err(e) = res {
            let msg = "unable to read the resource with the given primary key from database";
            warn!("{}", msg);
            return Err(RaError::DbError(String::from(msg)));
        }

        Ok(res.unwrap())
    }

    pub fn search<'a>(&self, res_def: &ResourceDef, filter: &'a Ast<'a>) -> Result<SearchSet, EvalError> {
        let mut results = SearchSet::new();

        let mut count = 0;
        let mut total = 0;
        let start = Instant::now();
        let prefix = &res_def.hash;
        let mut inner = self.db.prefix_iterator(prefix);
        for (k, v) in inner {
            if !k.starts_with(prefix) {
                break;
            }

            total += 1;
            let e = Element::new(ElementType::EmbeddedDocument, v.as_ref());
            let st = Rc::new(SystemType::Element(e));
            let mut ctx = UnresolvableExecContext::new(Rc::clone(&st));
            let pick = eval(&mut ctx, &filter, st)?;
            if pick.is_truthy() && count < 20 {
                count += 1;
                let de = BsonDeserializer::from_rawbson(e);
                let val: Document = rawbson::de::from_doc(e.as_document().unwrap())?;
                results.add(val);
            }
        }
        let elapsed = start.elapsed().as_secs();
        println!("searched through {} records {}", total, elapsed);
        Ok(results)
    }

    pub fn save_batch(&self, wb: WriteBatch) -> Result<(), RaError> {
        debug!("saving batch");
        self.db.write(wb)?;
        Ok(())
    }

    fn insert_search_param_batch(&self, prefix: &[u8; 4], mut data: Document, wb: &mut WriteBatch) -> Result<Document, RaError> {
        let res_id = Ksuid::generate();
        data.insert("id", Bson::from(res_id.to_base62()));

        // update metadata
        let mut meta = data.get_mut("meta");
        if let None = meta {
            data.insert("meta", bson!({}));
            meta = data.get_mut("meta");
        }

        let mut meta = meta.unwrap().as_document_mut().unwrap();
        meta.insert("versionId", Bson::from(1));
        // this has to be inserted as a string otherwise when serialized to JSON
        // dates are formatted in extended-JSON format
        meta.insert("lastUpdated", Bson::from(Utc::now().format(bson_utils::DATE_FORMAT).to_string()));

        let mut vec_bytes = Vec::new();
        data.to_writer(&mut vec_bytes);

        let pk = prefix_id(prefix, res_id.as_bytes());
        wb.put(&pk, vec_bytes.as_slice());

        Ok(data)
    }

    pub fn new_index_iter<'d>(&'d self, search_param_hash: &'d [u8]) -> DBIterator<'d> {
        let cf = self.db.cf_handle(CF_INDEX).unwrap();
        self.db.prefix_iterator_cf(cf, search_param_hash)
        //IndexIterator{prefix:search_param_hash, inner}
    }

    pub fn resolve(&self, relative_url: &str, sd: &SchemaDef) -> Result<Vec<u8>, EvalError> {
        let mut parts = relative_url.splitn(2, "/");
        let res_name = parts.next();
        if let None = res_name {
            return Err(EvalError::new(format!("invalid reference URL {}", relative_url)));
        }
        let res_name = res_name.unwrap();
        let rd = sd.resources.get(res_name);
        if let None = rd {
            return Err(EvalError::new(format!("invalid resource type {} in URL {}", res_name, relative_url)));
        }

        let res_id = parts.next();
        if let None = res_id {
            return Err(EvalError::new(format!("invalid reference URL {}", relative_url)));
        }

        let res_id = res_id.unwrap();
        let id = Ksuid::from_base62(res_id);
        if let Err(e) = id {
            return Err(EvalError::new(format!("invalid resource ID {} in URL {}", res_id, relative_url)));
        }

        let mut pk = [0; 24];
        pk[..4].copy_from_slice(&rd.unwrap().hash);
        pk[4..].copy_from_slice(id.unwrap().as_bytes());

        let res_data = self.db.get(pk);
        if let Err(e) = res_data {
            return Err(EvalError::new(format!("couldn't resolve the relative URL {} ({})", relative_url, e.to_string())));
        }

        let res_data = res_data.unwrap();
        if let None = res_data {
            return Err(EvalError::new(format!("resource key exists but has no data associated with it. Reference URL {}", relative_url)));
        }

        Ok(res_data.unwrap())
    }
}

impl Iterator for ResourceIterator<'_> {
    type Item = Document;

    fn next(&mut self) -> Option<Self::Item> {
        let kv = self.inner.next();
        if let Some(mut kv) = kv {
            if !kv.0.starts_with(self.prefix) {
                return None;
            }
            // a rawbson::DocBuf can be returned but the accessor
            // methods on it return Option and it is a bit of inconvenience.
            // trading this inconvenience for copying of memory
            let mut c = Cursor::new(kv.1);
            let doc = Document::from_reader(&mut c);
            if let Err(e) = doc {
                warn!("invalid document data {}", e.to_string());
                return None;
            }
            return Some(doc.unwrap());
        }
        None
    }
}

/* will only be possible once GATs are available in the next version of Rust
impl Iterator for IndexIterator<'_> {
    type Item<'a> = IndexRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.inner.next();
        if let None = row {
            return None;
        }
        let row = row.unwrap();
        let row_prefix = &row.0[..4];
        if row_prefix != self.prefix {
            return None;
        }

        let pos = row.0.len() - 24;
        let hasVal = row.0[4] == 1;
        let mut norm_val_in_key = None;
        if hasVal {
            norm_val_in_key = Some(&row.0[5..pos]);
        }

        let res_key = &row.0[pos..];
        let ir = IndexRow{key: res_key, norm_val: norm_val_in_key, value: row.1.as_ref()};
        Some(ir)
    }
}
*/
#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::res_schema::parse_res_def;
    use crate::utils::test_utils::{parse_expression, read_patient, to_docbuf};

    use super::*;

    #[test]
    fn test_search() -> Result<(), anyhow::Error> {
        let path = PathBuf::from("/tmp/testdb1");
        std::fs::remove_dir_all(&path);
        let barn = Barn::open_with_default_schema(&path)?;
        let sd = parse_res_def(&barn.read_schema()?)?;
        let patient_schema = sd.resources.get("Patient").unwrap();
        let data = read_patient();
        let data = bson::to_document(&data).unwrap();
        let mut data = barn.insert(patient_schema, data, &sd, false)?;
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

        let lhs = Rc::new(SystemType::Element(inserted_data));
        let rhs = Rc::new(SystemType::Element(fetched_data));
        let equals_result = SystemType::equals(lhs, rhs)?;
        assert_eq!(true, equals_result.as_bool().unwrap());

        std::fs::remove_dir_all(&path);
        Ok(())
    }
}
