use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use bson::{Bson, Document};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::private::PathAsDisplay;
use rocksdb::{Env, DB, Options, IteratorMode, DBCompressionType, IngestExternalFileOptions};

use crate::errors::RaError;
use rawbson::DocBuf;
use std::cmp::Ordering;
use crate::res_schema::ResourceDef;
use ksuid::Ksuid;
use crate::utils;

pub struct Barn {
    env: Env,
    db: DB,
    opts: Options,
}

impl Barn {
    pub fn open_for_bulk_load<R>(db_path: PathBuf) -> Result<Barn, RaError>
        where R: Read {
        let mut opts = Self::default_db_options();
        opts.prepare_for_bulk_load();
        Self::_open(db_path, &mut opts)
    }

    pub fn open<R>(db_path: PathBuf) -> Result<Barn, RaError>
        where R: Read {
        let mut opts = Self::default_db_options();
        Self::_open(db_path, &mut opts)
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

    fn _open<R>(db_path: PathBuf, res_db_opts: &mut Options) -> Result<Barn, RaError>
    where R: Read {
        let is_mdb_ext = db_path.to_str().unwrap().ends_with(".mdb");
        if !db_path.exists() || !is_mdb_ext {
            let r = fs::create_dir_all(db_path.clone());
            match r {
                Err(e) => {
                    warn!("unable to create the db environment directory {}", db_path.as_display());
                    return Err(EnvOpenError);
                },
                Ok(_) => {
                }
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
        data.remove(&res_def.id_attr_name);
        data.insert(&res_def.id_attr_name, res_id);

        // TODO move this block to update and replace calls
        // check version history
        // let history_pk = res_def.new_history_prefix_id(ksid.as_bytes());
        // let history_count_rec = self.db.get(&history_pk);
        // let mut history_count = 1; // history number always points to the current version (and it always starts with 1)
        // if history_count_rec.is_ok() {
        //     let history_count_rec = history_count_rec.unwrap().unwrap();
        //     history_count = utils::u32_from_le_bytes(history_count_rec.as_bytes());
        // }

        // convert to BSON document
        let doc_buf = DocBuf::from_document(&*data);

        let put_result = self.db.put(&pk.to_le_bytes(), doc_buf.as_bytes());
        if let Err(e) = put_result {
            let msg = format!("unable to insert the record {}", e);
            warn!(&msg);
            return Err(RaError::InsertError(msg));
        }

        // handle references
        for ref_prop in &res_def.ref_props {
            let ref_node = data.get(ref_prop.as_str());
            if ref_node.is_some() {

            }
        }
        Ok(())
    }

    pub fn get(&self, id: u64, res_name: String) -> Result<Document, RaError> {
        let barrel = self.barrels.get(res_name.as_str());
        if let None = barrel {
            return Err(RaError::UnknownResourceName);
        }

        barrel.unwrap().get(id)
    }

    pub fn search(&self, res_name: String, expr: String, sn: Sender<Result<Vec<u8>, std::io::Error>>) -> Result<(), RaError> {
        let barrel = self.barrels.get(res_name.as_str());
        if let None = barrel {
            return Err(RaError::UnknownResourceName);
        }

        let barrel = barrel.unwrap();
        let mut cursor = barrel.db.iterator(IteratorMode::Start);

        // the first row will always be key 0 which stores the PK value, and will be skipped
        cursor.next();

        let mut count = 0;
        loop {
            let row = cursor.next();
            if None == row {
                break;
            }

            let (key, mut data) = row.unwrap();
            unsafe {
                let result = DocBuf::new_unchecked(Vec::from(data));
                count += 1;
                let beic = result.get("Business_Entities_in_Colorado");
                match beic {
                    Ok(elm) => {
                        if elm.is_some() {
                            let elm = elm.unwrap();
                            let entity_id = elm.as_document().unwrap().get("entityid");
                            if entity_id.is_ok() {
                                let entityid = entity_id.unwrap().unwrap().as_str().unwrap();
                                if entityid == "20201233700" {
                                    let send_result = sn.send(Ok(result.as_bytes().to_owned()));
                                    if let Err(e) = send_result {
                                        warn!("error received while sending search results {:?}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        warn!("failed to parse BSON document, stopping further processing {:?}", e);
                        break;
                    }
                }
            }
        }

        drop(sn);

        println!("read {} entries", count);
        Ok(())
    }

    pub fn bulk_load<R>(&self, source: R, res_name: &str, ignore_errors: bool) -> Result<(), RaError>
        where R: Read {
        let barrel = self.barrels.get(res_name);
        if let None = barrel {
            return Err(RaError::UnknownResourceName);
        }

        let barrel = barrel.unwrap();
        barrel.bulk_load(source, ignore_errors)
    }

    pub fn close(&mut self) {
        info!("closing the environment");
        for (res, b) in &self.barrels {
            for (idx_name, idx) in &b.indices {
            }
            b.db.flush();
        }
    }
}

impl Index {
    fn insert(&self, db: &mut DB, k: &Value, v: u64) -> Result<(), RaError> {
        let cf_handle = db.cf_handle(self.name.as_str()).unwrap();
        let mut put_result = Ok(());
        match self.val_type.as_str() {
            "integer" => {
                if let Some(i) = k.as_i64() {
                    put_result = db.put_cf(cf_handle, &i.to_le_bytes(), &v.to_le_bytes());
                }
            },
            "string" => {
                if let Some(s) = k.as_str() {
                    let mut key_data: Vec<u8>;
                    let match_word = self.val_format.as_str();
                    match  match_word {
                        "date-time" => {
                            key_data = schema::parse_datetime(s)?;
                        },
                        "date" => {
                            let date_with_zero_time = format!("{} 00:00:00", s);
                            let d = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S");
                            if let Err(e) = d {
                                warn!("{}", e);
                                return Err(RaError::InvalidAttributeValueError);
                            }
                            key_data = d.unwrap().timestamp_millis().to_le_bytes().to_vec();
                        },
                        _ => {
                           key_data = s.trim().to_lowercase().into_bytes();
                        }
                    }

                    put_result = db.put_cf(cf_handle, AsRef::<Vec<u8>>::as_ref(&key_data), &v.to_le_bytes());
                }
            },
            "number" => {
                if let Some(f) = k.as_f64() {
                    put_result = db.put_cf(cf_handle, &f.to_le_bytes(), &v.to_le_bytes());
                }
            },
            _ => {
                return Err(RaError::UnsupportedIndexValueType);
            }
        }

        if let Err(e) = put_result {
            return Err(RaError::TxWriteError);
        }
        Ok(())
    }
}

impl Barrel {
    fn get(&self, id: u64) -> Result<Document, RaError> {
        if id <= 0 {
            debug!("invalid resource identifier {}", id);
            return Err(RaError::ResourceNotFoundError);
        }

        let get_result = self.db.get(&id.to_le_bytes());
        match get_result {
            Err(e) => {
                debug!("resource not found with identifier {}", id);
                Err(RaError::ResourceNotFoundError)
            },
            Ok(mut data) => {
                let result = Document::from_reader(&mut data.unwrap().as_slice());
                match result {
                    Ok(val) => {
                        /*let d_obj = val.as_object_mut().unwrap();
                        let id_val;
                        match self.id_attr_type.as_str() {
                            "string" => {
                                id_val = Value::from(format!("{}", id));
                            },
                            _ => {
                                id_val = Value::from(id);
                            }
                        }
                        d_obj.insert(self.id_attr_name.clone(), id_val);*/
                        Ok(val)
                    },
                    Err(e) => {
                        warn!("failed to deserialize the resource with identifier {}", id);
                        Err(RaError::DeSerializationError)
                    }
                }
            }
        }
    }

    fn add_id_to_doc(&self, data: &mut Document, pk: u64) {
        let pk_val;
        match self.id_attr_type.as_str() {
            "string" => {
                pk_val = Bson::from(format!("{}", pk));
            },
            _ => {
                pk_val = Bson::from(pk);
            }
        }
        let pk_existing_attr = data.remove(&self.id_attr_name);
        if let Some(id_val) = pk_existing_attr {
            trace!("dropping the value {} given for ID attribute {}", &id_val, &self.id_attr_name);
        }

        data.insert(self.id_attr_name.clone(), pk_val);
    }

    fn bulk_load<R>(&self, source: R, ignore_errors: bool) -> Result<(), RaError>
    where R: Read {
        let pk_result = self.db.get(&DB_PRIMARY_KEY_KEY);
        let mut pk: u64 = 1;
        if let Ok(r) = pk_result {
            if r.is_some() {
                pk = from_le_bytes(&r.unwrap());
            }
        }

        let mut reader = BufReader::new(source);
        let mut buf: Vec<u8> = Vec::new();
        let mut count: u64 = 0;
        let sst_batch_size: u64 = 200_000;
        let mut sst_files: Vec<PathBuf> = Vec::new();
        let mut sst_temp_dir = PathBuf::new();
        sst_temp_dir.push(self.db.path());
        sst_temp_dir.push("__bulk_temp");

        let dir_result = fs::create_dir(sst_temp_dir.as_path());
        if let Err(e) = dir_result {
            warn!("failed to created the temporary directory {:?} to store SST files {:?}", sst_temp_dir.as_path(), e);
            return Err(RaError::IOError(e));
        }

        let mut sst_opts = self.opts.clone();
        sst_opts.prepare_for_bulk_load();
        //sst_opts.set_disable_auto_compactions(true);
        let cmp = |k1: &[u8], k2: &[u8]| -> Ordering {
            println!("comparing {:?} with {:?}", k1, k2);
          Ordering::Less
        };
        sst_opts.set_comparator("key-comparator", cmp);

        let mut err: Option<RaError> = None;

        let mut sst_file: rocksdb::SstFileWriter = rocksdb::SstFileWriter::create(&sst_opts);
        loop {
            let byte_count = reader.read_until(b'\n', &mut buf);
            if let Err(e) = byte_count {
                err = Some(RaError::IOError(e));
                break;
            }
            let byte_count = byte_count.unwrap();
            if byte_count <= 0 {
                break;
            }

            if count % sst_batch_size == 0 {
                if count != 0 {
                    sst_file.finish();
                }
                sst_file = rocksdb::SstFileWriter::create(&self.opts);
                let mut p = PathBuf::from(&sst_temp_dir);
                p.push(format!("file{}.sst", pk));
                let open_result = sst_file.open(&p);
                if let Err(e) = open_result {
                    warn!("failed to create new SST file {:?} {:?}", &p, e);
                    break;
                }
                sst_files.push(PathBuf::from(String::from(p.to_str().unwrap())));
            }

            let val: serde_json::Result<Value> = serde_json::from_reader(buf.as_slice());

            match val {
                Err(e) => {
                    warn!("failed to parse record {:?}", e);
                    if !ignore_errors {
                        err = Some(RaError::InvalidResourceError);
                        break;
                    }
                }

                Ok(v) => {
                    let bson_val = v.serialize(bson::Serializer::new()).unwrap();
                    let mut doc = bson_val.as_document().unwrap().to_owned();
                    self.add_id_to_doc(&mut doc, pk);
                    let doc_buf = DocBuf::from_document(&doc);

                    pk += 1;
                    //let t = time::Instant::now().elapsed().whole_milliseconds() as u64;
                    let put_result = sst_file.put(&pk.to_le_bytes(), doc_buf.as_bytes());
                    if let Err(e) = put_result {
                        warn!("failed to store record {:?}", e);
                        err = Some(RaError::TxWriteError);
                        break;
                    }

                    count += 1;

                    if count > sst_batch_size {
                        break;
                    }
                }
            }
            buf.clear();
        }

        info!("merging {} files", sst_files.len());
        if !sst_files.is_empty() {
            sst_file.finish(); // this is the last file
            let mut ingest_opts = IngestExternalFileOptions::default();
            ingest_opts.set_move_files(true);
            let ingest_result = self.db.ingest_external_file_opts(&ingest_opts, sst_files);
            if let Err(e) = ingest_result {
                warn!("failed to ingest SST files {:?}", e);
                err = Some(RaError::TxWriteError);
            }
            else {
                let put_result = self.db.put(&DB_PRIMARY_KEY_KEY, &pk.to_le_bytes());
                if let Err(e) = put_result {
                    warn!("failed to write the primary key value {} into DB", pk);
                    err = Some(RaError::TxWriteError);
                }
            }

            // info!("removing temporary directory");
            // let _ = fs::remove_dir_all(sst_temp_dir);
        }

        if err.is_some() {
            return Err(err.unwrap());
        }

        info!("inserted {} records", count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_barn() {
        let env_dir = PathBuf::from("/tmp/barn");
        let schema_file = fs::File::open("config/schema.json").unwrap();
        let db_conf_file = fs::File::open("config/db-conf.json").unwrap();
        let db_conf = serde_json::from_reader(db_conf_file).unwrap();

        let result = Barn::open(env_dir, &db_conf, schema_file);
        match result {
            Ok(ref b) => {
                let dir = fs::read_dir(Path::new(&env_dir));
                match dir {
                    Ok(mut f) => {
                        let mut actual = 0;
                        f.all(|n| { actual = actual +1; true});
                        assert_eq!(2, actual);
                    },
                    _ => {
                        assert!(false);
                    }
                }
            },
            Err(ref e) => {
                println!("{:#?}", e);
                assert!(false);
            }
        }

        let barn = result.unwrap();
        for dr in &db_conf.resources {
            let barrel = barn.barrels.get(dr.0);
            if let None =  barrel {
                println!("database for resource {} not found", dr.0);
                assert!(false);
            }

            for i in &dr.1.indices {
                let index_name = format!("{}_{}", dr.0, i.attr_path);
                let index = barrel.unwrap().indices.get(&index_name);
                if let None = index {
                    println!("database for index {} not found", &index_name);
                    assert!(false);
                }
            }
        }
    }
}
