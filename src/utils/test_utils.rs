use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::rc::Rc;

use bson::Document;
use bson::spec::ElementType;
use rawbson::DocBuf;
use rawbson::elem::Element;
use rocket::{Build, Config, Rocket};
use rocksdb::{DB, Options};
use serde_json::{Map, Value};
use crate::api::base::ApiBase;
use crate::api::rest;
use crate::barn::Barn;
use crate::errors::RaError;
use crate::rapath::scanner::scan_tokens;
use crate::rapath::expr::Ast;
use crate::rapath::parser::parse;
use crate::res_schema::SchemaDef;

pub struct TestContainer {
    path: PathBuf,
    initialized: RefCell<bool>
}

impl Drop for TestContainer {
    fn drop(&mut self) {
        let opts = Options::default();
        let r = DB::destroy(&opts, &self.path);
        if let Err(e) = r {
            panic!("failed to remove the folder {:?} {}", self.path, e.to_string());
        }
    }
}

impl TestContainer {
    pub fn new() -> Self {
        let path = PathBuf::from(format!("/tmp/testcontainer-{}", ksuid::Ksuid::generate().to_base62()));
        Self{path, initialized: RefCell::new(false)}
    }

    pub fn create_server_with_example_patient(&self) -> Rocket<Build> {
        if *self.initialized.borrow() {
            panic!("container was already initialized");
        }
        let db = self.setup_db().expect("initialization of database failed");
        let mut config = Config::default();
        config.address = Ipv4Addr::new(0,0,0,0).into();
        config.port = 7090;
        config.cli_colors = false;
        let api_base = ApiBase::new(db, String::from("http://localhost:7090/")).unwrap();
        let data = read_patient_example();
        api_base.create("Patient", &data).expect("failed to insert example patient record");
        *self.initialized.borrow_mut() = true;
        rest::mount(api_base, config).unwrap()
    }

    pub fn setup_api_base_with_example_patient(&self) -> ApiBase {
        if *self.initialized.borrow() {
            panic!("container was already initialized");
        }
        let db = self.setup_db().expect("initialization of database failed");
        let api_base = ApiBase::new(db, String::from("")).unwrap();
        let data = read_patient_example();
        api_base.create("Patient", &data).expect("failed to insert example patient record");
        *self.initialized.borrow_mut() = true;
        api_base
    }

    fn setup_db(&self) -> Result<Barn, RaError> {
        Barn::open_with_default_schema(&self.path)
    }

    pub fn setup_db_with_example_patient(&self) -> Result<(Barn, SchemaDef), RaError> {
        if *self.initialized.borrow() {
            panic!("container was already initialized");
        }
        let barn = self.setup_db()?;
        let sd = barn.build_schema_def()?;
        let patient_schema = sd.resources.get("Patient").unwrap();
        let data = read_patient_example();
        let data = bson::to_document(&data).unwrap();
        barn.insert(patient_schema, data, &sd, false)?;
        *self.initialized.borrow_mut() = true;
        Ok((barn, sd))
    }
}

pub fn read_patient() -> Value {
    let f = File::open("test_data/resources/patient-example-a.json").expect("file patient-example-a.json not found");
    serde_json::from_reader(f).expect("couldn't deserialize the example patient-a JSON")
}

pub fn read_bundle() -> Value {
    let f = File::open("test_data/resources/bundle-example.json").expect("file bundle-example.json not found");
    serde_json::from_reader(f).expect("deserialize the bundle-example JSON")
}

pub fn read_chained_search_bundle() -> Value {
    let f = File::open("test_data/resources/chained-search-bundle.json").expect("file chained-search-bundle.json not found");
    serde_json::from_reader(f).expect("deserialize the chained-search-bundle.json JSON")
}

// named after the file taken from FHIR examples from hl7.org
pub fn read_patient_example() -> Value {
    let f = File::open("test_data/resources/patient-example.json").expect("file patient-example.json not found");
    serde_json::from_reader(f).expect("couldn't deserialize the example patient JSON")
}

pub fn to_docbuf(val: &Value) -> DocBuf {
    let doc = bson::to_bson(val).expect("failed to convert to Bson");
    DocBuf::from_document(doc.as_document().unwrap())
}

pub fn parse_expression(s: &str) -> Ast {
    let tokens = scan_tokens(s).unwrap();
    parse(tokens).unwrap()
}

pub fn update(doc: &mut Value, pointer: &str, v: Value) {
    let mut target = doc.pointer(pointer);
    if let None = target {
        let path_parts = pointer[1..].split("/");
        let mut iter = path_parts.into_iter().peekable();
        let mut current_path = String::new();
        current_path.push('/');
        while let s = iter.next() {
            match s {
                Some(s) => {
                    let peek = iter.peek();
                    if let None = peek {
                        let parent = doc.pointer_mut(current_path.as_str()).unwrap();
                        match parent {
                            Value::Object(ref mut m) => {
                                m.insert(String::from(s), v);
                            },
                            Value::Array(ref mut arr) => {
                                arr.insert(0,v);
                            },
                            t => {
                                panic!("target should be either an Object or an Array, found {}", t);
                            }
                        }
                        break;
                    }
                    else {
                        let cp = current_path.as_str();
                        let mut tmp_path = String::from(cp);
                        if cp != "/" {
                            tmp_path.push('/');
                        }
                        tmp_path.push_str(s);
                        let t = doc.pointer(tmp_path.as_str());
                        if let None =  t {
                            let mut child = Value::Object(Map::new());
                            doc.pointer_mut(current_path.as_str()).unwrap().as_object_mut().expect("intermediate path is not an object").insert(String::from(s), child);
                        }
                        current_path.clear();
                        current_path.push_str(tmp_path.as_str());
                    }
                },
                None => break
            }
        }
    }
    else {
        let pos = &pointer.rfind('/').unwrap();
        let root = &pointer[0..*pos];
        let key = &pointer[pos+1 ..];

        let target = doc.pointer_mut(root).unwrap();

        match target {
            Value::Object(ref mut m) => {
                m.insert(String::from(key), v);
            },
            Value::Array(ref mut arr) => {
                arr.insert(0,v);
            },
            t => {
                panic!("unexpected pointer value {} for {}", t, &root);
            }
        }
    }
}