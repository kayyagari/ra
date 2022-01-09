use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::rc::Rc;

use bson::Document;
use bson::spec::ElementType;
use rawbson::DocBuf;
use rawbson::elem::Element;
use serde_json::{Map, Value};

pub fn read_patient() -> Value {
    let f = File::open("test_data/resources/patient-example-a.json").expect("couldn't read the sample patient JSON file");
    serde_json::from_reader(f).expect("couldn't deserialize the sample patient JSON")
}

pub fn to_docbuf(val: &Value) -> DocBuf {
    let doc = bson::to_bson(val).expect("failed to convert to Bson");
    DocBuf::from_document(doc.as_document().unwrap())
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