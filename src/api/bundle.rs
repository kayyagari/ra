use bson::{Bson, Document};
use ksuid::Ksuid;
use serde::Serialize;
use serde_json::Value;

use crate::errors::RaError;

pub struct RequestBundle {
    btype: BundleType,
    entries: Vec<Entry>
}

#[derive(Serialize)]
pub struct Entry {
    req_method: Method,
    req_url: String,
    full_url: String,
    resource: Document
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub enum BundleType {
    Document,
    Message,
    Transaction,
    TransactionResponse,
    Batch,
    BatchResponse,
    History,
    SearchSet,
    Collection
}

#[derive(Serialize)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Delete,
    Patch
}

impl Method {
    pub fn from<S: AsRef<str>>(s: S) -> Result<Method, RaError> {
        use self::Method::*;
        match s.as_ref() {
            "GET" => Ok(Get),
            "HEAD" => Ok(Head),
            "POST" => Ok(Post),
            "PUT" => Ok(Put),
            "DELETE" => Ok(Delete),
            "PATCH" => Ok(Patch),
            name => Err(RaError::invalid_err(format!("unknown method name {}", name)))
        }
    }
}

impl BundleType {
    pub fn from<S: AsRef<str>>(s: S) -> Result<BundleType, RaError> {
        use self::BundleType::*;
        match s.as_ref() {
            "document" => Ok(Document),
            "message" => Ok(Message),
            "transaction" => Ok(Transaction),
            "transaction-response" => Ok(TransactionResponse),
            "batch" => Ok(Batch),
            "batch-response" => Ok(BatchResponse),
            "history" => Ok(History),
            "searchset" => Ok(SearchSet),
            "collection" => Ok(Collection),
            name => Err(RaError::invalid_err(format!("unknown bundle type {}", name)))
        }
    }
}

impl RequestBundle {
    pub fn from(mut val: Value) -> Result<RequestBundle, RaError> {
        let btype = val.get("type").unwrap().as_str().unwrap();
        let btype = BundleType::from(btype)?;

        let entries = val.get_mut("entry").unwrap();
        let mut entries = entries.as_array_mut().unwrap();
        let ref_links = RequestBundle::gather_refs(entries)?;

        let mut resources: Vec<Entry> = Vec::new();
        for item in entries {
            let full_url = item.get("fullUrl").unwrap().as_str().unwrap();
            let full_url = String::from(full_url);
            let req_url = item.pointer("/request/url").unwrap().as_str().unwrap();
            let req_url = String::from(req_url);
            let req_method = item.pointer("/request/method").unwrap().as_str().unwrap();
            let req_method = Method::from(req_method)?;
            let resource_val = item.get_mut("resource").unwrap();

            if btype == BundleType::Transaction {
                RequestBundle::replace_refs(resource_val, &ref_links);
            }

            let resource = bson::to_document(resource_val).unwrap();

            let e = Entry { full_url, req_url, req_method, resource };
            resources.push(e);
        }

        Ok(RequestBundle { btype, entries: resources })
    }

    fn gather_refs(entries: &mut Vec<Value>) -> Result<Vec<(String, String, String, String)>, RaError> {
        let mut ref_links: Vec<(String, String, String, String)> = Vec::new();

        for item in entries {
            let req_method = item.pointer("/request/method").unwrap().as_str().unwrap();
            let req_method = Method::from(req_method)?;
            let old_url = item.get("fullUrl").unwrap().as_str().unwrap().to_owned();
            let resource = item.get_mut("resource").unwrap();
            match req_method {
                Method::Post | Method::Put | Method::Patch => {
                    let new_id = Ksuid::generate().to_base62();
                    let res_type = resource.get("resourceType").unwrap().as_str();
                    if res_type.is_none() {
                        return Err(RaError::invalid_err(format!("missing resourceType in the entry with id {}", &old_url)));
                    }

                    let res_type = res_type.unwrap();
                    // only URN is supported
                    if !old_url.starts_with("urn:uuid:") {
                        return Err(RaError::invalid_err(format!("only fullUrl of type urn:uuid: is allowed, offending entry with id {}", &old_url)));
                    }

                    let old_id = old_url.split_at(9).1.to_owned();
                    let new_url = format!("{}/{}", res_type, &new_id);
                    ref_links.push((old_url, old_id, new_url, new_id.clone()));
                    resource.as_object_mut().unwrap().insert(String::from("id"), Value::String(new_id));
                },
                _ => {}
            }
        }

        Ok(ref_links)
    }

    pub fn replace_refs(v: &mut Value, refs: &Vec<(String, String, String, String)>) {
        match v {
            Value::String(s) => {
                for (old_url, old_id, new_url, new_id) in refs {
                    if s.contains(old_id) {
                        let mut tmp = s.replace(old_url, new_url);
                        if tmp.contains(old_id) {
                            tmp = tmp.replace(old_id, new_id);
                        }
                        s.clear();
                        s.push_str(&tmp);
                    }
                }
            },
            Value::Object(m) => {
                for (k, o) in m {
                    RequestBundle::replace_refs(o, refs);
                }
            },
            Value::Array(a) => {
                for i in a {
                    RequestBundle::replace_refs(i, refs);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[test]
    fn test_reference_resolution() -> Result<(), anyhow::Error> {
        let f = File::open("test_data/resources/bundle-example.json").unwrap();
        let val: Value = serde_json::from_reader(f).unwrap();
        let bundle = RequestBundle::from(val)?;
        assert_eq!(BundleType::Transaction, bundle.btype);
        let s = serde_json::to_string(&bundle.entries).unwrap();
        println!("{}", s);
        Ok(())
    }
}