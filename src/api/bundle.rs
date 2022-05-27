use std::cmp::Ordering;
use std::collections::HashMap;
use bson::{Document};
use ksuid::Ksuid;
use serde::{Serialize, Serializer};
use serde::ser::{SerializeMap, SerializeStruct};
use serde_json::Value;

use crate::errors::RaError;
use crate::utils::bson_utils::get_str;

pub struct RequestBundle {
    pub btype: BundleType,
    pub entries: Vec<RequestEntry>
}

pub struct SearchSet {
    pub(crate) entries: Vec<SearchEntry>
}

pub struct SearchEntry {
    pub resource: Document,
    pub mode: SearchEntryMode
}

pub enum SearchEntryMode {
    Match,
    Include,
    Outcome
}

#[derive(Serialize)]
pub struct RequestEntry {
    pub req_method: Method,
    pub req_url: String,
    pub full_url: String,
    pub resource: Document,
    #[serde(skip_serializing)]
    pub ra_id: Ksuid
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

/// the values of this enum are placed in the respective positions
/// based on the transaction processing rules of a bundle so that
/// entries can be sorted using the Vec.sort() function
/// DO NOT alter the positions of values in this enum
#[derive(Debug, Serialize, Ord, PartialOrd, Eq, PartialEq)]
pub enum Method {
    Delete,
    Post,
    Put,
    Patch,
    Get,
    Head
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
            name => Err(RaError::bad_req(format!("unknown method name {}", name)))
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
            name => Err(RaError::bad_req(format!("unknown bundle type {}", name)))
        }
    }
}

impl SearchSet {
    pub fn new() -> Self {
        Self{entries: Vec::new()}
    }

    pub fn add(&mut self, d: Document) {
        self.entries.push(SearchEntry{resource: d, mode: SearchEntryMode::Match});
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl RequestBundle {
    pub fn from(mut val: Value) -> Result<RequestBundle, RaError> {
        let btype = val.get("type").unwrap().as_str().unwrap();
        let btype = BundleType::from(btype)?;

        let entries = val.get_mut("entry").unwrap();
        let mut entries = entries.as_array_mut().unwrap();
        let ref_links = RequestBundle::gather_refs(entries)?;

        let mut resources: Vec<RequestEntry> = Vec::new();
        for item in entries {
            let full_url = item.get("fullUrl").unwrap().as_str().unwrap();
            let full_url = String::from(full_url);
            let req_url = item.pointer("/request/url").unwrap().as_str().unwrap();
            let req_url = String::from(req_url);
            let req_method = item.pointer("/request/method").unwrap().as_str().unwrap();
            let req_method = Method::from(req_method)?;
            let resource_val = item.get_mut("resource").unwrap();

            if btype == BundleType::Transaction {
                RequestBundle::replace_refs(resource_val, &ref_links, "");
            }

            let ra_id = resource_val.get("id");
            if let None = ra_id {
                return Err(RaError::bad_req(format!("missing id attribute in the resource with fullUrl {}", full_url)));
            }
            let ra_id = ra_id.unwrap().as_str();
            if let None = ra_id {
                return Err(RaError::bad_req(format!("no id found in the resource with fullUrl {}", full_url)));
            }
            let ra_id = Ksuid::from_base62(ra_id.unwrap());
            if let Err(e) = ra_id {
                return Err(RaError::bad_req(format!("invalid id found in the resource with fullUrl {}", full_url)));
            }
            let ra_id = ra_id.unwrap();

            let resource = bson::to_document(resource_val).unwrap();

            let e = RequestEntry { full_url, req_url, req_method, resource, ra_id };
            resources.push(e);
        }

        if btype == BundleType::Transaction {
            resources.sort_unstable();
        }

        Ok(RequestBundle { btype, entries: resources })
    }

    fn gather_refs(entries: &mut Vec<Value>) -> Result<Vec<(String, String, String, String)>, RaError> {
        let mut ref_links: Vec<(String, String, String, String)> = Vec::new();

        for item in entries {
            let req_method = item.pointer("/request/method").unwrap().as_str().unwrap();
            let req_method = Method::from(req_method)?;
            let old_url = item.get("fullUrl").unwrap().as_str().unwrap().to_owned();
            let resource = item.get_mut("resource");
            if let None = resource {
                return Err(RaError::bad_req(format!("missing resource in the entry with fullUrl {}", &old_url)));
            }
            let resource = resource.unwrap();
            let res_name = resource.get("resourceType").unwrap().as_str().unwrap();
            match req_method {
                Method::Post | Method::Put | Method::Patch => {
                    let new_id = Ksuid::generate().to_base62();
                    let res_type = resource.get("resourceType").unwrap().as_str();
                    if res_type.is_none() {
                        return Err(RaError::bad_req(format!("missing resourceType in the entry with fullUrl {}", &old_url)));
                    }

                    let res_type = res_type.unwrap();
                    let mut old_id= None;
                    if old_url.starts_with("urn:uuid:") {
                        old_id = Some(old_url.split_at(9).1.to_owned());
                    }
                    else {
                        let url_val = url::Url::parse(&old_url);
                        if let Err(e) = url_val {
                            return Err(RaError::bad_req(format!("invalid URL in fullUrl {}", &old_url)));
                        }
                        let url_val = url_val.unwrap();
                        let delim = format!("/{}/", res_name);
                        let url_path = url_val.path();
                        let mut parts = url_path.splitn(2, delim.as_str());
                        if let Some(_) = parts.next() {
                            if let Some(id) = parts.next() {
                                old_id = Some(format!("{}/{}", res_name, id));
                            }
                        }
                    }

                    if let None = old_id {
                        return Err(RaError::bad_req(format!("couldn't extract ID from the fullUrl {}", &old_url)));
                    }

                    let new_url = format!("{}/{}", res_type, &new_id);
                    ref_links.push((old_url, old_id.unwrap(), new_url, new_id.clone()));
                    resource.as_object_mut().unwrap().insert(String::from("id"), Value::String(new_id));
                },
                _ => {}
            }
        }

        Ok(ref_links)
    }

    pub fn replace_refs(v: &mut Value, refs: &Vec<(String, String, String, String)>, key: &str) {
        match v {
            Value::String(s) => {
                if key == "div" || key == "reference" {
                    for (old_url, old_id, new_url, new_id) in refs {
                        if s.contains(old_id) {
                            let mut tmp = s.replace(old_url, new_url);
                            if tmp.contains(old_id) {
                                tmp = tmp.replace(old_id, new_url);
                            }
                            s.clear();
                            s.push_str(&tmp);
                        }
                    }
                }
            },
            Value::Object(m) => {
                for (k, o) in m {
                    RequestBundle::replace_refs(o, refs, k);
                }
            },
            Value::Array(a) => {
                for i in a {
                    RequestBundle::replace_refs(i, refs, key);
                }
            }
            _ => {}
        }
    }
}

impl Eq for RequestEntry {}
impl PartialEq for RequestEntry {
    fn eq(&self, other: &Self) -> bool {
        self.full_url.eq(&other.full_url)
    }
}

impl Ord for RequestEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.req_method.cmp(&other.req_method)
    }
}

impl PartialOrd for RequestEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        //Some(self.req_method.cmp(&other.req_method))
        Some(self.cmp(other))
    }
}

impl Serialize for SearchSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut state = serializer.serialize_struct("", 4)?;
        state.serialize_field("resourceType", "Bundle");
        state.serialize_field("type", "searchset");
        state.serialize_field("id", &uuid::Uuid::new_v4().to_string());
        state.serialize_field("count", &self.entries.len());

        state.serialize_field("entries", &self.entries);
        state.end()
    }
}

impl Serialize for SearchEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut state = serializer.serialize_map(Some(2))?;
        let res_name = get_str(&self.resource, "resourceType");
        let id = get_str(&self.resource,"id");
        // TODO add the prefix the baseurl
        let full_url = format!("{}/{}", res_name, id);
        state.serialize_entry("fullUrl", full_url.as_str());
        state.serialize_entry("resource", &self.resource);

        //let mode = HashMap::new();
        state.serialize_key("search");
        state.serialize_value(&self.mode);

        state.end()
    }
}

impl Serialize for SearchEntryMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut state = serializer.serialize_map(Some(1))?;
        state.serialize_key("mode");
        match self {
            SearchEntryMode::Match => {
                state.serialize_value("match");
            },
            SearchEntryMode::Include => {
                state.serialize_value("include");
            },
            SearchEntryMode::Outcome => {
                state.serialize_value("outcome");
            }
        }

        state.end()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use bson::doc;

    use super::*;

    #[test]
    fn test_reference_resolution() -> Result<(), anyhow::Error> {
        let mut candidates = Vec::new();
        candidates.push("test_data/resources/bundle-example.json");
        candidates.push("test_data/resources/chained-search-bundle.json");
        for c in candidates {
            let f = File::open(c).unwrap();
            let val: Value = serde_json::from_reader(f).unwrap();
            let bundle = RequestBundle::from(val)?;
            assert_eq!(BundleType::Transaction, bundle.btype);
            let s = serde_json::to_string(&bundle.entries).unwrap();
            println!("{}", s);
        }
        Ok(())
    }

    #[test]
    fn test_serialize_searchset() -> Result<(), anyhow::Error> {
        let mut ss = SearchSet::new();
        ss.add(doc! {"id": "id", "k1": "v1"});
        let val = serde_json::to_string(&ss)?;
        println!("{}", &val);
        assert!(val.len() > 0);
        Ok(())
    }
}