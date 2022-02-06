use bson::{Bson, Document};
use chrono::{DateTime, Utc};
use crate::errors::RaError;

pub fn get_str<'a>(doc: &'a Document, path: &str) -> &'a str {
    let o = get(doc, path);
    if let Some(o) = o {
        let val = o.as_str();
        if let Some(val) = val {
            return val;
        }
    }

    ""
}

pub fn get_time<'a>(doc: &'a Document, path: &str) -> Option<&'a DateTime<Utc>> {
    let o = get(doc, path);
    if let Some(o) = o {
        let val = o.as_datetime();
        if let Some(val) = val {
            return Some(val);
        }
    }

    None
}

pub fn get_int(doc: &Document, path: &str) -> i64 {
    let o = get(doc, path);
    if let Some(o) = o {
        let val = o.as_i64();
        if let Some(val) = val {
            return val;
        }
        else {
            let val = o.as_i32();
            if let Some(val) = val {
                return val as i64;
            }
        }
    }

    -1
}

fn get<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split(".");
    let mut o = doc.get(parts.next().unwrap());
    for s in parts {
        if let None = o {
            break;
        }
        o = find_key(o, s);
    }
    o
}

fn find_key<'a>(o: Option<&'a Bson>, k: &str) -> Option<&'a Bson> {
    let o = o.unwrap();
    match o {
        Bson::Document(d) => d.get(k),
        _ => None
    }
}

#[cfg(test)]
mod tests {
    use bson::bson;
    use super::*;

    #[test]
    fn test_get() {

        let t = "2022-02-06T11:45:00Z".parse::<DateTime<Utc>>().unwrap();
        let doc = bson!({"id": "abcd", "meta": { "versionId": 1, "lastUpdated": t.clone()}});
        let doc = doc.as_document().unwrap();
        assert_eq!("abcd", get_str(doc, "id"));
        assert_eq!(1, get_int(doc, "meta.versionId"));

        let last_modified = get_time(&doc, "meta.lastUpdated").unwrap();
        //Last-Modified: <day-name>, <day> <month> <year> <hour>:<minute>:<second> GMT
        let last_modified = last_modified.format("%a, %d %m %Y %H:%M:%S GMT").to_string();
        assert_eq!("Sun, 06 02 2022 11:45:00 GMT", last_modified);
    }
}