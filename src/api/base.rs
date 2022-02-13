use std::fmt::format;
use bson::Document;
use log::{debug, warn};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use rocket::request::{FromRequest, Outcome};
use std::convert::Infallible;
use rocket::Request;

use crate::api::bundle;
use crate::api::bundle::{BundleType, Method, RequestBundle, SearchSet};
use crate::barn::Barn;
use crate::errors::{EvalError, IssueSeverity, IssueType, RaError};
use crate::rapath::expr::Ast;
use crate::rapath::parser::parse;
use crate::rapath::scanner::scan_tokens;
use crate::res_schema::{parse_res_def, SchemaDef};
use crate::ResourceDef;
use crate::utils::test_utils::parse_expression;

pub struct ApiBase {
    db: Barn,
    schema: SchemaDef
}

pub enum RaResponse {
    Success,
    Created(Document),
    SearchResult(SearchSet)
}

pub struct ConditionalHeaders<'r> {
    pub if_none_exist: Option<&'r str>,
}

#[derive(Serialize, Deserialize)]
pub struct OperationOutcome {
    #[serde(rename="resourceType")]
    rtype: &'static str,
    text: Narrative,
    issue: Vec<Box<BackboneElement>>
}

#[derive(Serialize, Deserialize)]
struct	BackboneElement {
    severity: IssueSeverity,
    code: IssueType,
    diagnostics: String,
    //expression: Option<String>
}

#[derive(Serialize, Deserialize)]
struct Narrative {
    status: NarrativeStatus,
    div: String
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum NarrativeStatus {
    Generated,
    Extensions,
    Additional,
    Empty
}

#[derive(Debug)]
pub struct SearchQuery<'r> {
    pub params: Vec<(&'r str, &'r str)>
}


#[derive(Debug, Eq, PartialEq)]
pub enum ReturnContent {
    Minimal,
    Representation,
    OperationOutcome
}

impl ReturnContent {
    pub fn from<S: AsRef<str>>(s: S) -> Self {
        match s.as_ref() {
            // using 'return=' prefix to avoid a call to sub-string on the header value
            "return=minimal" => ReturnContent::Minimal,
            "return=representation" => ReturnContent::Representation,
            "return=OperationOutcome" => ReturnContent::OperationOutcome,
            _ => ReturnContent::Minimal
        }
    }
}

#[derive(Debug)]
pub struct ResponseHints {
    pub rturn: ReturnContent,
    pub pretty: bool,
    pub summary: bool,
    pub elements: bool,
}

impl ResponseHints {
    pub fn default() -> Self {
        ResponseHints{rturn: ReturnContent::Minimal, pretty: false, elements: false, summary: false}
    }
}

impl OperationOutcome {
    pub fn new_error<S: AsRef<str>>(code: IssueType, msg: S) -> Self {
        let r = msg.as_ref();
        let div = format!(r#"<div xmlns="http://www.w3.org/1999/xhtml"><h1>Operation Outcome</h1><span>{}</span></div>"#, r);
        let text = Narrative{status: NarrativeStatus::Generated, div};
        let i1 = BackboneElement{severity: IssueSeverity::Error, code, diagnostics: r.to_string()};
        let issue = vec![Box::new(i1)];
        OperationOutcome{issue, text, rtype: "OperationOutcome"}
    }

    pub fn serialize(&self) -> String {
        let r = serde_json::to_string(self);
        if let Err(e) = r {
            let msg = "failed to serialize the OperationOutcome";
            warn!("{}", msg);
            return String::from(msg);
        }
        r.unwrap()
    }
}

impl ApiBase {
    pub fn new(db: Barn) -> Result<Self, RaError> {
        let schema = db.read_schema()?;
        let schema = parse_res_def(&schema)?;
        Ok(ApiBase{db, schema})
    }

    fn transaction(&self, val: Value) -> Result<RaResponse, RaError> {
        debug!("validating the transaction bundle");
        self.schema.validate(&val)?;
        let req_bundle = RequestBundle::from(val)?;
        debug!("processing transaction bundle");
        let mut wb = WriteBatch::default();
        for e in req_bundle.entries {
            match e.req_method {
                Method::Delete => {

                },
                Method::Post => {
                    let data = e.resource;
                    let rd = self.get_res_def(&data)?;
                    let r = self.db.insert_batch(&e.ra_id, rd, data, &mut wb, &self.schema)?;
                },
                Method::Put => {

                },
                Method::Patch => {

                },
                Method::Get => {

                },
                Method::Head => {

                }
            }
        }

        self.db.save_batch(wb)?;
        Ok(RaResponse::Success)
    }

    pub fn create(&self, res_name: &str, val: &Value) -> Result<RaResponse, RaError> {
        self.schema.validate(&val)?;
        let doc = bson::to_document(val)?;
        let rd = self.get_res_def(&doc)?;

        if res_name != rd.name {
            return Err(RaError::bad_req(format!("received {}'s data on {}'s endpoint", &rd.name, res_name)));
        }

        let doc = self.db.insert(rd, doc, &self.schema)?;
        Ok(RaResponse::Created(doc))
    }

    pub fn bundle(&self, val: Value) -> Result<RaResponse, RaError> {
        let btype = val.get("type");
        if let None = btype {
            return Err(RaError::bad_req("missing type property"));
        }

        let btype = btype.unwrap().as_str();
        if let None = btype {
            return Err(RaError::bad_req("missing value for type property"));
        }

        let btype = BundleType::from(btype.unwrap())?;

        match btype {
            BundleType::Transaction => self.transaction(val),
            _ => {
                return Err(RaError::bad_req(format!("unsupported bundle type {:?}", btype)));
            }
        }
    }

    pub fn search_query(&self, res_name: &str, query: &SearchQuery, hints: &ResponseHints) -> Result<RaResponse, RaError> {
        let (key, val) = query.params[0];
        debug!("parsing filter {}", val);
        let tokens = scan_tokens(val)?;
        let ast = parse(tokens)?;
        let rd = self.get_res_def_by_name(res_name)?;
        self.search(rd, &ast)
    }

    pub fn search(&self, rd: &ResourceDef, filter: &Ast) -> Result<RaResponse, RaError> {
        let result = self.db.search(rd, filter)?;
        Ok(RaResponse::SearchResult(result))
    }

    fn get_res_def_by_name(&self, name: &str) -> Result<&ResourceDef, RaError>{
        let rd = self.schema.resources.get(name);
        if let None = rd {
            return Err(RaError::NotFound(format!("unknown resourceType {}", name)));
        }

        Ok(rd.unwrap())
    }

    fn get_res_def(&self, d: &Document) -> Result<&ResourceDef, RaError>{
        let res_type = d.get_str("resourceType")?;
        self.get_res_def_by_name(res_type)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;

    use anyhow::Error;
    use serde_json::json;

    use crate::utils::test_utils::parse_expression;

    use super::*;

    #[test]
    fn test_bundle_transaction() -> Result<(), Error> {
        let path = PathBuf::from("/tmp/testdb");
        let barn = Barn::open_with_default_schema(&path)?;

        let gateway = ApiBase::new(barn)?;

        let f = File::open("test_data/resources/bundle-example.json").unwrap();
        let val: Value = serde_json::from_reader(f).unwrap();

        let resp = gateway.bundle(val)?;
        let patient_schema = gateway.schema.resources.get("Practitioner").unwrap();
        let filter = parse_expression("name.where(family = 'Kuvalis369')");
        let results = gateway.search(patient_schema, &filter)?;
        if let RaResponse::SearchResult(ss) = results {
            assert_eq!(1, ss.entries.len());
        }
        else {
            assert!(false, "expected a SearchSet");
        }

        std::fs::remove_dir_all(&path);
        Ok(())
    }

    #[test]
    fn test_operation_outcome_ser() {
        let oo = OperationOutcome::new_error(IssueType::Processing, "resource not found");
        let s = oo.serialize();

        let expected = json!({"issue":[{"code":"processing","diagnostics":"resource not found","severity":"error"}],"resourceType":"OperationOutcome","text":{"div":"<div xmlns=\"http://www.w3.org/1999/xhtml\"><h1>Operation Outcome</h1><span>resource not found</span></div>","status":"generated"}});
        let actual: Value = serde_json::from_str(&s).unwrap();
        assert!(expected.eq(&actual));
    }
}

