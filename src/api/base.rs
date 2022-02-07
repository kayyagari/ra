use bson::Document;
use log::debug;
use rocksdb::WriteBatch;
use serde::{Serialize, Deserialize};
use serde_json::Value;

use crate::api::bundle;
use crate::api::bundle::{Method, RequestBundle};
use crate::barn::Barn;
use crate::errors::{EvalError, IssueSeverity, IssueType, RaError};
use crate::rapath::expr::Ast;
use crate::res_schema::{parse_res_def, SchemaDef};
use crate::ResourceDef;

pub struct ApiBase {
    db: Barn,
    schema: SchemaDef
}

pub enum RaResponse {
    Success,
    Created(Document)
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

impl OperationOutcome {
    pub fn new_error<S: AsRef<str>>(code: IssueType, msg: S) -> Self {
        let r = msg.as_ref();
        let div = format!(r#"<div xmlns="http://www.w3.org/1999/xhtml"><h1>Operation Outcome</h1><span>{}</span></div>"#, r);
        let text = Narrative{status: NarrativeStatus::Generated, div};
        let i1 = BackboneElement{severity: IssueSeverity::Error, code, diagnostics: r.to_string()};
        let issue = vec![Box::new(i1)];
        OperationOutcome{issue, text, rtype: "OperationOutcome"}
    }
}

impl ApiBase {
    pub fn new(db: Barn) -> Result<Self, RaError> {
        let schema = db.read_schema()?;
        let schema = parse_res_def(&schema)?;
        Ok(ApiBase{db, schema})
    }

    pub fn transaction(&self, val: Value) -> Result<RaResponse, RaError> {
        debug!("validating the transaction bundle");
        self.schema.validate(&val)?;
        let req_bundle = RequestBundle::from(val)?;
        // req_bundle.entries.
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

    pub fn search(&self, rd: &ResourceDef, filter: &Ast) -> Result<Vec<Document>, EvalError> {
        self.db.search(rd, filter)
    }

    fn get_res_def(&self, d: &Document) -> Result<&ResourceDef, RaError>{
        let res_type = d.get_str("resourceType")?;
        let rd = self.schema.resources.get(res_type);
        if let None = rd {
            return Err(RaError::bad_req(format!("unknown resourceType {}", res_type)));
        }

        Ok(rd.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;

    use anyhow::Error;
    use serde_json::json;

    use crate::test_utils::parse_expression;

    use super::*;

    #[test]
    fn test_bundle_transaction() -> Result<(), Error> {
        let path = PathBuf::from("/tmp/testdb");
        std::fs::remove_dir_all(&path);
        let barn = Barn::open_with_default_schema(&path)?;

        let gateway = ApiBase::new(barn)?;

        let f = File::open("test_data/resources/bundle-example.json").unwrap();
        let val: Value = serde_json::from_reader(f).unwrap();

        let resp = gateway.transaction(val)?;
        let patient_schema = gateway.schema.resources.get("Practitioner").unwrap();
        let filter = parse_expression("name.where(family = 'Kuvalis369')");
        let results = gateway.search(patient_schema, &filter)?;
        assert_eq!(1, results.len());

        Ok(())
    }

    #[test]
    fn test_operation_outcome_ser() {
        let oo = OperationOutcome::new_error(IssueType::Processing, "resource not found");
        let s = serde_json::to_string(&oo).unwrap();

        let expected = json!({"issue":[{"code":"processing","diagnostics":"resource not found","severity":"error"}],"resourceType":"OperationOutcome","text":{"div":"<div xmlns=\"http://www.w3.org/1999/xhtml\"><h1>Operation Outcome</h1><span>resource not found</span></div>","status":"generated"}});
        let actual: Value = serde_json::from_str(&s).unwrap();
        assert!(expected.eq(&actual));
    }
}