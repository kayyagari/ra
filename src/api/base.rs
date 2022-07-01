use std::fmt::format;
use bson::Document;
use log::{debug, warn};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use rocket::request::{FromRequest, Outcome};
use std::convert::Infallible;
use rawbson::Doc;
use rocket::Request;

use crate::api::bundle;
use crate::api::bundle::{BundleType, Method, RequestBundle, SearchSet};
use crate::barn::Barn;
use crate::errors::{EvalError, IssueSeverity, IssueType, RaError};
use crate::rapath::expr::Ast;
use crate::rapath::parser::parse;
use crate::rapath::scanner::scan_tokens;
use crate::res_schema::{parse_res_def, parse_search_param, SchemaDef};
use crate::ResourceDef;
use crate::search::{ComparisonOperator, Filter, Modifier};
use crate::search::executor::execute_search_query;
use crate::search::filter_converter::param_to_filter;

pub struct ApiBase {
    pub(crate) db: Barn,
    pub(crate) schema: SchemaDef,
    pub(crate) base_url: String
}

pub enum RaResponse {
    Success,
    Created(Document),
    SearchResult(SearchSet)
}

pub struct ConditionalHeaders<'r> {
    pub if_none_exist: Option<&'r str>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OperationOutcome {
    #[serde(rename="resourceType")]
    rtype: &'static str,
    text: Narrative,
    issue: Vec<Box<BackboneElement>>
}

#[derive(Debug, Serialize, Deserialize)]
struct	BackboneElement {
    severity: IssueSeverity,
    code: IssueType,
    diagnostics: String,
    //expression: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
struct Narrative {
    status: NarrativeStatus,
    div: String
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum NarrativeStatus {
    Generated,
    Extensions,
    Additional,
    Empty
}

#[derive(Debug)]
pub struct SearchQuery<'r> {
    pub params: Vec<(&'r str, &'r str)>,
    pub sort: Option<&'r str>,
    pub count: u32,
    pub include: Option<&'r str>,
    pub revinclude: Option<&'r str>,
    pub total: Total,
    pub contained: Contained,
    pub contained_type: ContainedType,
    pub summary: bool,
    pub elements: bool,
    pub ignore_unknown_params: bool
}


#[derive(Debug, Eq, PartialEq)]
pub enum ReturnContent {
    Minimal,
    Representation,
    OperationOutcome
}

#[derive(Debug, Eq, PartialEq)]
pub enum Total {
    None,
    Estimate,
    Accurate
}

#[derive(Debug, Eq, PartialEq)]
pub enum Contained {
    DoNotReturn,
    Return,
    Both
}

#[derive(Debug, Eq, PartialEq)]
pub enum ContainedType {
    Container,
    Contained
}

impl From<&str> for Total {
    fn from(s: &str) -> Self {
        match s {
            "estimate" => Total::Estimate,
            "accurate" => Total::Accurate,
            _ => Total::None
        }
    }
}

impl From<&str> for Contained {
    fn from(s: &str) -> Self {
        match s {
            "true" => Contained::Return,
            "both" => Contained::Both,
            _ => Contained::DoNotReturn
        }
    }
}

impl From<&str> for ContainedType {
    fn from(s: &str) -> Self {
        match s {
            "contained" => ContainedType::Contained,
            _ => ContainedType::Container
        }
    }
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
    pub fn new(db: Barn, base_url: String) -> Result<Self, RaError> {
        let schema = db.build_schema_def()?;
        Ok(ApiBase{db, schema, base_url})
    }

    fn transaction(&self, val: Value) -> Result<RaResponse, RaError> {
        debug!("validating the transaction bundle");
        self.schema.validate(&val)?;
        let req_bundle = RequestBundle::from(val)?;
        debug!("processing transaction bundle");
        let mut to_be_indexed = Vec::new();
        let mut wb = WriteBatch::default();
        for e in req_bundle.entries {
            match e.req_method {
                Method::Delete => {

                },
                Method::Post => {
                    let data = e.resource;
                    let rd = self.get_res_def(&data)?;
                    let (_, doc_bytes, db_id) = self.db.insert_batch(&e.ra_id, rd, data, &mut wb, &self.schema, true)?;
                    to_be_indexed.push((db_id, doc_bytes, rd));
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
        if !to_be_indexed.is_empty() {
            debug!("indexing after saving data from batch");
            let mut wb = WriteBatch::default();
            for (db_id, doc, rd) in to_be_indexed {
                self.db.index_searchparams(&mut wb, &db_id, &doc, rd, &self.schema)?;
            }
            self.db.save_batch(wb)?;
        }
        Ok(RaResponse::Success)
    }

    pub fn create(&self, res_name: &str, val: &Value) -> Result<RaResponse, RaError> {
        self.schema.validate(&val)?;
        let doc = bson::to_document(val)?;
        let rd = self.get_res_def(&doc)?;

        if res_name != rd.name {
            return Err(RaError::bad_req(format!("received {}'s data on {}'s endpoint", &rd.name, res_name)));
        }

        let doc = self.db.insert(rd, doc, &self.schema, false)?;
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
        debug!("searching on {}", res_name);
        let rd = self.schema.get_res_def_by_name(res_name)?;
        let mut filter= None;
        if query.params.len() == 1 {
            let (key, val) = query.params[0];
            let tmp = param_to_filter(key, val, &rd, &self.schema);
            if let Err(e) = tmp {
                if !query.ignore_unknown_params {
                    return Err(RaError::BadRequest(e.to_string()));
                }
            }
            else {
                filter = Some(tmp.unwrap());
            }
        }
        else {
            let mut children = Vec::new();
            for (key, val) in &query.params {
                let sf = param_to_filter(key, val, &rd, &self.schema);
                if let Err(e) = sf {
                    if !query.ignore_unknown_params {
                        return Err(RaError::BadRequest(e.to_string()));
                    }
                }
                else {
                    children.push(Box::new(sf.unwrap()));
                }
            }

            if !children.is_empty() {
                filter = Some(Filter::AndFilter {children});
            }
        }

        if let None = filter {
            return Err(RaError::BadRequest(format!("none of the given search parameters are known to the server")));
        }

        execute_search_query(&filter.unwrap(), query, rd, &self.db, &self.schema)
    }

    pub fn search(&self, rd: &ResourceDef, filter: &Ast) -> Result<RaResponse, RaError> {
        let result = self.db.search(rd, filter)?;
        Ok(RaResponse::SearchResult(result))
    }

    fn get_res_def(&self, d: &Document) -> Result<&ResourceDef, RaError>{
        let res_type = d.get_str("resourceType")?;
        self.schema.get_res_def_by_name(res_type)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;

    use anyhow::Error;
    use serde_json::json;
    use crate::configure_log4rs;

    use crate::utils::test_utils::parse_expression;

    use super::*;

    #[test]
    fn test_bundle_transaction() -> Result<(), Error> {
        configure_log4rs();
        let path = PathBuf::from("/tmp/bundle_transaction_testdb");
        std::fs::remove_dir_all(&path);
        let barn = Barn::open_with_default_schema(&path)?;

        let gateway = ApiBase::new(barn, String::from(""))?;

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

