use bson::Document;
use log::debug;
use rocksdb::WriteBatch;
use serde_json::Value;
use crate::api::bundle;
use crate::api::bundle::{Method, RequestBundle};
use crate::barn::Barn;
use crate::errors::{EvalError, RaError};
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

pub struct OperationOutcome {

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
        let doc = bson::to_document(val)?;
        let rd = self.get_res_def(&doc)?;

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
            return Err(RaError::invalid_err(format!("unknown resourceType {}", res_type)));
        }

        Ok(rd.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;
    use super::*;
    use anyhow::Error;
    use crate::test_utils::parse_expression;

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
}