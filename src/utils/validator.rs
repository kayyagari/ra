use jsonschema::{JSONSchema};
use serde_json::{Value};
use thiserror::Error;
use crate::errors::RaError;

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("`{0}`")]
    InvalidElement(String)
}

pub fn validate_resource(schema: &JSONSchema, r: &Value) -> Result<(), RaError> {
    // is_valid() is faster than validate()
    // and because validate() wasn't giving the precise details of the offending element
    // it is good to replace with is_valid()
    let valid = schema.is_valid(r);
    if !valid {
        return Err(RaError::SchemaValidationError);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use jsonschema::JSONSchema;
    use serde_json::{json, Value};
    use std::fs::File;
    use crate::utils::validator::validate_resource;
    use crate::res_schema::parse_res_def;
    //use crate::configure_log4rs;

    #[test]
    fn test_validation() {
        //configure_log4rs();
        let val: Value = serde_json::from_reader(File::open("test_data/fhir.schema-4.0.json").unwrap()).unwrap();
        parse_res_def(&val);
        let schema = JSONSchema::compile(&val).unwrap();

        let patient_resource: Value = serde_json::from_reader(File::open("test_data/resources/patient-example-a.json").unwrap()).unwrap();
        let start = std::time::Instant::now();
        let result = validate_resource(&schema, &patient_resource);
        let end = std::time::Instant::now();
        assert!(result.is_ok());
        println!("time taken to validate: {}", end.duration_since(start).as_millis());

        let patient_resource = json!({"id": 1});
        let result = validate_resource(&schema, &patient_resource);
        assert!(result.is_err());
    }
}
