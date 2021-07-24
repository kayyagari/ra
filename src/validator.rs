use jsonschema::{JSONSchema};
use serde_json::{Value};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("`{0}`")]
    InvalidElement(String)
}

pub fn validate_resource(schema: &JSONSchema, r: &Value) -> Result<(), Vec<ValidationError>> {
    let result = schema.validate(r);
    if result.is_err() {
        let errors = result.err().unwrap();
        let mut schema_errors: Vec<ValidationError> = Vec::new();
        for e in errors {
            schema_errors.push(ValidationError::InvalidElement(e.to_string()));
        }

        return Err(schema_errors);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use jsonschema::JSONSchema;
    use serde_json::Value;
    use std::fs::File;
    use crate::validator::validate_resource;

    #[test]
    fn test_validation() {
        let val: Value = serde_json::from_reader(File::open("test_resources/fhir.schema-4.0.json").unwrap()).unwrap();
        let schema = JSONSchema::compile(&val).unwrap();

        let patient_resource: Value = serde_json::from_reader(File::open("test_resources/resources/patient-example-a.json").unwrap()).unwrap();
        let result = validate_resource(&schema, &patient_resource);
        assert!(result.is_ok());
    }
}
