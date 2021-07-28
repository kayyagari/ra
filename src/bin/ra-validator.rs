use ra_registry::validator;
use jsonschema::{JSONSchema};
use serde_json::{Value};
use std::fs::File;

fn main() {
    let val: Value = serde_json::from_reader(File::open("../../test_data/fhir.schema-4.0.json").unwrap()).unwrap();
    let schema = JSONSchema::compile(&val).unwrap();

    for i in 1..10 {
        let patient_resource: Value = serde_json::from_reader(File::open("../../test_data/resources/patient-example-a.json").unwrap()).unwrap();
        let start = std::time::Instant::now();
        let result = validator::validate_resource(&schema, &patient_resource);
        let end = std::time::Instant::now();
        assert!(result.is_ok());
        println!("{}", end.duration_since(start).as_millis());
    }
}
