use std::io::Cursor;
use log::warn;
use serde_json::Value;
use zip::read::ZipFile;
use zip::ZipArchive;
use crate::errors::RaError;

pub fn get_default_schema_bytes() -> &'static [u8] {
    include_bytes!("resources/fhir.schema-4.0.json.zip")
}

pub fn get_default_search_param_bytes() -> &'static [u8] {
    include_bytes!("resources/search-parameters-4.0.json.zip")
}

pub fn parse_compressed_json(data: &[u8]) -> Result<Value, RaError> {
    let cursor = Cursor::new(data);
    let z = ZipArchive::new(cursor);
    if let Err(e) = z {
        let msg = format!("failed to read the input stream of bzip2 compressed data {:?}", e);
        warn!("{}", &msg);
        return Err(RaError::SchemaParsingError(msg));
    }

    let mut z = z.unwrap();
    let z = z.by_index(0);
    if let Err(e) = z {
        let msg = format!("failed to read the bzip2 compressed data {:?}", e);
        warn!("{}", &msg);
        return Err(RaError::SchemaParsingError(msg));
    }

    let z = z.unwrap();

    let val: serde_json::Result<Value> = serde_json::from_reader(z);

    if let Err(e) = val {
        let msg = format!("failed to deserialize the schema from the compressed schema file: {}", e.to_string());
        warn!("{}", &msg);
        return Err(RaError::SchemaParsingError(msg));
    }

    Ok(val.unwrap())
}