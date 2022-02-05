#[macro_use] extern crate rocket;

use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::time::Instant;
use rocket::{Config, routes};
use serde_json::Value;
use zip::ZipArchive;
use ra_registry::api::base::ApiBase;
use ra_registry::api::rest;
use ra_registry::barn::Barn;
use ra_registry::rapath::parser::parse;
use ra_registry::rapath::scanner::scan_tokens;
use ra_registry::res_schema::{parse_res_def, ResourceDef, SchemaDef};

use ra_registry::{configure_log4rs, validator};

#[rocket::main]
async fn main() {
    configure_log4rs();
    let path = PathBuf::from("/tmp/testdb");
    //std::fs::remove_dir_all(&path);
    let barn = Barn::open_with_default_schema(&path).unwrap();
    let api_base = ApiBase::new(barn).unwrap();

    let mut config = Config::default();
    config.address = Ipv4Addr::new(0,0,0,0).into();
    config.port = 7090;
    let server = rocket::build().manage(api_base).configure(config);
    server.mount("/", routes![rest::create]).launch().await;

    // let sd = parse_res_def(&barn.read_schema()?).unwrap();
    // let patient_schema = sd.resources.get("Patient").unwrap();
    //
    // let start = Instant::now();
    // let count = load_patients(PathBuf::from("/Users/dbugger/Downloads/synthea-fhir-samples.zip"), patient_schema, &barn, &sd);
    //
    // let elapsed = start.elapsed().as_millis();
    // println!("time took to insert {} records {}ms", count, elapsed);
    //
    // let tokens = scan_tokens("name.where(family = 'Delgado712')").unwrap();
    // let filter = parse(tokens).unwrap();
    // let start = Instant::now();
    // let results = barn.search(patient_schema, &filter).unwrap();
    // let elapsed = start.elapsed().as_millis();
    // println!("search returned {} results in {}ms", results.len(), elapsed);
}

fn load_patients(p: PathBuf, res_def: &ResourceDef, db: &Barn, sd: &SchemaDef) -> i32 {
    if !p.ends_with(".zip") {

    }

    let mut count = 0;
    let f = File::open(p.as_path()).expect("zip file is not readable");
    let mut archive = ZipArchive::new(f).expect("failed to open the zip file");
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        if entry.is_file() {
            println!("reading entry {}", entry.name());
            let val: Value = serde_json::from_reader(entry).unwrap();
            let resources = val.get("entry").unwrap().as_array().unwrap();
            for r in resources {
                let rt = r.get("resource").unwrap();
                if rt.get("resourceType").unwrap().as_str().unwrap() == "Patient" {
                    //println!("inserting name.family {:?}", rt.pointer("/name[0]/family"));
                    let data = bson::to_document(&rt).unwrap();
                    //let mut data = data.as_document().unwrap().to_owned();
                    db.insert(res_def, data, sd).unwrap();
                    count += 1;
                    break;
                }
            }
        }
    }

    count
}