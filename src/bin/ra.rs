use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::time::Instant;
use bson::spec::BinarySubtype::Uuid;
use clap::{Arg, ArgMatches};
use ksuid::Ksuid;
use log::{debug, info, warn};
use rocket::{Config, routes};
use rocket::fairing::AdHoc;
use serde_json::Value;
use zip::ZipArchive;
use ra_registry::api::base::ApiBase;
use ra_registry::api::rest;
use ra_registry::barn::Barn;
use ra_registry::rapath::parser::parse;
use ra_registry::rapath::scanner::scan_tokens;
use ra_registry::res_schema::{parse_res_def, ResourceDef, SchemaDef};

use ra_registry::configure_log4rs;

#[rocket::main]
async fn main() {
    configure_log4rs();

    let opts = create_opts();

    let dir = opts.value_of("dir").unwrap();
    let path = PathBuf::from(dir);

    //std::fs::remove_dir_all(&path);
    let barn = Barn::open_with_default_schema(&path).unwrap();
    let api_base = ApiBase::new(barn).unwrap();

    let start = opts.is_present("start");
    if start {
        let mut config = Config::default();
        config.address = Ipv4Addr::new(0,0,0,0).into();
        config.port = 7090;
        config.cli_colors = false;

        let mut server = rocket::build().manage(api_base).configure(config);
        server = server.attach(AdHoc::on_request("Create trace ID", |req, _| Box::pin(async move {
            log_mdc::insert("request_id", uuid::Uuid::new_v4().to_string());
        }
        )));
        server.mount("/", routes![rest::create, rest::bundle, rest::search]).launch().await;
    }
    else if opts.is_present("import") {
        let import = opts.value_of("import").unwrap();
        debug!("importing from {}", import);
        if !import.ends_with(".zip") {
            info!("unsupported archive format, only ZIP files are supported");
        }
        else {
            let archive = PathBuf::from(import);

            let start = Instant::now();
            let count = load_data(archive, &api_base);
            let elapsed = start.elapsed().as_millis();
            println!("time took to insert {} records {}ms", count, elapsed);
        }
    }
}

fn create_opts() -> ArgMatches {
    let matches = clap::App::new("ra")
        .arg(Arg::new("dir")
            .short('d')
            .long("dir")
            .help("path to the data directory")
            .takes_value(true)
            .default_value("/tmp/testdb"))
        .arg(Arg::new("start")
            .short('s')
            .long("start")
            .help("start the server")
            .takes_value(false)
            .conflicts_with("import"))
        .arg(Arg::new("import")
            .short('i')
            .long("import")
            .help("path to the archive(.zip) file to be imported")
            .takes_value(true)
            .required(false))
        .get_matches();
    matches
}

fn load_data(p: PathBuf, gateway: &ApiBase) -> usize {
    let mut count: usize = 0;
    let f = File::open(p.as_path()).expect("zip file is not readable");
    let mut archive = ZipArchive::new(f).expect("failed to open the zip file");
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        if entry.is_file() {
            println!("reading entry {}", entry.name());
            let val: serde_json::Result<Value> = serde_json::from_reader(entry);
            if let Err(e) = val {
                info!("failed to parse the file {}", e.to_string());
                continue;
            }
            let val = val.unwrap();
            let result = gateway.bundle(val);
            if let Err(e) = result {
                warn!("failed to process the bundle {:?}", e);
            }
            else {
                count += 1;
            }
        }
    }

    count
}