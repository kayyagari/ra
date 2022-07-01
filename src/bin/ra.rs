use std::fs::File;
use std::net::IpAddr;
use std::os;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::time::Instant;
use bson::spec::BinarySubtype::Uuid;
use clap::{Parser, Subcommand};
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
use ra_registry::errors::RaError;

#[rocket::main]
async fn main() {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Start{data_dir, base_url, port, tls} => {
            configure_log4rs();
            let burl;
            if let Some(s) = base_url {
                burl = s.to_string();
            }
            else {
                burl = format!("http://localhost:{}/base", port);
            }
            let api_base = create_api_base(data_dir, burl).unwrap();
            let mut config = Config::default();
            config.address = IpAddr::from_str("0.0.0.0").unwrap();
            info!("binding to the local host interface {}", &config.address);
            config.port = *port;
            config.cli_colors = false;
            config.tls = None; // TODO support TLS
            let server = rest::mount(api_base, config);
            if let Err(e) = server {
                exit(151);
            }
            server.unwrap().launch().await;
        },
        Commands::Import {data_dir, zip_file} => {
            let api_base = create_api_base(data_dir, String::from("")).unwrap();

            if !zip_file.ends_with(".zip") {
                println!("unsupported archive format, only ZIP files are supported");
            }
            else {
                println!("importing from {:?}", zip_file);
                let start = Instant::now();
                let count = load_data(zip_file, &api_base);
                let elapsed = start.elapsed().as_millis();
                println!("time took to insert {} records {}ms", count, elapsed);
            }
        }
    }
}

fn create_api_base(data_dir: &PathBuf, base_url: String) -> Result<ApiBase, RaError> {
    let barn = Barn::open_with_default_schema(data_dir)?;
    ApiBase::new(barn, base_url)
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    #[clap(subcommand)]
    command: Commands
}

#[derive(Subcommand)]
pub enum Commands {
    /// Starts the server
    Start {
        /// path to the data directory
        #[clap(short='d', long, value_parser=clap::value_parser!(PathBuf), value_name="Data Directory")]
        data_dir: PathBuf,

        /// base URL that will be accessible globally
        #[clap(short='b', long, value_parser = validate_url)]
        base_url: Option<String>,

        /// local port number at which the server listens
        #[clap(default_value_t = 7090, short='p', long, value_parser = clap::value_parser!(u16))]
        port: u16,

        #[clap(default_value_t = false, short='s', long)]
        tls: bool
    },

    /// Imports data in bulk
    Import {
        /// path to the data directory
        #[clap(short='d', long, value_parser=clap::value_parser!(PathBuf), value_name="Data Directory")]
        data_dir: PathBuf,

        /// path to the archive(.zip) file to be imported
        #[clap(short='z', long, action, value_parser=clap::value_parser!(PathBuf), value_name="ZIP File")]
        zip_file: PathBuf
    }
}

fn load_data(p: &PathBuf, gateway: &ApiBase) -> usize {
    let mut count: usize = 0;
    let f = File::open(p.as_path()).expect("zip file is not readable");
    let mut archive = ZipArchive::new(f).expect("failed to open the zip file");
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        if entry.is_file() {
            println!("reading entry {}", entry.name());
            let val: serde_json::Result<Value> = serde_json::from_reader(entry);
            if let Err(e) = val {
                println!("failed to parse the file {}", e.to_string());
                continue;
            }
            let val = val.unwrap();
            let result = gateway.bundle(val);
            if let Err(e) = result {
                println!("failed to process the bundle {:?}", e);
            }
            else {
                count += 1;
            }
        }
    }

    count
}

fn validate_url(input: &str) -> Result<String, String> {
    let r = url::Url::parse(&input);
    if let Err(e) = r {
        return Err(format!("{} is not a valid URL", &input));
    }

    let protocol = r.unwrap().scheme().to_lowercase();
    if !(protocol == "http" || protocol == "https") {
        return Err(format!("{} is not a HTTP URL", &input));
    }

    Ok(input.to_string())
}