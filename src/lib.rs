use log4rs::append::console::ConsoleAppender;
use log4rs::Config;
use log4rs::config::{Appender, Root};
use log::LevelFilter;
use std::collections::HashMap;
use crate::res_schema::ResourceDef;
use log4rs::encode::pattern::PatternEncoder;
use thiserror::Error;

pub mod validator;
pub mod res_schema;
mod context;
pub mod errors;
pub mod barn;
mod utils;
mod filter_scanner;
mod filter_parser;
mod dtypes;
mod search;
pub mod rapath;
mod test_utils;
pub mod importer;
mod resources;
pub mod api;
mod bson_utils;

pub fn configure_log4rs() {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} [{X(request_id)(no_request_id)}] {l} {M} - {m}\n")))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
        .unwrap();

    let _handle = log4rs::init_config(config).unwrap();
}
