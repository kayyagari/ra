extern crate core;

use log4rs::append::console::ConsoleAppender;
use log4rs::Config;
use log4rs::config::{Appender, Root};
use log::{warn, LevelFilter, Metadata};
use std::collections::HashMap;
use crate::res_schema::ResourceDef;
use log4rs::encode::pattern::PatternEncoder;
use thiserror::Error;

pub mod res_schema;
mod context;
pub mod errors;
pub mod barn;
pub mod utils;
mod dtypes;
mod search;
pub mod rapath;
pub mod importer;
pub mod api;

pub fn configure_log4rs() {
    // below check makes this method to be called from multiple locations
    let initialized_logger = log::logger();
    if initialized_logger.enabled(&Metadata::builder().build()) {
        warn!("************ logging is enabled, skipping re-configuring ****************");
        return;
    }

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} [{X(request_id)(no_request_id)}] {l} {M} - {m}\n")))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
        .unwrap();

    let _handle = log4rs::init_config(config).unwrap();
}
