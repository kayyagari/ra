use log4rs::append::console::ConsoleAppender;
use log4rs::Config;
use log4rs::config::{Appender, Root};
use log::LevelFilter;
use std::collections::HashMap;
use crate::res_schema::ResourceDef;
use log4rs::encode::pattern::PatternEncoder;

pub mod validator;
mod res_schema;
mod context;
mod errors;
mod barn;
mod utils;

fn configure_log4rs() {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} [{X(request_id)(no_request_id)}] {l} {M} - {m}\n")))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
        .unwrap();

    let _handle = log4rs::init_config(config).unwrap();
}