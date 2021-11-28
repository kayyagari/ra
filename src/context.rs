use std::collections::HashMap;
use crate::res_schema::ResourceDef;
use serde_json::Value;

pub struct ServerContext {
    pub res_defs: HashMap<String, ResourceDef>
}

pub struct OpContext {
    pub res_name: String,
    pub payload: Value,
    pub protocol: String,
    pub headers: HashMap<String, String>,
    pub client_ip: String,
    pub client_port: u32,
}

impl ServerContext {
    pub fn insert(&self, op_ctx: OpContext) {

    }
}
