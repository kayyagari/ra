use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Config {
    /// the base URL (this is the external facing URL from which requests can be proxied to the local URL listening at the localhost and the port)
    #[serde(rename = "baseUrl")]
    base_url: String,

    /// this is the actual URL where the server listens for requests
    #[serde(rename = "port")]
    port: u16,

    /// list of supported ResourceTypes
    #[serde(rename = "supportedResTypes")]
    supported_res_types: Vec<String>,

    versioning: Versioning,
    #[serde(rename = "readHistory")]
    read_history: bool,

    #[serde(rename = "updateCreate")]
    update_create: bool,

    #[serde(rename = "conditionalCreate")]
    conditional_create: bool,

    #[serde(rename = "conditionalRead")]
    conditional_read: bool,

    #[serde(rename = "conditionalUpdate")]
    conditional_update: bool,

    #[serde(rename = "conditionalDelete")]
    conditional_delete: bool,

    #[serde(rename = "referencePolicy")]
    reference_policy: ReferencePolicy,

    #[serde(rename = "searchInclude")]
    search_include: bool,

    #[serde(rename = "searchRevInclude")]
    search_rev_include: bool,
}

#[derive(Serialize, Deserialize)]
pub enum Versioning {
    No_version,
    Versioned,
    Versioned_Update
}

#[derive(Serialize, Deserialize)]
pub enum ReferencePolicy {
    Literal,
    Logical,
    Resolves,
    Enforced,
    Local
}