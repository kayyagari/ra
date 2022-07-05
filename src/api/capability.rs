use bson::{Bson, Document};
use chrono::Utc;
use crate::res_schema::SchemaDef;
use crate::utils;

pub const CAPABILITY_STATEMENT_ID: &str = "2BO62RJ5I2iw5lrVgJU0jzXKp6S";

pub fn gen_capability_stmt(schema: &SchemaDef, base_url: &str) -> Document {
    let mut doc = bson::Document::new();
    doc.insert("id", CAPABILITY_STATEMENT_ID);
    doc.insert("resourceType", "CapabilityStatement");
    doc.insert("url", format!("{}/CapabilityStatement/{}", base_url, CAPABILITY_STATEMENT_ID));
    doc.insert("name", "RaServer");
    doc.insert("status", "active");
    doc.insert("date", Utc::now().format(utils::bson_utils::DATE_FORMAT).to_string());
    doc.insert("fhirVersion", schema.get_fhir_version());
    //doc.insert("publisher", "TODO"); // TODO
    //doc.insert("copyright", "TODO"); // TODO
    doc.insert("kind", "instance");
    let mut formats = bson::Array::new();
    formats.push(Bson::from("application/fhir+json"));
    formats.push(Bson::from("json"));
    doc.insert("format", Bson::Array(formats));
    // let mut patch_formats = bson::Array::new();
    // patch_formats.push(Bson::from("application/fhir+json"));
    // doc.insert("patchFormat", patch_formats);

    // software
    let mut software = bson::Document::new();
    software.insert("name", "Ra FHIR Server");
    let version = env!("CARGO_PKG_VERSION");
    software.insert("version", version);
    doc.insert("software", software);

    // implementation
    let mut implementation = Document::new();
    implementation.insert("description", "Ra FHIR Server");
    implementation.insert("url", base_url);
    doc.insert("implementation", implementation);

    let interaction_codes = ["create", "read", "search-type"]; // "history-instance", "history-type", "patch", "update", "vread", "delete"
    let mut interaction = bson::Array::new();
    for c in interaction_codes {
        interaction.push(Bson::from(c));
    }

    let mut search_include = bson::Array::new();
    search_include.push(Bson::from("*"));

    let mut resource = bson::Array::new();
    for (k, v) in &schema.resources {
        let mut res_doc = bson::Document::new();
        res_doc.insert("type", k);
        res_doc.insert("profile", format!("http://hl7.org/fhir/StructureDefinition/{}", k));
        res_doc.insert("interaction", &interaction);
        res_doc.insert("versioning", "no-version");
        res_doc.insert("readHistory", false);
        res_doc.insert("updateCreate", false);
        res_doc.insert("conditionalCreate", false);
        res_doc.insert("conditionalRead", false);
        res_doc.insert("conditionalUpdate", false);
        res_doc.insert("conditionalDelete", false);
        //res_doc.insert("referencePolicy", "enforced");
        res_doc.insert("searchInclude", &search_include);

        let mut search_param = bson::Array::new();
        let res_search_params = schema.get_search_params_of(k);
        if let Some(res_search_params) = res_search_params {
            for (pname, pid) in res_search_params {
                let spd = schema.get_search_param(*pid).unwrap();
                let mut spdoc = Document::new();
                spdoc.insert("name", &spd.name);
                spdoc.insert("definition", &spd.url);
                spdoc.insert("type", &spd.param_type.to_string());
                search_param.push(Bson::Document(spdoc));
            }
        }
        res_doc.insert("searchParam", search_param);
        resource.push(Bson::Document(res_doc));
    }

    let mut rest_doc = bson::Document::new();
    rest_doc.insert("mode", "server");
    rest_doc.insert("resource", resource);

    let mut rest = bson::Array::new();
    rest.push(Bson::Document(rest_doc));

    doc.insert("rest", rest);
    doc
}
