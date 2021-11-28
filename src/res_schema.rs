use std::collections::HashMap;

use crc32fast::Hasher;
use log::{debug, error, info, warn};
use log4rs;
use serde_json::Value;

extern crate crc32fast;

pub struct ResourceDef {
    pub name: String,
    pub hash: [u8;4],
    pub history_hash: [u8;4],
    pub reference_hash: [u8;4],
    pub ref_props: HashMap<String, [u8; 4]>
}

pub struct ReferenceValue {
    /// the hash of the resourcetype's name
    pub res_type: [u8; 4],
    /// ID of the resource
    pub res_id: [u8; 20],
    /// version number of the resource, zero if not mentioned and that means use the current version
    pub version: u32
}

pub fn parse_res_def(schema_doc: &Value) -> HashMap<String, ResourceDef> {
    info!("reading resource mappings");
    let mapping = schema_doc.pointer("/discriminator/mapping");
    if mapping.is_none() {
        let msg = "no resource mapping found in the schema";
        error!("{}", msg);
        panic!(msg);
    }

    let mut resource_defs = HashMap::new();
    let mapping = mapping.unwrap().as_object().unwrap();
    for (k, v) in mapping {
        let hash: [u8; 4] = get_crc_hash(k);
        let history_hash: [u8; 4] = get_crc_hash(&format!("{}_history", k));
        let reference_hash: [u8; 4] = get_crc_hash(&format!("{}_reference", k));

        let def_pointer = v.as_str().unwrap().strip_prefix("#").unwrap();
        let res_schema_def = schema_doc.pointer(def_pointer).unwrap();
        debug!("{}'s schema definition {}", k, res_schema_def);
        let res_props = res_schema_def.as_object().unwrap();
        let res_props = res_props.get("properties").unwrap().as_object().unwrap();

        let mut ref_props : Vec<String> = Vec::new();
        for (pk, pv) in res_props {
            let mut ref_prop = pv.get("$ref");
            if ref_prop.is_none() {
                let items = pv.get("items");
                if items.is_some() {
                    let items = items.unwrap().as_object().unwrap();
                    ref_prop = items.get("$ref");
                }
            }

            if ref_prop.is_some() {
                let ref_prop = ref_prop.unwrap().as_str().unwrap();
                if ref_prop == "#/definitions/Reference" {
                    debug!(">> reference property: {}", pk);
                    ref_props.push(String::from(pk));
                }
            }
        }

        let res_def = ResourceDef {
            name: String::from(k),
            hash,
            history_hash,
            reference_hash,
            ref_props
        };

        resource_defs.insert(String::from(k), res_def);
    }

    resource_defs
}

fn get_crc_hash(k: &String) -> [u8;4] {
    let mut hasher = Hasher::new();
    hasher.update(k.as_bytes());
    let i = hasher.finalize();
    i.to_le_bytes()
}

impl ResourceDef {
    /// generates a new ID with hash as the prefix
    /// this value is used as the DB record's key
    pub fn new_prefix_id(&self, ksid: &[u8]) -> [u8; 24] {
        self.prepare_id(&self.hash, ksid)
    }

    /// generates a new version history ID with history hash as the prefix
    /// this value is used as the DB record's key
    pub fn new_history_prefix_id(&self, ksid: &[u8]) -> [u8; 24] {
        self.prepare_id(&self.history_hash, ksid)
    }

    fn prepare_id(&self, prefix: &[u8], ksid: &[u8]) -> [u8; 24]{
        let mut tmp: [u8; 24] = [0; 24];
        tmp.copy_from_slice(prefix);
        tmp[4..].copy_from_slice(ksid);

        tmp
    }
}

#[cfg(test)]
mod tests {
    use crate::res_schema::get_crc_hash;
    use crate::utils::u32_from_le_bytes;

    /// a trivial test to check that CRC doesn't produce a collision when
    /// the letters are interchanged in the same string
    #[test]
    fn test_crc_collision() {
        let s1 = String::from("abcd");
        let h1 = u32_from_le_bytes(&get_crc_hash(&s1));

        let s2 = String::from("bacd");
        let h2 = u32_from_le_bytes(&get_crc_hash(&s2));

        assert_ne!(h1, h2, "CRC collision found");
    }
}