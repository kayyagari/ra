use std::collections::HashMap;

use crc32fast::Hasher;
use jsonschema::JSONSchema;
use log::{debug, error, info, trace, warn};
use serde_json::Value;
use crate::dtypes::DataType;
use crate::{utils};
use crate::errors::RaError;
use crate::utils::prefix_id;

extern crate crc32fast;

pub struct SchemaDef {
    pub props: HashMap<String, Box<PropertyDef>>,
    pub resources: HashMap<String, ResourceDef>,
    schema: JSONSchema
}

pub struct ResourceDef {
    /// Name of the resource
    pub name: String,
    /// Hash of the resource's name. This will be used as the prefix for the keys in DB
    pub hash: [u8;4],
    /// Prefix hash of the keys used for storing resource's version history
    pub history_hash: [u8;4],
    /// Prefix hash of the keys used for storing the referrals from other resources
    pub revinclude_hash: [u8;4],
    /// A map of all reference properties and their associated prefix hashes
    pub ref_props: HashMap<String, [u8; 4]>,
    pub attributes: HashMap<String, Box<PropertyDef>>,
    pub search_params: HashMap<&'static str, &'static SearchParamDef>
}

pub struct SearchParamDef {
    pub code: String,
    pub param_type: &'static str,
    pub expression: String,
    pub conditional_expression: bool,
    pub components: Option<Vec<String>>
}

pub struct PropertyDef {
    pub name: String,
    pub ref_type_name: String,
    pub props: Option<HashMap<String, Box<PropertyDef>>>,
    pub dtype: DataType,
    pub primitive: bool,
    pub collection: bool
}

pub struct ReferenceValue {
    /// the hash of the resourcetype's name
    pub res_type: [u8; 4],
    /// ID of the resource
    pub res_id: [u8; 20],
    /// version number of the resource, zero if not mentioned and that means use the current version
    pub version: u32
}

impl SchemaDef {
    pub fn get_prop(&self, res_name: &String, at_path: &String) -> Option<&Box<PropertyDef>> {
        let res = self.resources.get(res_name);
        if res.is_none() {
            debug!("no resource definition exists with the name {}", res_name);
            return Option::None;
        }

        let res = res.unwrap();

        let mut path_parts = at_path.split(".");
        let mut prop: Option<&Box<PropertyDef>> = Option::None;
        let first = path_parts.next();
        if let Some(f) = first {
            prop = res.attributes.get(f);
        }

        if prop.is_some() {
            for sub_at in path_parts {
                let t = prop.unwrap();
                if t.primitive {
                    prop = Option::None;
                }
                else {
                    if let Some(y) = self.props.get(&t.ref_type_name) {
                        if let Some(z) = &y.props {
                            prop = z.get(sub_at);
                        }
                        else {
                            prop = Option::None;
                        }
                    }
                    else {
                        prop = Option::None;
                    }
                }

                if prop.is_none() {
                    break;
                }
            }
        }

        prop
    }
}

pub fn parse_res_def(schema_doc: &Value) -> Result<SchemaDef, RaError> {
    info!("parsing schema...");
    let jschema = JSONSchema::compile(schema_doc);
    if let Err(e) = jschema {
        warn!("{}", e.to_string());
        return Err(RaError::SchemaParsingError(e.to_string()));
    }

    let prop_name = schema_doc.pointer("/discriminator/propertyName");

    info!("reading resource mappings");
    let mapping = schema_doc.pointer("/discriminator/mapping");
    if mapping.is_none() {
        let msg = "no resource mapping found in the schema";
        error!("{}", msg);
        return Err(RaError::SchemaParsingError(String::from(msg)));
    }

    let mut resource_defs = HashMap::new();
    let mapping = mapping.unwrap().as_object().unwrap();
    let global_props = parse_prop_definitions(schema_doc, mapping)?;

    for (res_name, res_def_path) in mapping {
        let hash: [u8; 4] = utils::get_crc_hash(res_name);
        let history_hash: [u8; 4] = utils::get_crc_hash(&format!("{}_history", res_name));
        let revinclude_hash: [u8; 4] = utils::get_crc_hash(&format!("{}_revinclude", res_name));

        let def_pointer = res_def_path.as_str().unwrap().strip_prefix("#").unwrap();
        let res_schema_def = schema_doc.pointer(def_pointer).unwrap();
        trace!("{}'s schema definition {}", res_name, res_schema_def);
        let res_props = res_schema_def.as_object().unwrap();
        let res_props = res_props.get("properties").unwrap().as_object().unwrap();

        let mut ref_props : HashMap<String, [u8; 4]> = HashMap::new();
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
                    trace!(">> reference property: {}", pk);
                    let crc_hash = format!("{}_{}", res_name, pk);
                    let crc_hash = utils::get_crc_hash(&crc_hash);
                    ref_props.insert(String::from(pk), crc_hash);
                }
            }
        }

        let attributes = parse_complex_prop_def(res_props)?;
        let res_def = ResourceDef {
            name: String::from(res_name),
            hash,
            history_hash,
            revinclude_hash,
            ref_props,
            attributes,
            search_params: HashMap::new() // TODO
        };

        resource_defs.insert(String::from(res_name), res_def);
    }

    let s = SchemaDef { props: global_props, resources: resource_defs, schema: jschema.unwrap() };
    Ok(s)
}

fn parse_prop_definitions(schema_doc: &Value, mapping: &serde_json::map::Map<String, Value>) -> Result<HashMap<String, Box<PropertyDef>>, RaError> {
    let prop_definitions = schema_doc.pointer("/definitions");
    if prop_definitions.is_none() {
        let msg = "no property definitions found in the schema";
        error!("{}", msg);
        return Err(RaError::SchemaParsingError(String::from(msg)));
    }
    let prop_definitions = prop_definitions.unwrap().as_object().unwrap();
    let mut all_props: HashMap<String, Box<PropertyDef>> = HashMap::new();
    for (k, v) in prop_definitions {
        if k == "ResourceList" || mapping.contains_key(k) {
            continue;
        }

        let pdef_json = v.as_object().unwrap();
        let definition_props = pdef_json.get("properties");
        if definition_props.is_none() {
            let pdef = parse_single_prop_def(k, pdef_json)?;
            all_props.insert(k.clone(), Box::new(pdef));
        }
        else { // parse complex property
            let definition_props = definition_props.unwrap().as_object().unwrap();
            let complex_at_props = parse_complex_prop_def(definition_props)?;
            let dtype = DataType::from_str(k);
            let pdef = PropertyDef{name: k.clone(), dtype, primitive: false, props: Option::Some(complex_at_props), collection: false, ref_type_name: String::from("")};

            all_props.insert(k.clone(), Box::new(pdef));
        }
    }

    Ok(all_props)
}

fn parse_complex_prop_def(definition_props: &serde_json::map::Map<String, Value>) -> Result<HashMap<String, Box<PropertyDef>>, RaError> {
    let mut complex_at_props: HashMap<String, Box<PropertyDef>> = HashMap::new();
    for (k, v) in definition_props {
        let inner_pdef_json = v.as_object().unwrap();
        let pdef = parse_single_prop_def(k, inner_pdef_json)?;
        complex_at_props.insert(k.clone(), Box::new(pdef));
    }

    Ok(complex_at_props)
}

fn parse_single_prop_def(name: &String, pdef_json: &serde_json::map::Map<String, Value>) -> Result<PropertyDef, RaError> {
    let mut ref_prop = pdef_json.get("$ref");
    let type_prop = pdef_json.get("type");

    let mut type_val = "string"; // default
    if let Some(t)= type_prop {
        type_val = t.as_str().unwrap();
    }

    let mut collection = false;
    if type_val == "array" {
        collection = true;
        let items = pdef_json.get("items").expect("expected items property").as_object().unwrap();
        ref_prop = items.get("$ref");
    }


    if let Some(r) = ref_prop {
        type_val = r.as_str().unwrap();
        let tmp = type_val.strip_prefix("#/definitions/");
        if let Some(t) = tmp {
            type_val = t;
        }
    }

    let dtype = DataType::from_str(type_val);

    let msg = format!("datatype of attribute {} is {:?}", name, &dtype);
    trace!("{}", &msg);

    let primitive = dtype.is_primitive();
    let pdef = PropertyDef{name: name.clone(), dtype, primitive, props: Option::None, collection, ref_type_name: String::from(type_val)};
    Ok(pdef)
}

fn parse_search_params() {

}

impl ResourceDef {
    /// generates a new ID with hash as the prefix
    /// this value is used as the DB record's key
    pub fn new_id(&self, ksid: &[u8]) -> [u8; 24] {
        prefix_id(&self.hash, ksid)
    }

    /// generates a new version history ID with history hash as the prefix
    /// this value is used as the DB record's key
    pub fn new_history_id(&self, ksid: &[u8]) -> [u8; 24] {
        prefix_id(&self.history_hash, ksid)
    }

    /// (_include) Observation(O1) -> Patient(P1) : <Observation-ref-attribute-crc32-hash><Observation-id><Patient-type-crc32-hash>=<Patient-id>
    /// e.g <Observation_subject><O1><Patient><P1>
    pub fn new_ref_fwd_id<S: AsRef<str>>(&self, for_at_name: S, from_id: &[u8], to: &ResourceDef, to_id: &[u8]) -> [u8; 48] {
        let mut tmp: [u8; 48] = [0; 48];
        let from_hash = self.ref_props.get(for_at_name.as_ref()).unwrap();
        tmp[..4].copy_from_slice(from_hash);
        tmp[4..24].copy_from_slice(from_id);
        tmp[24..28].copy_from_slice(&to.hash);
        tmp[28..].copy_from_slice(to_id);

        tmp
    }

    /// (_revinclude) Observation(O1) <- Patient(P1) : <Patient_revinclude-crc32-hash><Patient-id><Observation-type-crc32-hash>=<Observation-id>
    /// e.g <Patient_revinclude><P1><Observation><O1>
    pub fn new_ref_rev_id(&self, to_id: &[u8], to: &ResourceDef, from_id: &[u8]) -> [u8; 48] {
        let mut tmp: [u8; 48] = [0; 48];
        tmp[..4].copy_from_slice(&to.revinclude_hash);
        tmp[4..24].copy_from_slice(to_id);
        tmp[24..28].copy_from_slice(&self.hash);
        tmp[28..].copy_from_slice(from_id);

        tmp
    }
}

#[cfg(test)]
mod tests {
    use crate::res_schema::{parse_res_def, SchemaDef};
    use crate::utils::{get_crc_hash, u32_from_le_bytes};
    use std::fs::File;
    use anyhow::Error;
    use serde_json::Value;
    use crate::configure_log4rs;
    use crate::dtypes::DataType;

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

    #[test]
    fn test_schema_parsing() {
        configure_log4rs();
        let f = File::open("test_data/fhir.schema-4.0.json").unwrap();
        let v: Value = serde_json::from_reader(f).unwrap();
        let s = parse_res_def(&v);
        assert!(s.is_ok(), "schema processing failed");

        let s = s.unwrap();

        let dt = s.props.get("string").unwrap();
        assert_eq!(dt.dtype, DataType::STRING);
        let dt = s.props.get("xhtml").unwrap();
        assert_eq!(dt.dtype, DataType::STRING);

        let patient_contact_attributes = [ "_gender", "address", "extension", "gender", "id", "modifierExtension", "name", "organization", "period", "relationship", "telecom"];
        let pc = s.props.get("Patient_Contact").unwrap();
        let pc = pc.props.as_ref().unwrap();
        assert_eq!(patient_contact_attributes.len(), pc.len());

        for k in patient_contact_attributes {
            assert!(pc.contains_key(k));
        }

        // check resource props
        let patient = s.resources.get("Patient");
        assert!(patient.is_some());
        let patient = patient.unwrap();
        let expected = v.pointer("/definitions/Patient/properties").unwrap().as_object().unwrap().len();
        assert_eq!(expected, patient.attributes.len());
        let name = patient.attributes.get("name").unwrap();
        assert_eq!(DataType::HUMANNAME, name.dtype);
    }

    fn parse_schema() -> SchemaDef {
        let f = File::open("test_data/fhir.schema-4.0.json").unwrap();
        let v: Value = serde_json::from_reader(f).unwrap();
        parse_res_def(&v).unwrap()
    }

    struct AttributeCandidate<'a> {
        path: &'a str,
        found: bool,
        collection: bool,
        primitive: bool
    }

    #[test]
    fn test_attribute_searching() {
        let s = parse_schema();
        let mut candidates = vec!();
        let name = AttributeCandidate{ path: "name", found: true, collection: true, primitive: false};
        candidates.push(name);

        let id = AttributeCandidate{ path: "id", found: true, collection: false, primitive: true};
        candidates.push(id);

        let familyName = AttributeCandidate{ path: "name.family", found: true, collection: false, primitive: true};
        candidates.push(familyName);

        let familyNameFamily = AttributeCandidate{ path: "name.family.family", found: false, collection: false, primitive: false};
        candidates.push(familyNameFamily);

        let identifier = AttributeCandidate{ path: "identifier.type.coding.code", found: true, collection: false, primitive: true};
        candidates.push(identifier);

        for c in candidates {
            let prop = s.get_prop(&String::from("Patient"), &String::from(c.path));
            if c.found {
                assert!(prop.is_some());
                let prop_at = prop.unwrap();
                assert_eq!(c.collection, prop_at.collection);
                assert_eq!(c.primitive, prop_at.primitive);
            }
            else {
                assert!(prop.is_none());
            }
        }
    }

    #[test]
    fn test_reference_id_generation() -> Result<(), Error> {
        let s = parse_schema();
        let patient = s.resources.get("Patient").unwrap();
        let observation = s.resources.get("Observation").unwrap();

        let oid = ksuid::Ksuid::generate();
        let subject = String::from("subject");
        let ref_id = observation.new_ref_fwd_id(&subject, oid.as_bytes(), patient, oid.as_bytes());
        assert_eq!(48, ref_id.len());
        assert_eq!(observation.ref_props.get(&subject).unwrap(), &ref_id[..4]);
        assert_eq!(oid.as_bytes(), &ref_id[4..24]);
        assert_eq!(&patient.hash, &ref_id[24..28]);
        assert_eq!(oid.as_bytes(), &ref_id[28..]);

        let rev_ref_id = observation.new_ref_rev_id(oid.as_bytes(), patient, oid.as_bytes());
        assert_eq!(48, rev_ref_id.len());
        assert_eq!(&patient.revinclude_hash, &rev_ref_id[..4]);
        assert_eq!(oid.as_bytes(), &rev_ref_id[4..24]);
        assert_eq!(&observation.hash, &rev_ref_id[24..28]);
        assert_eq!(oid.as_bytes(), &rev_ref_id[28..]);
        Ok(())
    }
}