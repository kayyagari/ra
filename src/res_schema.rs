use std::collections::{HashMap, VecDeque};
use bson::Document;

use crc32fast::Hasher;
use jsonschema::JSONSchema;
use log::{debug, error, info, trace, warn};
use regex::Regex;
use serde_json::Value;
use crate::dtypes::DataType;
use crate::{utils};
use crate::errors::RaError;
use crate::rapath::expr::Ast;
use crate::rapath::parser::{parse, parse_with_schema};
use crate::rapath::scanner::scan_tokens;
use crate::search::SearchParamType;
use crate::utils::{get_crc_hash, prefix_id};
use crate::utils::validator::validate_resource;

extern crate crc32fast;

pub struct SchemaDef {
    pub props: HashMap<String, Box<PropertyDef>>,
    pub resources: HashMap<String, ResourceDef>,
    pub search_params: HashMap<u32, SearchParamDef>,
    search_params_by_res_name: HashMap<String, HashMap<String, u32>>,
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
    //pub search_params: HashMap<&'static str, &'static SearchParamDef>
}

#[derive(Debug)]
pub struct SearchParamDef {
    // using an integer generated from ksuid to make the cloning cheap
    // and use less memory, this is NOT "hash" of the search parameter
    pub id: u32,
    pub code: String,
    pub param_type: SearchParamType,
    // storing the expressions in string form instead of Ast. Ast requires
    // SystemType to be Send + Sync also this forces the use of Arc, so
    // probably the best is to store this Ast in a cache inside barn.rs
    pub expressions: HashMap<String, Option<SearchParamExpr>>,
    pub components: Option<Vec<String>>,
    pub multiple_or: bool,
    pub multiple_and: bool,
    pub targets: Option<HashMap<String, bool>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SearchParamExpr {
    pub hash: [u8;4], // this is the CRC hash of Resource's name + "_" + search param's code
    pub expr: String
}

#[derive(Debug)]
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

    pub fn validate(&self, val: &Value) -> Result<(), RaError> {
        validate_resource(&self.schema, val)
    }

    pub fn add_search_param(&mut self, spd: SearchParamDef) {
        for (res_name, expr) in &spd.expressions {
            if !self.search_params_by_res_name.contains_key(res_name) {
                self.search_params_by_res_name.insert(res_name.clone(), HashMap::new());
            }
            let search_params_of_res = self.search_params_by_res_name.get_mut(res_name).unwrap();
            if let Some(expr) = expr {
                search_params_of_res.insert(spd.code.clone(), spd.id);
            }
        }
        self.search_params.insert(spd.id, spd);
    }

    #[inline]
    pub fn get_search_params_of(&self, res_name: &String) -> Option<&HashMap<String, u32>> {
        self.search_params_by_res_name.get(res_name)
    }

    #[inline]
    pub fn get_search_param(&self, id: u32) -> Option<&SearchParamDef> {
        self.search_params.get(&id)
    }

    #[inline]
    pub fn get_search_param_expr_for_res(&self, code: &str, res_name: &str) -> Option<(&SearchParamDef, Option<&SearchParamExpr>)> {
        let res_params = self.search_params_by_res_name.get(res_name);
        if let Some(res_params) = res_params {
            let param = res_params.get(code);
            if let Some(param_id) = param {
                let spd = self.search_params.get(param_id).unwrap();
                let sp_expr = spd.expressions.get(res_name);
                if let Some(sp_expr) = sp_expr {
                    let expr = sp_expr.as_ref();
                    return Some((spd, expr));
                }
            }
        }

        None
    }

    #[inline]
    pub fn get_res_def_by_name(&self, name: &str) -> Result<&ResourceDef, RaError> {
        let rd = self.resources.get(name);
        if let None = rd {
            return Err(RaError::NotFound(format!("unknown resourceType {}", name)));
        }

        Ok(rd.unwrap())
    }

    /// FIXME inefficient lookup. The fix requires adding lifetime annotation to SchemaDef
    #[inline]
    pub fn get_res_def_by_hash(&self, hash: &[u8]) -> Result<&ResourceDef, RaError> {
        for (_, v) in &self.resources {
            if v.hash == hash {
                return Ok(v);
            }
        }
        Err(RaError::NotFound(format!("unknown resourceType {:?}", hash)))
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
            //search_params: HashMap::new() // TODO
        };

        resource_defs.insert(String::from(res_name), res_def);
    }

    let s = SchemaDef { props: global_props, resources: resource_defs, schema: jschema.unwrap(),
                        search_params: HashMap::new(), search_params_by_res_name: HashMap::new() };
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

pub fn parse_search_param(param_value: &Document, sd: &SchemaDef) -> Result<SearchParamDef, RaError> {
    let id = param_value.get_str("id")?;
    let code = param_value.get_str("code")?;
    let ptype = param_value.get_str("type")?;
    let ptype = SearchParamType::from(ptype)?;

    let mut components: Option<Vec<String>> = None;
    if ptype == SearchParamType::Composite {

    }

    let mut multiple_or = param_value.get_bool("multipleOr").unwrap_or(true);
    let mut multiple_and = param_value.get_bool("multipleAnd").unwrap_or(true);

    let mut expression = None;
    let expr = param_value.get("expression");
    if let Some(e) = expr {
        expression = Some(e.as_str().unwrap().to_string());
    }

    let mut res_expr_map: HashMap<String, Option<SearchParamExpr>> = HashMap::new();
    let base = param_value.get_array("base")?;
    if base.len() == 1 {
        let res_name = base[0].as_str().unwrap();
        res_expr_map.insert(res_name.to_string(), None);
        if let Some(e) = expr {
            let e = e.as_str().unwrap();
            let _ = parse_search_param_expression(e, code, sd)?; // validating the expression
            let hash = get_crc_hash(format!("{}_{}", res_name, code));
            let search_expr = SearchParamExpr{expr: e.to_string(), hash};
            res_expr_map.insert(res_name.to_string(), Some(search_expr));
        }
    }
    else {
        let res_name_pattern = Regex::new("[a-zA-Z]+\\.").unwrap();
        for b in base {
            res_expr_map.insert(b.as_str().unwrap().to_string(), None);
        }
        if let Some(e) = expr {
            let sub_exprs = split_union_expr(e.as_str().unwrap())?;
            for se in sub_exprs {
                let res_match = res_name_pattern.find(se);
                if let Some(res_match) = res_match {
                    let res_name = res_match.as_str();
                    let res_name = &res_name[0..res_name.len()-1];
                    if res_expr_map.contains_key(res_name) {
                        trace!("parsing expression {} for resource {}", se, res_name);
                        let _ = parse_search_param_expression(se, code, sd)?; // validating the expression
                        if let Some(part_of_res_expr) =  res_expr_map.get_mut(res_name) {
                            // join the parts of expression related to the same resource
                            // e.g AllergyIntolerance.code | AllergyIntolerance.reaction.substance
                            if let Some(part_of_res_expr) = part_of_res_expr {
                                part_of_res_expr.expr.push_str(" | ");
                                part_of_res_expr.expr.push_str(se);
                            }
                            else {
                                let hash = get_crc_hash(format!("{}_{}", res_name, code));
                                let search_expr = SearchParamExpr{expr: se.to_string(), hash};
                                res_expr_map.insert(res_name.to_string(), Some(search_expr));
                            }
                        }
                    }
                    else {
                        warn!("couldn't find the resource name {} in the base, it is likely that the expression split is buggy, full expression is {}", res_name, e.as_str().unwrap());
                    }
                }
            }
        }
    }

    let mut target_resources: Option<HashMap<String, bool>> = None;
    let target = param_value.get("target");
    if let Some(target) = target {
        let target = target.as_array().unwrap();
        let mut target_map = HashMap::new();
        for t in target {
            target_map.insert(t.as_str().unwrap().to_string(), true);
        }

        target_resources = Some(target_map);
    }

    let id = get_crc_from_id(id);
    let spd = SearchParamDef { id, code: code.to_string(),
        param_type: ptype,
        components, expressions: res_expr_map, multiple_or, multiple_and,
        targets: target_resources
    };

    Ok(spd)
}

fn parse_search_param_expression<'s>(expr: &str, code: &str, sd: &'s SchemaDef) -> Result<Ast<'s>, RaError> {
    let tokens = scan_tokens(expr);
    if let Err(e) = tokens {
        return Err(RaError::SearchParamParsingError(format!("invalid expression {} of search param {} {}", expr, code, e.to_string())));
    }

    let ast = parse_with_schema(tokens.unwrap(), Some(sd));
    if let Err(e) = ast {
        return Err(RaError::SearchParamParsingError(format!("unable to parse the expression {} of search param {} {}", expr, code, e.to_string())));
    }

    Ok(ast.unwrap())
}

fn split_union_expr(expr: &str) -> Result<Vec<&str>, RaError> {
    let mut parts: Vec<&str> = Vec::new();
    let mut chars = expr.char_indices().peekable();

    let mut start = 0;
    let mut end = 0;

    let mut stack = VecDeque::new();
    let mut prev = ' ';
    loop {
        match chars.next() {
            Some((_, c)) => {
                match c {
                    '|' => {
                        if stack.is_empty() {
                            let e = &expr[start..end].trim();
                            parts.push(e);
                            start = end + 1;
                        }
                    },
                    '(' => {
                        stack.push_front(c);
                    },
                    ')' => {
                        let p = stack.pop_front().unwrap_or(' ');
                        if p != '(' {
                            return Err(RaError::SearchParamParsingError(format!("invalid expression {} mismatched parentheses", expr)));
                        }
                    },
                    '"' | '\'' => {
                        if prev != '\\' {
                            let p = stack.get(0);
                            if let None = p {
                                stack.push_front(c);
                            }
                            else {
                                let p = p.unwrap();
                                if p == &c {
                                    stack.pop_front();
                                }
                                else {
                                    stack.push_front(c);
                                }
                            }
                        }
                    },
                    _ => {prev = c;}
                }
            },
            None => {break;}
        }
        end += 1;
    }

    if !stack.is_empty() {
        return Err(RaError::SearchParamParsingError(format!("invalid expression {} in search parameter", expr)));
    }

    if parts.is_empty() {
        parts.push(expr.trim());
    }
    else if start < expr.len() { // last expression, its validity cannot be enforced here
        parts.push(&expr[start..end].trim());
    }

    Ok(parts)
}

// this is used for generating unique IDs for SearchParamDef
// instances that need to be stored in memory
fn get_crc_from_id(id: &str) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(id.as_bytes());
    hasher.finalize()
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
    use std::collections::HashMap;
    use crate::res_schema::{parse_res_def, parse_search_param, SchemaDef, split_union_expr};
    use crate::utils::{get_crc_hash, u32_from_le_bytes};
    use std::fs::File;
    use anyhow::Error;
    use bson::{doc, Document};
    use rocket::form::validate::Len;
    use serde_json::Value;
    use crate::configure_log4rs;
    use crate::dtypes::DataType;
    use crate::rapath::parser::parse_with_schema;
    use crate::rapath::scanner::scan_tokens;
    use crate::search::SearchParamType;
    use crate::utils::test_utils::parse_expression;
    use pretty_assertions::{assert_eq, assert_ne};

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
        //configure_log4rs();
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

    #[test]
    fn test_search_param_expr_parsing() -> Result<(), Error> {
        let s = parse_schema();
        let res_name = "ActivityDefinition";
        let expr = "(ActivityDefinition.useContext.value = 1 )";
        let tokens = scan_tokens(expr).unwrap();
        let mut expr = parse_with_schema(tokens, Some(&s)).unwrap();
        println!("{}", expr);
        Ok(())
    }

    #[test]
    fn test_split_union_expr() {
        let mut candidates = Vec::new();
        candidates.push(("Patient.telecom.where(system='phone') | Person.telecom.where(system='phone')", 2, vec!["Patient.telecom.where(system='phone')", "Person.telecom.where(system='phone')"]));
        candidates.push(("a.c", 1, vec!["a.c"]));
        candidates.push(("a.c | a.r.s ", 2, vec!["a.c", "a.r.s"]));
        candidates.push(("c.c | (dr.c as CC)", 2, vec!["c.c", "(dr.c as CC)"]));
        candidates.push(("c.c | ((dr.c as CC) | a.b = 2)", 2, vec!["c.c", "((dr.c as CC) | a.b = 2)"]));
        candidates.push(("c.c | a.b = \"has an |\"", 2, vec!["c.c", "a.b = \"has an |\""]));
        candidates.push(("c.c | a.b = 'has an |'", 2, vec!["c.c", "a.b = 'has an |'"]));
        candidates.push(("c.c | a.b = 'has an \\' escaped char'", 2, vec!["c.c", "a.b = 'has an \\' escaped char'"]));
        candidates.push(("c.c | (((dr.c as CC)))", 2, vec!["c.c", "(((dr.c as CC)))"]));
        candidates.push(("Account.subject.where(resolve() is Patient)", 1, vec!["Account.subject.where(resolve() is Patient)"]));
        for (input, count, expected) in candidates {
            let parts = split_union_expr(input).unwrap();
            assert_eq!(count, parts.len());
            assert_eq!(parts, expected);
        }
    }

    #[test]
    fn test_parse_search_param() {
        let f = File::open("test_data/fhir.schema-4.0.json").unwrap();
        let v: Value = serde_json::from_reader(f).unwrap();
        let sd = parse_res_def(&v).unwrap();

        let doc = doc!{"id": "id1", "code":"_text","base":["DomainResource"],"type":"string"};
        let spd = parse_search_param(&doc, &sd).unwrap();
        assert_eq!("_text", &spd.code);
        assert_eq!(true, spd.multiple_or);
        assert_eq!(true, spd.multiple_and);
        assert_eq!(None, spd.components);
        assert_eq!(None, spd.targets);
        assert_eq!(SearchParamType::String, spd.param_type);
        assert_eq!(None, *spd.expressions.get("DomainResource").unwrap());

        let doc = doc!{"id": "id2", "code":"code","base":["AllergyIntolerance","Condition"],"type":"token","expression":"AllergyIntolerance.code | AllergyIntolerance.reaction.substance | Condition.code"};
        let spd = parse_search_param(&doc, &sd).unwrap();
        assert_eq!("code", &spd.code);
        assert_eq!(String::from("AllergyIntolerance.code | AllergyIntolerance.reaction.substance"), spd.expressions.get("AllergyIntolerance").unwrap().as_ref().unwrap().expr);
        assert_eq!(String::from("Condition.code"), spd.expressions.get("Condition").unwrap().as_ref().unwrap().expr);
        assert_eq!(SearchParamType::Token, spd.param_type);
    }

    #[test]
    fn test_prase_all_search_params_of_v4() {
        let f = File::open("test_data/fhir.schema-4.0.json").unwrap();
        let v: Value = serde_json::from_reader(f).unwrap();
        let mut sd = parse_res_def(&v).unwrap();

        let sp_file = File::open("test_data/search-parameters-4.0.json").unwrap();
        let v: Value = serde_json::from_reader(sp_file).unwrap();
        let params = v.pointer("/entry").unwrap().as_array().unwrap();
        let mut expected_params_per_res = HashMap::new();
        for p in params {
            let d = bson::to_bson(p.get("resource").unwrap()).unwrap();
            let d = d.as_document().unwrap();
            let base = d.get_array("base").unwrap();
            for res_name in base {
                let res_name = res_name.as_str().unwrap().to_owned();
                if !expected_params_per_res.contains_key(&res_name) {
                    expected_params_per_res.insert(res_name.clone(), Vec::new());
                }
                expected_params_per_res.get_mut(&res_name).unwrap().push(d.get_str("code").unwrap().to_owned());
            }
            let spd = parse_search_param(d, &sd).unwrap();
            sd.add_search_param(spd);
        }

        let parsed_params_of_patient = sd.get_search_params_of(&String::from("Patient")).unwrap();
        let mut parsed_param_names_of_patient: Vec<String> = parsed_params_of_patient.iter().map(|e| e.0.to_string()).collect();
        parsed_param_names_of_patient.sort();

        let mut expected_params_of_patient = expected_params_per_res.get("Patient").unwrap().to_owned();
        expected_params_of_patient.sort();

        assert_eq!(expected_params_of_patient, parsed_param_names_of_patient);
        assert_eq!(expected_params_of_patient.len(), parsed_params_of_patient.len());
        println!("{}", expected_params_of_patient.len());
        println!("{:?}", expected_params_of_patient);
    }
}