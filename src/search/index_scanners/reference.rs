use std::collections::HashMap;
use std::convert::TryInto;
use std::rc::Rc;
use ksuid::Ksuid;
use log::warn;
use rawbson::elem::Element;
use rocket::debug;
use rocket::form::validate::Contains;
use rocksdb::DBIterator;
use crate::barn::Barn;
use crate::rapath::engine::{eval, ExecContext, UnresolvableExecContext};
use crate::rapath::expr::Ast;
use crate::rapath::parser::parse;
use crate::rapath::scanner::scan_tokens;
use crate::rapath::stypes::SystemType;
use crate::search::index_scanners::{IndexScanner, SelectedResourceKey};
use crate::search::{ComparisonOperator, Modifier};
use crate::search::ComparisonOperator::*;

pub struct ReferenceIndexScanner<'f, 'd: 'f> {
    ref_id: Ksuid,
    ref_type: Option<[u8; 4]>,
    itr: DBIterator<'d>,
    index_prefix: &'f [u8],
    modifier: Modifier
}

pub struct ReferenceIdIndexScanner<'f> {
    rpath_expr: Ast<'f>,
    itr: DBIterator<'f>,
    db: &'f Barn,
    index_prefix: &'f [u8]
}

pub fn new_reference_scanner<'f, 'd: 'f>(ref_id: Ksuid, ref_type: Option<[u8; 4]>, itr: DBIterator<'d>, index_prefix: &'f [u8], modifier: Modifier) -> ReferenceIndexScanner<'f, 'd> {
    ReferenceIndexScanner { ref_id, ref_type, itr, index_prefix, modifier }
}

pub fn new_reference_id_scanner<'f>(rpath_expr: Ast<'f>, db: &'f Barn, index_prefix: &'f [u8]) -> ReferenceIdIndexScanner<'f> {
    let itr = db.new_index_iter(index_prefix);
    ReferenceIdIndexScanner { rpath_expr, itr, db, index_prefix }
}

impl<'f, 'd: 'f> IndexScanner for ReferenceIndexScanner<'f, 'd> {
    fn next(&mut self) -> SelectedResourceKey {
        todo!()
    }

    fn collect_all(&mut self) -> HashMap<[u8; 24], bool> {
        let mut res_keys = HashMap::new();
        loop {
            let row = self.itr.next();
            if let None = row {
                break;
            }
            let row = row.unwrap();
            let row_prefix = &row.0[..4];
            if row_prefix != self.index_prefix {
                break;
            }

            let pos = row.0.len() - 24;
            let hasVal = row.0[4] == 1; // TODO don't think this flag is necessary for references, we can save a byte
            let actual_ref_type = &row.0[5..9];
            let actual_ref_id = &row.0[9..pos];
            let mut found = false;
            if let Some(ref ref_type) = self.ref_type {
                if actual_ref_type == ref_type && actual_ref_id == self.ref_id.as_bytes() {
                    found = true;
                }
            }
            else if actual_ref_id == self.ref_id.as_bytes() {
                found = true;
            }

            if found {
                let mut tmp: [u8; 24] = [0; 24];
                tmp.copy_from_slice(&row.0[pos..]);
                res_keys.insert(tmp, true);
                break; // only one ID will be present
            }
        }

        res_keys
    }
}

impl<'f> IndexScanner for ReferenceIdIndexScanner<'f> {
    fn next(&mut self) -> SelectedResourceKey {
        todo!()
    }

    fn collect_all(&mut self) -> HashMap<[u8; 24], bool> {
        let mut res_keys = HashMap::new();
        loop {
            let row = self.itr.next();
            if let None = row {
                break;
            }
            let row = row.unwrap();
            let row_prefix = &row.0[..4];
            if row_prefix != self.index_prefix {
                break;
            }

            let pos = row.0.len() - 24;
            let hasVal = row.0[4] == 1; // TODO don't think this flag is necessary for references, we can save a byte
            let ref_pk = &row.0[5..pos];
            let mut found = false;
            let target = self.db.get_resource_by_pk(ref_pk.try_into().unwrap());
            if let Err(e) = target {
                continue;
            }
            if let Some(target) = target.unwrap() {
                let el = Element::new(rawbson::elem::ElementType::EmbeddedDocument, target.as_ref());
                let root = Rc::new(SystemType::Element(el));
                let ctx = UnresolvableExecContext::new(root);
                let r = eval(&ctx, &self.rpath_expr, ctx.root_resource());
                if let Ok(r) = r {
                    found = r.is_truthy();
                }
                else {
                    warn!("failed to evaluate fhirpath expression on the target resource for reference search by identifier");
                }
            }

            if found {
                let mut tmp: [u8; 24] = [0; 24];
                tmp.copy_from_slice(&row.0[pos..]);
                res_keys.insert(tmp, true);
            }
        }

        res_keys
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use rawbson::{Doc, DocBuf};
    use serde_json::Value;
    use crate::barn::CF_INDEX;
    use crate::{configure_log4rs, search};
    use crate::search::executor::to_index_scanner;
    use crate::utils::bson_utils::get_str;
    use crate::utils::test_utils::{read_bundle, TestContainer};
    use super::*;

    #[test]
    fn test_reference_search_using_scanner() -> Result<(), Error> {
        let tc = TestContainer::new();
        let api_base = tc.setup_api_base_with_example_patient();
        let bundle = read_bundle();
        api_base.bundle(bundle).unwrap();

        let (spd, expr) = api_base.schema.get_search_param_expr_for_res("subject", "Encounter").unwrap();
        let expr = expr.unwrap();
        let mut itr = api_base.db.new_index_iter(&expr.hash);
        let (k, v) = itr.next().unwrap();
        assert_eq!(expr.hash, &k[0..4]);

        let patient_rd = api_base.schema.get_res_def_by_name("Patient")?;
        let rd = api_base.schema.get_res_def_by_name("Encounter")?;
        let encounter = api_base.db.get_resource_iter(rd).next().unwrap();
        let patient_ref = get_str(&encounter, "subject.reference");
        let patient_id = patient_ref.strip_prefix("Patient/").unwrap();
        let mut candidates = vec![];
        candidates.push((format!("subject:Patient eq \"{}\"", patient_ref), 1, patient_id));
        candidates.push((format!("subject eq \"{}\"", patient_ref), 1, patient_id));
        candidates.push((format!("subject eq \"{}\"", patient_id), 1, patient_id));

        for (input, expected, expected_target_id) in candidates {
            println!("{}", input);
            let filter = search::parse_filter(&input)?;
            let mut idx_scanner = to_index_scanner(&filter, rd, &api_base.schema, &api_base.db)?;
            let keys = idx_scanner.collect_all();
            assert_eq!(expected, keys.len());
            let target_id = Ksuid::from_base62(expected_target_id)?;
            let mut target_res_pk = [0; 24];
            target_res_pk[..4].copy_from_slice(&patient_rd.hash);
            target_res_pk[4..].copy_from_slice(target_id.as_bytes());
            let target_res = api_base.db.get_resource_by_pk(&target_res_pk)?.unwrap();
            let target_res = Doc::new(target_res.as_ref())?;
            let actual_target_id = target_res.get_str("id")?.unwrap();
            assert_eq!(expected_target_id, actual_target_id);
        }

        let input = format!("subject:Patient eq \"Observation/{}\"", patient_id);
        let filter = search::parse_filter(&input)?;
        let mut idx_scanner = to_index_scanner(&filter, rd, &api_base.schema, &api_base.db);
        assert!(idx_scanner.is_err());

        Ok(())
    }

    #[test]
    fn test_reference_search_by_identifier() -> Result<(), Error> {
        let tc = TestContainer::new();
        let api_base = tc.setup_api_base_with_example_patient();
        let bundle = read_bundle();
        api_base.bundle(bundle).unwrap();

        let (spd, expr) = api_base.schema.get_search_param_expr_for_res("service-provider", "Encounter").unwrap();
        let expr = expr.unwrap();
        let mut itr = api_base.db.new_index_iter(&expr.hash);
        let (k, v) = itr.next().unwrap();
        assert_eq!(expr.hash, &k[0..4]);

        let org_rd = api_base.schema.get_res_def_by_name("Organization")?;
        let rd = api_base.schema.get_res_def_by_name("Encounter")?;
        let encounter = api_base.db.get_resource_iter(rd).next().unwrap();
        let org_ref = get_str(&encounter, "serviceProvider.reference");
        let org_id = org_ref.strip_prefix("Organization/").unwrap();
        let mut candidates = vec![];

        // Organization's identifier
        let system = "https://github.com/synthetichealth/synthea";
        let code = "9e27";

        candidates.push((format!("service-provider:identifier eq \"{}\"", code), 1, org_id));
        candidates.push((format!("service-provider:identifier eq \"|{}\"", code), 1, org_id));
        candidates.push((format!("service-provider:identifier eq \"{}|\"", system), 1, org_id));
        candidates.push((format!("service-provider:identifier eq \"{}|{}\"", system, code), 1, org_id));

        for (input, expected, expected_target_id) in candidates {
            println!("{}", input);
            let filter = search::parse_filter(&input)?;
            let mut idx_scanner = to_index_scanner(&filter, rd, &api_base.schema, &api_base.db)?;
            let keys = idx_scanner.collect_all();
            assert_eq!(expected, keys.len());
            let target_id = Ksuid::from_base62(expected_target_id)?;
            let mut target_res_pk = [0; 24];
            target_res_pk[..4].copy_from_slice(&org_rd.hash);
            target_res_pk[4..].copy_from_slice(target_id.as_bytes());
            let target_res = api_base.db.get_resource_by_pk(&target_res_pk)?.unwrap();
            let target_res = Doc::new(target_res.as_ref())?;
            let actual_target_id = target_res.get_str("id")?.unwrap();
            assert_eq!(expected_target_id, actual_target_id);
        }

        Ok(())
    }
}