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
use crate::errors::EvalError;
use crate::rapath::engine::{eval, ExecContext, UnresolvableExecContext};
use crate::rapath::expr::Ast;
use crate::rapath::parser::parse;
use crate::rapath::scanner::scan_tokens;
use crate::rapath::stypes::SystemType;
use crate::res_schema::SchemaDef;
use crate::search::index_scanners::{IndexScanner, SelectedResourceKey};
use crate::search::{ComparisonOperator, Modifier};
use crate::search::ComparisonOperator::*;
use crate::search::executor::create_index_scanner;

pub struct ReferenceIndexScanner<'f, 'd: 'f> {
    ref_id: Ksuid,
    ref_type: Option<[u8; 4]>,
    itr: DBIterator<'d>,
    index_prefix: &'f [u8],
    modifier: Modifier<'f>
}

pub struct ReferenceIdIndexScanner<'f> {
    rpath_expr: Ast<'f>,
    itr: DBIterator<'f>,
    db: &'f Barn,
    index_prefix: &'f [u8]
}

pub struct ReferenceChainIndexScanner<'f> {
    itr: DBIterator<'f>,
    ref_type: Option<[u8; 4]>,
    db: &'f Barn,
    sd: &'f SchemaDef,
    index_prefix: &'f [u8],
    chain: ChainedParam<'f>
}

pub struct ChainedParam<'f> {
    name: &'f str,
    modifier: Modifier<'f>,
    value: Option<&'f str>,
    operator: &'f ComparisonOperator,
    cached_scanners: HashMap<[u8;4], Box<dyn IndexScanner<'f> + 'f>>,
    child: Option<Box<ChainedParam<'f>>>
}

pub fn new_reference_scanner<'f, 'd: 'f>(ref_id: Ksuid, ref_type: Option<[u8; 4]>, itr: DBIterator<'d>, index_prefix: &'f [u8], modifier: Modifier<'f>) -> ReferenceIndexScanner<'f, 'd> {
    ReferenceIndexScanner { ref_id, ref_type, itr, index_prefix, modifier }
}

pub fn new_reference_id_scanner<'f>(rpath_expr: Ast<'f>, db: &'f Barn, index_prefix: &'f [u8]) -> ReferenceIdIndexScanner<'f> {
    let itr = db.new_index_iter(index_prefix);
    ReferenceIdIndexScanner { rpath_expr, itr, db, index_prefix }
}

pub fn new_reference_chain_scanner<'f>(chain: ChainedParam<'f>, ref_type: Option<[u8; 4]>, db: &'f Barn, sd: &'f SchemaDef, index_prefix: &'f [u8]) -> ReferenceChainIndexScanner<'f> {
    let itr = db.new_index_iter(index_prefix);
    ReferenceChainIndexScanner{itr, db, sd, index_prefix, chain, ref_type}
}

impl<'f, 'd: 'f> IndexScanner<'f> for ReferenceIndexScanner<'f, 'd> {
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
            let hasVal = row.0[4] == 1;
            if !hasVal {
                continue;
            }

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

impl<'f> ReferenceIdIndexScanner<'f> {
    fn eval_row(&self, ref_pk: &'f [u8]) -> Result<bool, EvalError> {
        let mut eval_result = false;
        let target = self.db.get_resource_by_pk(ref_pk.try_into().unwrap());
        if let Err(e) = target {
            return Err(EvalError::new(format!("{:?}", e)));
        }

        if let Some(target) = target.unwrap() {
            let el = Element::new(rawbson::elem::ElementType::EmbeddedDocument, target.as_ref());
            let root = Rc::new(SystemType::Element(el));
            let ctx = UnresolvableExecContext::new(root);
            let r = eval(&ctx, &self.rpath_expr, ctx.root_resource());
            if let Ok(r) = r {
                eval_result = r.is_truthy();
            }
            else {
                warn!("failed to evaluate fhirpath expression on the target resource for reference search by identifier");
            }
        }

        Ok(eval_result)
    }
}

impl<'f> IndexScanner<'f> for ReferenceIdIndexScanner<'f> {
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
            let hasVal = row.0[4] == 1;
            if !hasVal {
                continue;
            }

            let ref_pk = &row.0[5..pos];
            let eval_result = self.eval_row(ref_pk);
            if let Err(e) = eval_result {
                continue;
            }

            if eval_result.unwrap() {
                let mut tmp: [u8; 24] = [0; 24];
                tmp.copy_from_slice(&row.0[pos..]);
                res_keys.insert(tmp, true);
            }
        }

        res_keys
    }

    fn chained_search(&mut self, res_pks: &mut HashMap<[u8; 24], [u8; 24]>, sd: &SchemaDef, db: &Barn) -> Result<HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>>, EvalError> {
        let mut keys: HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>> = HashMap::new();
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
            if res_pks.is_empty() {
                break;
            }
            let pos = row.0.len() - 24;
            let this_pk = &row.0[pos..];
            let ref_to_res_pk = res_pks.get(this_pk);
            if let Some(ref_to_res_pk) = ref_to_res_pk {
                let ref_pk = &row.0[5..pos];
                let eval_result = self.eval_row(ref_pk);
                if let Err(e) = eval_result {
                    continue;
                }
                if eval_result.unwrap() {
                    let this_res_type= &row.0[pos..pos+4];
                    if !keys.contains_key(this_res_type) {
                        let this_res_type_sized = this_res_type.try_into().unwrap();
                        keys.insert(this_res_type_sized, HashMap::new());
                    }
                    let this_pk_sized = this_pk.try_into().unwrap();
                    keys.get_mut(this_res_type).unwrap().insert(this_pk_sized, *ref_to_res_pk);
                    res_pks.remove(this_pk);
                }
            }
        }

        Ok(keys)
    }
}

impl<'f> ChainedParam<'f> {
    pub fn new(name: &'f str, modifier: Modifier<'f>, value: Option<&'f str>, operator: &'f ComparisonOperator) -> ChainedParam<'f> {
        let cached_scanners = HashMap::new();
        ChainedParam{name, modifier, value, cached_scanners, child: None, operator}
    }

    pub fn add_to_tail(&mut self, child: ChainedParam<'f>) {
        let mut tmp = self;
        loop {
            if let None = tmp.child {
                tmp.child = Some(Box::new(child));
                break;
            }

            tmp = tmp.child.as_mut().unwrap();
        }
    }

    pub fn chained_search(&mut self, res_pks: &mut HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>>, sd: &'f SchemaDef, db: &'f Barn) -> Result<HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>>, EvalError> {
        let mut chain_result: HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>> = HashMap::new();
        for (ref res_type, internal_map) in res_pks {
            if let None = self.cached_scanners.get(*res_type) {
                let mut value = "";
                if let Some(v) = self.value { // value exists for the last attribute in the chain
                    value = v;
                }
                let rd = sd.get_res_def_by_hash(*res_type);
                if let Err(e) = rd {
                    return Err(EvalError::new(format!("{:?}", e)));
                }
                let scanner = create_index_scanner(self.name, value, self.operator, self.modifier, None, rd.unwrap(), sd, db)?;
                self.cached_scanners.insert(**res_type, scanner);
            }

            let scanner = self.cached_scanners.get_mut(*res_type).unwrap();
            chain_result = scanner.chained_search(internal_map, sd, db)?;
            if let Some(ch) = &mut self.child {
                chain_result = ch.chained_search(&mut chain_result, sd, db)?;
            }
        }
        Ok(chain_result)
    }
}

impl <'f> IndexScanner<'f> for ReferenceChainIndexScanner<'f> {
    fn next(&mut self) -> SelectedResourceKey {
        todo!()
    }

    fn collect_all(&mut self) -> HashMap<[u8; 24], bool> {
        let mut res_keys = HashMap::new();
        let batch_size: u32 = 1; // TODO should be made configurable
        let mut count = 0;
        let mut batch: HashMap<[u8;4], HashMap<[u8; 24], [u8; 24]>> = HashMap::new();
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
            let hasVal = row.0[4] == 1;
            if !hasVal {
                continue;
            }
            let ref_res_type = &row.0[5..9];
            if let Some(ref expected_ref_type) = self.ref_type {
                if expected_ref_type != ref_res_type {
                    continue;
                }
            }

            let ref_res_pk = &row.0[5..pos];
            let this_pk = &row.0[pos..];

            if !batch.contains_key(ref_res_type) {
                batch.insert(ref_res_type.try_into().unwrap(), HashMap::new());
            }
            let inner_map = batch.get_mut(ref_res_type).unwrap();
            let map_key = ref_res_pk.try_into().unwrap();
            inner_map.insert(map_key, this_pk.try_into().unwrap());
            count += 1;

            if count % batch_size == 0 {
                let chain = &mut self.chain;
                let found = chain.chained_search(&mut batch, self.sd, self.db);
                if let Err(e) = found {
                    warn!("{}", e);
                    break;
                }
                for (res_type, results) in found.unwrap() {
                    for (_, v) in results {
                        res_keys.insert(v, true);
                    }
                }
                batch.clear();
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
    use crate::utils::test_utils::{read_bundle, read_chained_search_bundle, TestContainer};
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

    #[test]
    fn test_chained_search() -> Result<(), Error> {
        configure_log4rs();
        let tc = TestContainer::new();
        let api_base = tc.setup_api_base_with_example_patient();
        let bundle = read_bundle();
        api_base.bundle(bundle).unwrap();

        let bundle = read_chained_search_bundle();
        api_base.bundle(bundle).unwrap();

        let rd = api_base.schema.get_res_def_by_name("DiagnosticReport")?;

        let mut candidates = Vec::new();
        candidates.push((format!("result.subject:identifier eq \"{}\"", "444222222"), 1));
        // candidates.push((format!("result.specimen.patient:identifier eq \"{}\"", "444222222"), 1));

        for (input, expected) in candidates {
            println!("{}", input);
            let filter = search::parse_filter(&input)?;
            let mut idx_scanner = to_index_scanner(&filter, rd, &api_base.schema, &api_base.db)?;
            let keys = idx_scanner.collect_all();
            assert_eq!(expected, keys.len());
            for (k, _) in keys {
                let target_res = api_base.db.get_resource_by_pk(&k)?.unwrap();
                let target_res = Doc::new(target_res.as_ref())?;
                let res_type = target_res.get_str("resourceType").unwrap().unwrap();
                assert_eq!("DiagnosticReport", res_type);
            }
        }

        Ok(())
    }
}