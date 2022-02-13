use bson::{Bson, bson, Document};
use bson::spec::ElementType;
use chrono::Utc;
use ksuid::Ksuid;
use log::debug;
use rocksdb::WriteBatch;
use crate::barn::Barn;
use crate::errors::RaError;
use crate::res_schema::SchemaDef;
use crate::{bson_utils, ResourceDef};

impl Barn {
    pub fn insert_batch(&self, ksid: &Ksuid, res_def: &ResourceDef, mut data: Document, wb: &mut WriteBatch, sd: &SchemaDef) -> Result<Document, RaError> {
        let res_id = ksid.to_base62();
        debug!("inserting a {} with ID {}", &res_def.name, &res_id);
        data.insert("id", Bson::from(res_id));

        // update metadata
        let mut meta = data.get_mut("meta");
        if let None = meta {
            data.insert("meta", bson!({}));
            meta = data.get_mut("meta");
        }
        // TODO is the below check needed??
        // else if let Some(m) = meta {
        //     if m.element_type() != ElementType::EmbeddedDocument {
        //
        //     }
        // }
        let mut meta = meta.unwrap().as_document_mut().unwrap();
        meta.insert("versionId", Bson::from(1));
        // this has to be inserted as a string otherwise when serialized to JSON
        // dates are formatted in extended-JSON format
        meta.insert("lastUpdated", Bson::from(Utc::now().format(bson_utils::DATE_FORMAT).to_string()));

        // TODO move this block to update and replace calls
        // check version history
        // let history_pk = res_def.new_history_prefix_id(ksid.as_bytes());
        // let history_count_rec = self.db.get(&history_pk);
        // let mut history_count = 1; // history number always points to the current version (and it always starts with 1)
        // if history_count_rec.is_ok() {
        //     let history_count_rec = history_count_rec.unwrap().unwrap();
        //     history_count = utils::u32_from_le_bytes(history_count_rec.as_bytes());
        // }

        let mut vec_bytes = Vec::new();
        data.to_writer(&mut vec_bytes);

        let pk = res_def.new_id(ksid.as_bytes());
        wb.put(&pk, vec_bytes.as_slice());

        // handle references
        for (ref_prop, _) in &res_def.ref_props {
            let ref_node = data.get(ref_prop.as_str());
            if let Some(ref_node) = ref_node {
                match ref_node.element_type() {
                    ElementType::Array => {
                        let ref_arr = ref_node.as_array();
                        if let Some(ref_arr) = ref_arr {
                            for item in ref_arr.iter() {
                                self.insert_ref(ref_prop, ksid.as_bytes(), item, res_def, wb, sd);
                            }
                        }
                    },
                    _ => {
                        self.insert_ref(ref_prop, ksid.as_bytes(), ref_node, res_def, wb, sd);
                    }
                }
            }
        }
        Ok(data)
    }

    fn insert_ref<S: AsRef<str>>(&self, ref_at_name: S, from_id: &[u8], item: &Bson, from: &ResourceDef, wb: &mut WriteBatch, sd: &SchemaDef) -> Result<(), RaError> {
        if let Some(item) = item.as_document() {
            if let Some(target) = item.get("reference") {
                if let Some(target) = target.as_str() {
                    let parts: Vec<&str> = target.split("/").collect();
                    if parts.len() == 2 {
                        let to = sd.resources.get(parts[0]);
                        if let None = to {
                            return Err(RaError::bad_req(format!("resource not found with the name {} in the reference {}", parts[0], target)));
                        }
                        let to_id = Ksuid::from_base62(parts[1]);
                        if let Err(e) = to_id {
                            return Err(RaError::bad_req(format!("reference ID {} is in invalid format", parts[1])));
                        }
                        let to = to.unwrap();
                        let to_id = to_id.unwrap();
                        let empty_val: &[u8;0] = &[0;0];
                        let fwd_id = from.new_ref_fwd_id(ref_at_name, from_id, to, to_id.as_bytes());
                        wb.put(fwd_id, empty_val);

                        let rev_id = from.new_ref_rev_id(to_id.as_bytes(), to, from_id);
                        wb.put(rev_id, empty_val);
                    }
                    else {
                        return Err(RaError::bad_req(format!("invalid format of reference {}, should be <Resource-name>/<ID>", target)));
                    }
                }
                else {
                    // TODO is this necessary to log or will the validation ensure that this case is covered?
                }
            }
        }
        Ok(())
    }
}