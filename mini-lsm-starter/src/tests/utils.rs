use crate::table::{SsTable, SsTableBuilder};
use std::path::Path;

pub fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:05}", idx).into_bytes()
}

pub fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

pub fn new_value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx + 1).into_bytes()
}

pub fn generate_sst(
    lower_bound: usize,
    upper_bound: usize,
    sst_id: usize,
    is_new: bool,
    dir: &Path,
) -> SsTable {
    let mut builder = SsTableBuilder::new(128);
    for idx in lower_bound..upper_bound {
        let key = key_of(idx);
        if is_new {
            let value = new_value_of(idx);
            builder.add(&key[..], &value[..]);
        } else {
            let value = value_of(idx);
            builder.add(&key[..], &value[..]);
        }
    }
    let path = dir.join(sst_id.to_string() + ".sst");
    builder.build(sst_id, None, path).unwrap()
}
