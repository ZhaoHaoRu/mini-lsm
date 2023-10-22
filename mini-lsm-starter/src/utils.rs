use std::path::PathBuf;
use tempfile::tempdir;

/// Describes a file on disk.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileMetaData {
    pub name: String,
    pub size: usize,
    // the key range for SstTable
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    Log,
    DBLock,
    Table,
    Descriptor,
    Current,
    Temp,
    InfoLog,
}

pub fn generate_sst_name(path: Option<&PathBuf>, sst_id: usize) -> PathBuf {
    if path.is_none() {
        return tempdir().unwrap().path().join(sst_id.to_string() + ".sst");
    }
    path.unwrap().join(sst_id.to_string() + ".sst")
}

