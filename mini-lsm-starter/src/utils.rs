use bytes::Bytes;
use std::env;
use std::ops::Bound;
use std::path::PathBuf;

pub fn generate_sst_name(path: Option<&PathBuf>, sst_id: usize) -> PathBuf {
    if path.is_none() {
        return env::current_dir()
            .unwrap()
            .join(sst_id.to_string() + ".sst");
    }
    path.unwrap().join(sst_id.to_string() + ".sst")
}

/// convert a slice of bytes to a slice of Bytes
pub fn from_slice_to_bytes(former: Bound<&[u8]>) -> Bound<Bytes> {
    match former {
        Bound::Excluded(data) => Bound::Excluded(Bytes::copy_from_slice(data)),
        Bound::Included(data) => Bound::Included(Bytes::copy_from_slice(data)),
        Bound::Unbounded => Bound::Unbounded,
    }
}
