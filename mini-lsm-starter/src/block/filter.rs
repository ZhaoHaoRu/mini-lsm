use bloomfilter::Bloom;
use std::cmp::max;

pub struct Filter {
    bloom_filter: Bloom<[u8]>,
}

impl Filter {
    /// generate a new filter block according to the data block content
    pub fn new(filter_block_size: usize) -> Self {
        let seed = [0u8; 32];
        let size = max(filter_block_size, 10);
        let filter = Bloom::new_with_seed(size, size / 10, &seed);
        Self {
            bloom_filter: filter,
        }
    }

    /// decode the filter block from the raw data and hash function
    pub fn decode(data: &[u8], filter_block_size: usize) -> Self {
        let shadow_filter: Bloom<[u8]> = Filter::new(filter_block_size).bloom_filter;
        let filter = Bloom::from_existing(
            data,
            shadow_filter.number_of_bits(),
            shadow_filter.number_of_hash_functions(),
            shadow_filter.sip_keys(),
        );
        Self {
            bloom_filter: filter,
        }
    }

    /// encode the filter block into raw data
    pub fn encode(&self) -> Vec<u8> {
        self.bloom_filter.bitmap()
    }

    /// check existence using bloom filter
    pub fn is_exist(&self, key: &[u8]) -> bool {
        self.bloom_filter.check(key)
    }

    /// insert a key into the bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        self.bloom_filter.set(key);
    }
}

#[cfg(test)]
mod tests {
    use crate::block::filter::Filter;

    #[test]
    fn test_filter() {
        let mut filter = Filter::new(128);
        filter.insert(b"11111");
        filter.insert(b"22222");
        filter.insert(b"33333");
        filter.insert(b"44444");
        assert!(filter.is_exist(b"11111"));
        assert!(filter.is_exist(b"22222"));
        assert!(filter.is_exist(b"33333"));
        assert!(filter.is_exist(b"44444"));
        let data = filter.encode();
        let filter2 = Filter::decode(&data, 128);
        assert!(filter2.is_exist(b"11111"));
        assert!(filter2.is_exist(b"22222"));
        assert!(filter2.is_exist(b"33333"));
        assert!(filter2.is_exist(b"44444"));
    }
}
