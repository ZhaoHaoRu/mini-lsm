# Bloom Filters

I have modified:
- `mini-lsm-starter/src/block/filter.rs`
- `mini-lsm-starter/src/block/builder.rs`
- `mini-lsm-starter/src/table/builder.rs`
- `mini-lsm-starter/src/table/iterator.rs`

## Structure of SSTable and Bloom Filter

Mimicking the structure of SSTables in LevelDB and adding a Bloom filter, the structure of SSTables is as follows:

```shell
+-------------------------+
|       Data Blocks       |
+-------------------------+
|      Filter Blocks      |
+-------------------------+
|       Meta Blocks       |
+-------------------------+
|   Filter Index Blocks   |
+-------------------------+
| Meta block offset (u32) |			# Indicate the position of the meta block
+-------------------------+
| Filter block offset (u32)|			# Indicate the position of the filter index block
+-------------------------+
```

- In Filter Blocks, Bloom filter data is stored, and in Filter Index Blocks, offsets for each filter block are stored.

```plaintext
-------------------------------------------------------
| offset1 (u32) | offset2 (u32) | ... | offsetN (u32) |
-------------------------------------------------------
```

## Usage of Bloom Filter

- To speed up data query efficiency in SSTables, before directly querying the content of a data block, LevelDB first determines whether the specified data block contains the required data based on the filtering data in the filter block. If it is determined that the data does not exist, there is no need to search this data block.
- Filter Index Blocks store the index of the Bloom filter, Meta Blocks store the metadata of the SSTable, and Data Blocks store the data of the SSTable.

- When generating SSTables, a filter block is generated for each data block, and a ready-made crate from `cargo.io` is used for the filter block.
    - The size of each filter block is 1/32 of a data block size.

## Test
check testcases in `mini-lsm-starter/src/block/filter.rs` to test.
