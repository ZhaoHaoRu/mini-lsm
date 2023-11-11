# Leveled Compaction

<!-- toc -->

In this part, I modified:

* `mini-lsm-starter/src/compaction.rs`
* `mini-lsm-starter/src/lsm_storage.rs`

Currently, the leveled compaction strategy has been implemented.

## SSTable Structure

* Each SSTable file has a fixed size of 256MB. SSTable files created from ImmutableMemTable are flushed to Level-0.

* Each level has a limit on the number of SSTable files. In any level except Level-0, the number of SSTable files between two levels is an exponential multiple. For example: Level-1 has 10 SSTable files, and Level-2 has 100 SSTable files.

* In any level except Level-0, the key ranges between SSTable files do not overlap. (In other words, all SSTable files in each level can be considered as one large SSTable file.)

## Compaction Algorithm

* If the number of SSTable files in Level-0 exceeds the limit (currently set to 4), then these 4 Level-0 SSTable files will automatically be compacted with all n SSTable files in Level-1 within the range.

* During the compaction process, the SSTable files participating in compaction are first merged and sorted by key. The sorted result is then written to a new SSTable file. If the size of the SSTable file reaches the 256MB limit, a new SSTable is generated to continue writing. This process continues until all data is written.

* Delete the 4 old SSTable files from Level-0 and n old SSTable files from Level-1 that participated in compaction. At this point, the SSTable from Level-0 is merged into Level-1. If the number of SSTable files in Level-1 exceeds the limit, select n latest SSTable files that exceed the limit from Level-1, and then compact them with SSTable files in Level-2.

* Check the key range of the selected Level-1 SSTable files and select all SSTable files in Level-2 that cover that range.

* Continue the compaction of all these SSTable files according to the algorithm described above.

* Repeat this process for other levels.

## Test

check testcases in `mini-lsm-starter/src/tests/day4_test.rs` and run `mini-lsm-starter/src/compaction.rs` to test.
 

