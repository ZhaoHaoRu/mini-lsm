# LSM in a Week

[![CI (main)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml/badge.svg)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml)

Build a simple key-value storage engine in a week!

## Tutorial

The tutorial is available at [https://skyzh.github.io/mini-lsm](https://skyzh.github.io/mini-lsm). You can use the provided starter
code to kick off your project, and follow the tutorial to implement the LSM tree.

## Development

```
cargo x install-tools
cargo x check
cargo x book
```

If you changed public API in the reference solution, you might also need to synchronize it to the starter crate.
To do this, use `cargo x sync`.

## Progress

The tutorial has 8 parts (which can be finished in 7 days):

* Day 1: Block encoding. SSTs are composed of multiple data blocks. We will implement the block encoding.
* Day 2: SST encoding.
* Day 3: MemTable and Merge Iterators.
* Day 4: Block cache and Engine. To reduce disk I/O and maximize performance, we will use moka-rs to build a block cache
  for the LSM tree. In this day we will get a functional (but not persistent) key-value engine with `get`, `put`, `scan`,
  `delete` API.
* Day 5: Compaction. Now it's time to maintain a leveled structure for SSTs.
* Day 6: Recovery. We will implement WAL and manifest so that the engine can recover after restart.
* Day 7: Bloom filter and key compression. They are widely-used optimizations in LSM tree structures.

We have reference solution up to day 4 and tutorial up to day 4 for now.

### compaction

目前实现了leveled compaction策略

#### SSTable结构

- 每个SSTable文件的固定大小为256M，从ImmutableMemTable创建的SSTable文件flush到Level-0中

- 每个Level有SSTable文件数量的限制。在除了Level-0的任意Level中，两级Level之间的SSTable文件数量呈指数级倍数。比如：Level-1中有10个SSTable文件，Level-2有100个SSTable文件

- 在除了Level-0的任意Level中，SSTable文件之间所包含的key的范围不重叠。（也就是说，每个Level的所有SSTable文件，可以看做是一个大的SSTable文件）

#### compaction算法

- 如果Level-0中SSTable数量超过限制（目前限制的是4），那么自动回将这4个Level-0的SSTable文件与Level-1的所有在范围中的n个SSTable文件进行Compaction。

- 在Compaction过程中，首先对参与compaction的SSTable文件按key进行归并排序，然后将排序后结果写入到新的SSTable文件中，如果SSTable文件大小到了256M上限，就新生成SSTable继续写。如此类推，直到写完所有数据。

- 删除参与Compaction的Level-0的4个和Level-1的n个旧的SSTable文件 此时Level-0的SSTable便merge到Level-1中了，那么如果Level-1的SSTable文件数量超过上限，那么就从Level-1中选出 n 个超量的最新的SSTable文件，然后将其与Level-2中的SSTable文件进行Compaction。

- 查看选出的Level-1 SSTable文件中key的范围，从Level-2中选出能覆盖该范围的所有SSTable文件

- 将以上的所有SSTable文件根据上面介绍的算法继续进行Compaction

- 对于其他层以此类推

### Bloom filter
仿照leveldb中SSTable的结构，在加入Bloom filter之后，SSTable的结构如下：
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
| Meta block offset (u32) |			# indicate the position of meta block
+-------------------------+
|Filter block offset (u32)|			# indicate the position of the filter index block
+-------------------------+
```
- 其中，Filter Blocks中存储的是Bloom filter的数据，Filter Index Blocks中存储的是每个filter block的offset
```
-------------------------------------------------------
| offset1 (u32) | offset2 (u32) | ... | offsetN (u32) |
-------------------------------------------------------
```
- 为了加快SSTable中数据查询的效率，在直接查询data block中的内容之前，leveldb首先根据filter block中的过滤数据判断指定的data block中是否有需要查询的数据，若判断不存在，则无需对这个data block进行数据查找。
，Filter Index Blocks中存储的是Bloom filter的索引，Meta Blocks中存储的是SSTable的元信息，Data Blocks中存储的是SSTable的数据。
- 在生成SSTable的时候，会为每个data block生成一个filter block，filter block使用了`cargo.io`中[现成的crate](https://docs.rs/bloomfilter/latest/bloomfilter/struct.Bloom.html)。
  - 每个filter block的大小为一个data block大小的 1/32

### WAL log

#### Log format

- 这里参考了leveldb log的格式，log文件包含一系列32KB大小的block，只有最后一个block可能是一个不完全的block

- 每个block由一系列的record构成，record的格式如下：
```
--------------------------------------------------------
｜ record length (u16) | record type (u8) | record data |
--------------------------------------------------------
```
- 如果key－value对过大，无法存放在单个block中，可能存放在多个不同的block中, 所以这里需要`record type`字段：

```
FULL == 1: 记录完全在一个block中
FIRST == 2: 当前block容纳不下所有的内容，记录的第一片在本block中
MIDDLE == 3: 记录的内容的起始位置不在本block，结束未知也不在本block
LAST == 4: 记录的内容起始位置不在本block，但 结束位置在本block
```
- record中记录的是key-value对的内容，key-value对的格式如下：
  - 如果`value length` = 0, 说明是delete
```
-------------------------------------------------------
| key length (u16) | value length (u16) | key | value |
-------------------------------------------------------
```

- 每个memtable / immutable memtable对应一个log file，log file的名字编号越大，对应的log越新

#### manifest
- log_number_：最小的有效 log number。小于 log_numbers_ 的 log 文件都可以删除。
- next_file_number_：下一个文件的编号 (file_number_)。
- 这里要求memtable的log number和其对应的sst文件的编号相同
- 在每次sync和compaction结束后，都需要更新manifest文件