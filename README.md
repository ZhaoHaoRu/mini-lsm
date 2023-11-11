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

## Tutorial Progress

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

The tutorial has reference solution up to day 4 and tutorial up to day 4 for now.

## My Progress

I have tried to design and complete day5-7 by myself, including:

* compaction
* recovery
* bloom filter

Please check `mini-lsm-book/src` for implementation details.

In the future, I will try to add Integration tests and a configuration file to support parameter modification.
