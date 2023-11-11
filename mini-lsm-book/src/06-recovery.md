# Write-Ahead Log for Recovery

In this part, we will implement write-ahead log (WAL) for recovery. I have modified:
- `mini-lsm-starter/src/log/builder.rs`
- `mini-lsm-starter/src/log/reader.rs`
- `mini-lsm-starter/src/log/iterator.rs`
- `mini-lsm-starter/src/log.rs`
- `mini-lsm-starter/src/lsm_storage.rs`

## Log Format

- The log file is composed of a series of records, and its format is inspired by the LevelDB log. Each record in the log file contains the content of a key-value pair, with the key-value pair format as follows:
  - If `value length` = 0, it indicates a delete.
```plaintext
-------------------------------------------------------
| key length (u16) | value length (u16) | key | value |
-------------------------------------------------------
```

- A block is the unit of the log cached in memory. When the length of the log cached in memory exceeds 128B, the log is written to disk.

- Each MemTable / Immutable MemTable corresponds to a log file, and the name of the log file is numbered, where a higher number corresponds to a newer log.

## Manifest

- The filename of the manifest file is `manifest`.
- log_number_: The smallest valid log number. Log files with numbers less than log_numbers_ can be deleted.
- next_file_number_: The next file number (file_number_).
- All sstable's sst_file_id, each level occupies one line.
- Here, it is required that the log number of the MemTable be the same as its corresponding SST file number.
- After each sync and compaction, the manifest file needs to be updated.

## Recovery

- Upon each startup, the manifest file is read, and based on the recorded log numbers and SST file numbers in the manifest file:
  - Read log files and write key-value pairs into MemTable.
  - Read SST files and recover the structure of SSTable.

## Persistent

- Before each insertion and deletion of key-value pairs, they are written to the log file.
- After each sync and compaction, the current structure is persistently stored by writing the manifest file to disk.
- After each sync and compaction, expired log files are deleted.

## Test

check testcases in `mini-lsm-starter/src/tests/day4_test.rs` and run `mini-lsm-starter/src/log/tests.rs` to test.
