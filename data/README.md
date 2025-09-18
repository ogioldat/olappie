# DB file conventions

## SSTable format

Example can be found in data/examples/sstable.sst (bloom filter size 100, data block size 4KB)

### File convention

[len key] key [len value] value [8] timestamp [1] tombstone flag
4 key1 10 some value 8 1751374012 1 0,4 key2 0  8 1751354012 1 1

### Header

- metadata,
- bloom filter,
- sparse index

### Data block

The actual content database stores, sorted string entries (with metadata and offsets).
