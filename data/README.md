# DB file conventions

## SSTable format

Example can be found in data/examples/sstable.sst (bloom filter size 100, data block size 4KB)

### File convention

[4 bytes]   key length (uint32)
[N bytes]   key data
[4 bytes]   value length (uint32)
[M bytes]   value data
[8 bytes]   timestamp (int64)
[1 byte]    tombstone flag (0/1)

### Header

- metadata,
- bloom filter,
- sparse index

### Data block

The actual content database stores, sorted string entries (with metadata and offsets).
