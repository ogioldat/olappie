# DB file conventions

## SSTable Binary Format

Example can be found in data/examples/sstable.bin (bloom filter size 100, data block size 4KB)

### Binary Layout (Little Endian)

#### Header Section
```
[4 bytes]   bloom filter size (int32)
[N bytes]   bloom filter bits (string representation)
[4 bytes]   sparse index size (int32)
[M bytes]   sparse index data (string representation)
```

#### Data Block Section
Each record follows this format:
```
[4 bytes]   key length (int32)
[N bytes]   key data (string)
[4 bytes]   value length (int32)
[M bytes]   value data (bytes)
[4 bytes]   timestamp size (int32) - always 8
[8 bytes]   timestamp (int64)
[4 bytes]   tombstone size (int32) - always 1
[1 byte]    tombstone flag (bool: 0/1)
```

### File Structure Overview

1. **Metadata Section**: Contains bloom filter and sparse index for efficient lookups
2. **Data Block**: Sequential sorted records with size prefixes for each field
3. **All integers**: Encoded in little-endian byte order
4. **Size prefixes**: Allow for variable-length data and safe deserialization
