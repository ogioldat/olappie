![OLAPpie Banner](https://img.shields.io/badge/🐘🍎🍰_TTRUNKSDB-A_TOY_NOSQL_DB-purple?style=for-the-badge)

# ttrunksdb


[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat-square&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen?style=flat-square)](.)
[![Contributions](https://img.shields.io/badge/Contributions-Welcome-orange?style=flat-square)](CONTRIBUTING.md)

**ttrunksdb** is an experimental write-intensive database engine inspired by ScyllaDB.
## ✨ Key Features

- 🚀 **LSM Tree Architecture** - Optimized for high-volume writes
- 💾 **Binary SSTable Format** - Efficient disk storage with bloom filters
- 📊 **Data Generation** - Built-in realistic test data generator

---

## 🚀 Quick Start

### Prerequisites
- **Go 1.25+**
- **Environment file** (copy from `.env.example`)

### 1️⃣ Start the Database Server
```bash
go run cmd/server/main.go
```
*Launches TCP server on port 8080*

### 2️⃣ Generate Test Data
```bash
# Generate 5,000 realistic records
go run cmd/datagen/main.go -n 5000 -size 128
```

### 3️⃣ Interactive CLI
```bash
go run cmd/cli/main.go
```

**CLI Commands:**
- `read <key>` - Retrieve value for key
- `write <key> <value>` - Store key-value pair
- `list` - Show all entries
- `help` - Command reference
- `quit` - Exit gracefully

### 4️⃣ Debug SSTable Files
```bash
go run cmd/debug/deserialize_sstables.go
```
*Converts binary SSTables to human-readable text format*

---

## 🛠️ Development Tools

| Command | Description | Example |
|---------|-------------|---------|
| `cmd/server` | TCP database server | `go run cmd/server/main.go` |
| `cmd/cli` | Interactive client | `go run cmd/cli/main.go` |
| `cmd/datagen` | Data generator | `go run cmd/datagen/main.go -n 1000` |
| `cmd/debug` | SSTable inspector | `go run cmd/debug/deserialize_sstables.go` |

### 🎮 Data Generator Options
```bash
go run cmd/datagen/main.go [flags]
  -n <number>     Records to generate (default: 1000)
  -size <bytes>   Value size in bytes (default: 64)
  -server <addr>  Server address (default: localhost:8080)
```

---

## 🔧 SSTable Binary Format

Our custom binary format optimizes for both storage efficiency and read performance:

### Header Section
```
[4 bytes]   bloom filter size (int32)
[N bytes]   bloom filter bits (string)
[4 bytes]   sparse index size (int32)
[M bytes]   sparse index data (string)
```

### Data Records
```
[4 bytes]   key length (int32)
[N bytes]   key data (string)
[4 bytes]   value length (int32)
[M bytes]   value data (bytes)
[4 bytes]   timestamp size = 8 (int32)
[8 bytes]   timestamp (int64)
[4 bytes]   tombstone size = 1 (int32)
[1 byte]    tombstone flag (bool)
```

*All integers encoded in little-endian byte order*

📋 **[Detailed Binary Layout Specification →](data/README.md)**

---

### Running Tests
```bash
go test ./...
```

---

## 📋 Development Roadmap

### ✅ Completed Features
- [x] **Memtable writes** - In-memory write buffer with efficient operations
- [x] **Memtable reads** - Fast in-memory key-value lookups
- [x] **Binary SSTable writes** - Efficient disk serialization with headers
- [x] **SSTable reads** - Sparse index and bloom filter optimized lookups
- [x] **L0 SSTables** - Level 0 storage implementation
- [x] **CLI client** - Interactive terminal interface with Bubble Tea
- [x] **Database server** - TCP server with JSON protocol
- [x] **Debug tools** - SSTable inspection and visualization utilities

### 🚧 TODO
- [ ] **Compaction engine** - Background SSTable merging and optimization
- [ ] **Multi-level SSTables** - Tiered storage for better performance
- [ ] **WAL recovery** - Write-ahead logging for crash consistency
- [ ] **Performance benchmarks** - Comprehensive testing suite for throughput/latency
- [ ] **ACID compliance assessment** - Transaction isolation and consistency analysis
- [ ] **Test coverage improvement** - Expand unit and integration test coverage
- [ ] **Query optimization** - Range queries and batch operations
- [ ] **Compression support** - LZ4/Snappy compression for SSTables
- [ ] **Metrics & monitoring** - Prometheus integration and runtime statistics
- [ ] **Distributed deployment** - Multi-node clustering support

---

## 📚 Inspiration & References

- **[ScyllaDB](https://www.scylladb.com/)** - LSM tree implementation insights
- **[RocksDB](https://rocksdb.org/)** - LSM storage engine design patterns

---
