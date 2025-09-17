# OLAPpie 🍰

An experimental write intensive, simplified, OLAP DB engine.

[ClickHouse](https://github.com/ClickHouse/ClickHouse) caught my attention, aiming to understand it better by building it's core concepts from scratch.

LSM Tree DB scheme

![SSTable Diagram](https://www.scylladb.com/wp-content/uploads/sstable-diagram.png)

*Source: [ScyllaDB Blog](https://www.scylladb.com/2019/09/17/what-the-sstable/)*

## Getting Started

### Prerequisites
- Go 1.25.0 or later
- `.env` file in the root directory (see `.env.example`)

### Available Commands

#### Database Server
Start the TCP database server that handles client connections:

```bash
go run cmd/server/main.go
```

The server listens on port 8080 by default and accepts JSON commands over TCP.

#### Interactive CLI Client
Launch an interactive terminal interface to connect to the database:

```bash
go run cmd/cli/main.go
```

**Available CLI commands:**
- `read <key>` - Read value for a key
- `write <key> <value>` - Write value to a key
- `list` - List all key-value pairs
- `help` - Show available commands
- `quit` - Exit the CLI

#### Data Generator
Generate test data by connecting to a running server:

```bash
go run cmd/datagen/main.go [options]
```

**Options:**
- `-n <number>` - Number of records to generate (default: 1000)
- `-size <bytes>` - Size of generated values in bytes (default: 64)
- `-server <address>` - Server address (default: localhost:8080)

**Example:**
```bash
# Generate 5000 records with 128-byte values
go run cmd/datagen/main.go -n 5000 -size 128
```

### Usage Example

1. **Start the server:**
   ```bash
   go run cmd/server/main.go
   ```

2. **Generate test data:**
   ```bash
   go run cmd/datagen/main.go -n 1000
   ```

3. **Use the CLI to query data:**
   ```bash
   go run cmd/cli/main.go
   # Then use commands like: read <key>, list, etc.
   ```

## Architecture

### Core Components
- **LSM Tree Storage** - Write-optimized storage engine
- **Memtable** - In-memory write buffer
- **SSTable** - Sorted string tables for disk storage
- **TCP Server** - JSON-based client-server protocol

## SSTable structure

[key1, value1, timestamp1]
[key2, value2, timestamp2]
...
