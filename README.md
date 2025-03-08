# Distributed SQLite with Raft and Sharding

This project implements a distributed SQLite database using the Raft consensus algorithm in Go. It allows multiple nodes to form a cluster, with one node acting as the leader and the others as followers. All write operations are forwarded to the leader, which then replicates them to the followers using the Raft protocol. The system now includes a proxy layer that supports sharding for horizontal scalability.

## Features

- Distributed SQLite database with Raft consensus
- HTTP API for executing SQL statements and queries
- Support for cluster membership changes (joining/leaving)
- Automatic leader election and failover
- Consistent reads and writes across the cluster
- Sharding support for horizontal scalability
- Automatic query routing based on table and shard key
- Table registration and management across shards
- Automatic table rebalancing between shards

## Architecture

The system consists of the following components:

1. **Server**: Manages the HTTP API and coordinates with the Raft consensus module
2. **FSM (Finite State Machine)**: Applies SQL commands to the SQLite database
3. **Raft**: Provides distributed consensus for database operations
4. **Proxy Layer**: Manages table registrations, sharding, and query routing

## Getting Started

### Prerequisites

- Go 1.21 or later
- SQLite

### Building

```bash
go build -o raft-sqlite
```

### Running a Single Node

```bash
./raft-sqlite -id node1 -data ./node1 -http :11000 -raft :12000
```

### Running a Cluster

Start the first node:

```bash
./raft-sqlite -id node1 -data ./node1 -http :11000 -raft :12000
```

Start additional nodes and join them to the cluster:

```bash
./raft-sqlite -id node2 -data ./node2 -http :11001 -raft :12001 -join localhost:11000
./raft-sqlite -id node3 -data ./node3 -http :11002 -raft :12002 -join localhost:11000
```

## API Usage

### Execute SQL Statement

```bash
curl -X POST http://localhost:11000/execute -d '{"query":"CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"}'
curl -X POST http://localhost:11000/execute -d '{"query":"INSERT INTO users (name, email) VALUES (?, ?)", "args":["John Doe", "john@example.com"]}'
```

### Query Data

```bash
curl -G http://localhost:11000/query --data-urlencode "q=SELECT * FROM users"
```

### Sharding API

#### Register a Table for Sharding

```bash
curl -X POST http://localhost:11000/table/register -d '{"table_name":"users", "shard_key":"id", "shard_func":"hash"}'
```

#### Add a Table to a Shard

```bash
curl -X POST http://localhost:11000/table/add-to-shard -d '{"table_name":"users", "shard_id":"shard1"}'
```

#### Rebalance Tables

```bash
curl -X POST http://localhost:11000/table/rebalance -d '{"table_name":"users"}'
```

#### Auto-Routing Queries

```bash
curl -G http://localhost:11000/query --data-urlencode "q=SELECT * FROM users WHERE id=123" -d "auto_route=true"
curl -X POST http://localhost:11000/execute -d '{"query":"INSERT INTO users (id, name, email) VALUES (?, ?, ?)", "args":["456", "Jane Doe", "jane@example.com"]}' -d "auto_route=true"
```

## Implementation Details

- The system uses the Hashicorp Raft implementation for consensus
- SQLite is used as the underlying database engine
- All write operations go through the Raft log to ensure consistency
- Read operations can be performed directly on any node
- The proxy layer manages table registrations and shard mappings
- Queries can be automatically routed to the appropriate shard based on table name and shard key
- Tables can be distributed across multiple shards for horizontal scaling

## Limitations

- The current implementation has a simplified snapshot and restore mechanism
- Large databases may experience performance issues during snapshot/restore
- The system assumes a reliable network environment

## Future Improvements

- Enhanced snapshot and restore mechanism
- Support for read-only replicas
- Better handling of network partitions
- Performance optimizations for large databases
- Authentication and authorization
- Advanced sharding strategies (range-based, geo-based, etc.)
- Connection pooling for shards
- Caching layer for frequently accessed data
- Shard migration and rebalancing tools
- Monitoring and metrics for shard performance

## License

This project is licensed under the MIT License - see the LICENSE file for details.
