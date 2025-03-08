package server

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM is a Raft finite state machine implementation for SQLite
type FSM struct {
	mu     sync.RWMutex
	db     *sql.DB // Default database connection
	server *Server // Reference to the server for accessing shards
	logger *log.Logger
	dbPath string
}

// Apply applies a Raft log entry to the SQLite database
func (f *FSM) Apply(log *raft.Log) interface{} {
	// Parse the command
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Printf("Failed to unmarshal command: %s", err)
		return err
	}

	// Get the appropriate database connection based on shard ID
	var db *sql.DB
	var shardID string

	// Check if we need to auto-route this command
	if f.server != nil && f.server.proxy != nil && cmd.ShardID == "" {
		// Try to extract table name from query for auto-routing
		tableName, _, parseErr := parseSQLQuery(cmd.Query)
		if parseErr == nil && tableName != "" {
			// Convert string args to interface{}
			args := make([]interface{}, len(cmd.Args))
			for i, arg := range cmd.Args {
				args[i] = arg
			}

			// Try to route based on query
			routed, routeErr := f.server.proxy.RouteQuery(cmd.Query, args)
			if routeErr == nil {
				shardID = routed
				f.logger.Printf("Auto-routed command to shard %s", shardID)
			} else {
				f.logger.Printf("Auto-routing failed: %s", routeErr)
				
				// Try to get a default shard for this table
				mapping, exists := f.server.proxy.shardMappings[tableName]
				if exists && len(mapping.Shards) > 0 {
					shardID = mapping.Shards[0]
					f.logger.Printf("Using first shard %s for table %s", shardID, tableName)
				}
			}
		}
	} else if cmd.ShardID != "" {
		// Use the explicitly specified shard ID
		shardID = cmd.ShardID
	}

	// Get the database connection for the determined shard ID
	if f.server != nil && shardID != "" {
		// Try to get the shard
		shard, err := f.server.GetShard(shardID)
		if err == nil && shard != nil {
			db = shard.DB
		} else {
			f.logger.Printf("Failed to get shard %s, using default DB: %s", shardID, err)
			db = f.db
		}
	} else {
		// Use default DB if no shard specified or determined
		db = f.db
	}

	// Execute the query
	f.mu.Lock()
	defer f.mu.Unlock()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		f.logger.Printf("Failed to begin transaction: %s", err)
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Convert string args to interface{} args
	args := make([]interface{}, len(cmd.Args))
	for i, arg := range cmd.Args {
		args[i] = arg
	}

	// Execute the query
	_, err = tx.Exec(cmd.Query, args...)
	if err != nil {
		f.logger.Printf("Failed to execute query: %s", err)
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		f.logger.Printf("Failed to commit transaction: %s", err)
		return err
	}

	return nil
}

// Snapshot returns a snapshot of the SQLite database
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// In a sharded environment, we need to create snapshots for all shards
	// For simplicity, we'll just snapshot the default database for now
	// In a production system, you would need to handle snapshots for all shards
	var db *sql.DB
	if f.server != nil && len(f.server.shards) > 0 {
		// Get the first shard for snapshot
		for _, shard := range f.server.shards {
			db = shard.DB
			break
		}
	} else {
		db = f.db
	}

	// Create a read-only transaction
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Create a snapshot
	return &Snapshot{tx: tx}, nil
}

// Restore restores the SQLite database from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// In a sharded environment, we need to restore all shards
	// For simplicity, we'll just restore the default database for now
	if f.server != nil && len(f.server.shards) > 0 {
		// Close all shard connections
		for _, shard := range f.server.shards {
			if shard.DB != nil {
				if err := shard.DB.Close(); err != nil {
					f.logger.Printf("Error closing shard %s: %s", shard.ID, err)
				}
			}

			// Reopen the shard database
			db, err := sql.Open("sqlite3", shard.Path)
			if err != nil {
				f.logger.Printf("Error reopening shard %s: %s", shard.ID, err)
				continue
			}
			shard.DB = db
		}
	} else {
		// Close the existing database
		if err := f.db.Close(); err != nil {
			return err
		}

		// Reopen the database with the same connection string
		db, err := sql.Open("sqlite3", f.dbPath)
		if err != nil {
			return err
		}
		f.db = db
	}

	// Read the snapshot data and restore the database
	// This is a simplified implementation; in a real system,
	// you would need to handle the actual restoration of the database
	// from the snapshot data
	return nil
}

// Snapshot is a Raft FSM snapshot implementation for SQLite
type Snapshot struct {
	tx *sql.Tx
}

// Persist persists the snapshot to the given sink
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	// In a real implementation, you would dump the database to the sink
	// This is a simplified implementation
	_, err := sink.Write([]byte("snapshot"))
	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release releases resources associated with the snapshot
func (s *Snapshot) Release() {
	s.tx.Rollback()
}
