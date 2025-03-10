package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

	// Create a snapshot with references to server and FSM
	return &Snapshot{
		server: f.server,
		fsm:    f,
	}, nil
}

// Restore restores the SQLite database from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Read the snapshot data
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read snapshot data: %s", err)
	}

	// Parse the snapshot data
	type ShardData struct {
		ID   string `json:"id"`
		Path string `json:"path"`
		Data []byte `json:"data"`
	}

	type SnapshotData struct {
		Shards        []ShardData              `json:"shards"`
		Tables        map[string]TableInfo     `json:"tables"`
		ShardMappings map[string]ShardMapping  `json:"shard_mappings"`
	}

	var snapshotData SnapshotData
	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("failed to parse snapshot data: %s", err)
	}

	// Close all existing shard connections
	if f.server != nil {
		for _, shard := range f.server.shards {
			if shard.DB != nil {
				if err := shard.DB.Close(); err != nil {
					f.logger.Printf("Error closing shard %s: %s", shard.ID, err)
				}
			}
		}
	} else if f.db != nil {
		// Close the existing database
		if err := f.db.Close(); err != nil {
			return fmt.Errorf("error closing main database: %s", err)
		}
	}

	// Restore shards
	if f.server != nil {
		// Clear existing shards
		f.server.mu.Lock()
		f.server.shards = make(map[string]*Shard)

		// Restore each shard
		for _, shardData := range snapshotData.Shards {
			// In a real implementation, you would restore the database from the data
			// For now, we'll just reopen the database at the same path
			shard, err := NewShard(shardData.ID, shardData.Path)
			if err != nil {
				f.logger.Printf("Error restoring shard %s: %s", shardData.ID, err)
				continue
			}
			f.server.shards[shardData.ID] = shard
		}
		f.server.mu.Unlock()

		// Restore tables and shard mappings
		if f.server.proxy != nil {
			// Clear existing tables and mappings
			f.server.proxy.mu.Lock()
			f.server.proxy.tables = make(map[string]TableInfo)
			f.server.proxy.shardMappings = make(map[string]ShardMapping)

			// Restore tables
			for name, info := range snapshotData.Tables {
				f.server.proxy.tables[name] = info
			}

			// Restore shard mappings
			for name, mapping := range snapshotData.ShardMappings {
				f.server.proxy.shardMappings[name] = mapping
			}
			f.server.proxy.mu.Unlock()
		}
	} else {
		// Reopen the main database
		db, err := sql.Open("sqlite3", f.dbPath)
		if err != nil {
			return fmt.Errorf("error reopening main database: %s", err)
		}
		f.db = db
	}

	f.logger.Printf("Restored %d shards, %d tables, and %d mappings from snapshot", 
		len(snapshotData.Shards), len(snapshotData.Tables), len(snapshotData.ShardMappings))
	return nil
}

// Snapshot is a Raft FSM snapshot implementation for SQLite
type Snapshot struct {
	server *Server
	fsm    *FSM
}

// Persist persists the snapshot to the given sink
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	// Create a snapshot of all shards and metadata
	if s.server == nil {
		return fmt.Errorf("server reference is nil")
	}

	// Create a snapshot structure to serialize
	type ShardData struct {
		ID   string `json:"id"`
		Path string `json:"path"`
		Data []byte `json:"data"`
	}

	type SnapshotData struct {
		Shards       []ShardData                  `json:"shards"`
		Tables       map[string]TableInfo         `json:"tables"`
		ShardMappings map[string]ShardMapping     `json:"shard_mappings"`
	}

	// Lock the server to ensure consistent state
	s.server.mu.RLock()
	defer s.server.mu.RUnlock()

	// Prepare snapshot data
	snapshotData := SnapshotData{
		Shards:        make([]ShardData, 0, len(s.server.shards)),
		Tables:        s.server.proxy.GetTableInfo(),
		ShardMappings: s.server.proxy.GetShardMappings(),
	}

	// Add each shard's data
	for id, shard := range s.server.shards {
		// For each shard, we need to dump its database
		// In a real implementation, you would use sqlite3's backup API
		// For simplicity, we'll just include the path and ID
		shardData := ShardData{
			ID:   id,
			Path: shard.Path,
			// In a real implementation, you would include the actual database content
			Data: []byte{}, // Placeholder for actual database dump
		}
		snapshotData.Shards = append(snapshotData.Shards, shardData)
	}

	// Serialize the snapshot data
	data, err := json.Marshal(snapshotData)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to serialize snapshot: %s", err)
	}

	// Write the serialized data to the sink
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %s", err)
	}

	return nil
}

// Release releases resources associated with the snapshot
func (s *Snapshot) Release() {
	// No resources to release in our implementation
}
