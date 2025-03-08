package server

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// MetaStore handles persistence of metadata for the proxy layer
type MetaStore struct {
	mu       sync.RWMutex
	metaPath string
	logger   *log.Logger
}

// MetaData represents the persistent metadata for the proxy layer
type MetaData struct {
	Tables        map[string]TableInfo    `json:"tables"`
	ShardMappings map[string]ShardMapping `json:"shard_mappings"`
}

// NewMetaStore creates a new metadata store
func NewMetaStore(dataDir string, logger *log.Logger) (*MetaStore, error) {
	// Create the metadata directory if it doesn't exist
	metaDir := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create meta directory: %w", err)
	}

	metaPath := filepath.Join(metaDir, "proxy_meta.json")
	
	return &MetaStore{
		metaPath: metaPath,
		logger:   logger,
	}, nil
}

// LoadMetaData loads metadata from disk
func (ms *MetaStore) LoadMetaData() (*MetaData, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Check if the metadata file exists
	if _, err := os.Stat(ms.metaPath); os.IsNotExist(err) {
		// Return empty metadata if file doesn't exist
		return &MetaData{
			Tables:        make(map[string]TableInfo),
			ShardMappings: make(map[string]ShardMapping),
		}, nil
	}

	// Read the metadata file
	data, err := os.ReadFile(ms.metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Parse the metadata
	var meta MetaData
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Initialize maps if they're nil
	if meta.Tables == nil {
		meta.Tables = make(map[string]TableInfo)
	}
	if meta.ShardMappings == nil {
		meta.ShardMappings = make(map[string]ShardMapping)
	}

	return &meta, nil
}

// SaveMetaData saves metadata to disk
func (ms *MetaStore) SaveMetaData(meta *MetaData) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Marshal the metadata to JSON
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write to a temporary file first
	tempPath := ms.metaPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary metadata file: %w", err)
	}

	// Rename the temporary file to the actual metadata file
	// This ensures atomic updates
	if err := os.Rename(tempPath, ms.metaPath); err != nil {
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	return nil
}

// RegisterTable adds or updates a table in the metadata
func (ms *MetaStore) RegisterTable(meta *MetaData, tableName, shardKey, shardFunc string) {
	meta.Tables[tableName] = TableInfo{
		Name:      tableName,
		ShardKey:  shardKey,
		ShardFunc: shardFunc,
	}
}

// AddTableToShard adds a table to a shard in the metadata
func (ms *MetaStore) AddTableToShard(meta *MetaData, tableName, shardID string) error {
	// Check if the table exists
	if _, exists := meta.Tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Get or create the shard mapping
	mapping, exists := meta.ShardMappings[tableName]
	if !exists {
		mapping = ShardMapping{
			TableName: tableName,
			Shards:    []string{},
		}
	}

	// Check if the shard is already in the mapping
	for _, s := range mapping.Shards {
		if s == shardID {
			return nil // Shard already exists, nothing to do
		}
	}

	// Add the shard to the mapping
	mapping.Shards = append(mapping.Shards, shardID)
	meta.ShardMappings[tableName] = mapping

	return nil
}

// RemoveTableFromShard removes a table from a shard in the metadata
func (ms *MetaStore) RemoveTableFromShard(meta *MetaData, tableName, shardID string) error {
	// Check if the table exists
	if _, exists := meta.Tables[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Check if the shard mapping exists
	mapping, exists := meta.ShardMappings[tableName]
	if !exists {
		return nil // No mapping exists, nothing to do
	}

	// Remove the shard from the mapping
	newShards := []string{}
	for _, s := range mapping.Shards {
		if s != shardID {
			newShards = append(newShards, s)
		}
	}

	// Update the mapping
	mapping.Shards = newShards
	meta.ShardMappings[tableName] = mapping

	return nil
}
