package server

import (
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// TableInfo stores metadata about a table
type TableInfo struct {
	Name      string
	ShardKey  string
	ShardFunc string // "hash", "range", "mod", etc.
}

// ShardMapping maps a table to its shards
type ShardMapping struct {
	TableName string
	Shards    []string // List of shard IDs
}

// RebalanceAlgorithm defines the interface for rebalance algorithms
type RebalanceAlgorithm interface {
	// Name returns the name of the algorithm
	Name() string
	
	// Rebalance redistributes data across shards
	// It takes the table name, current shard mapping, and table info
	// Returns the new shard mapping after rebalancing
	Rebalance(tableName string, mapping ShardMapping, tableInfo TableInfo, server *Server) (ShardMapping, error)
}

// ProxyLayer manages table distribution across shards
type ProxyLayer struct {
	mu                 sync.RWMutex
	server             *Server
	tables             map[string]TableInfo
	shardMappings      map[string]ShardMapping
	logger             *log.Logger
	metaStore          *MetaStore
	rebalanceAlgs      map[string]RebalanceAlgorithm
	defaultRebalanceAlg string
}

// NewProxyLayer creates a new proxy layer
func NewProxyLayer(server *Server, logger *log.Logger) *ProxyLayer {
	p := &ProxyLayer{
		server:             server,
		tables:             make(map[string]TableInfo),
		shardMappings:      make(map[string]ShardMapping),
		logger:             logger,
		rebalanceAlgs:      make(map[string]RebalanceAlgorithm),
		defaultRebalanceAlg: "round-robin",
	}
	
	// Register default rebalance algorithms
	p.RegisterRebalanceAlgorithm(&RoundRobinRebalance{})
	p.RegisterRebalanceAlgorithm(&HashBasedRebalance{})

	// Initialize the meta store if the server has a data directory
	if server != nil && server.dbPath != "" {
		// Use the parent directory of dbPath as our data directory
		dataDir := filepath.Dir(server.dbPath)
		metaStore, err := NewMetaStore(dataDir, logger)
		if err != nil {
			logger.Printf("Failed to initialize meta store: %s", err)
		} else {
			p.metaStore = metaStore
			// Load metadata from disk
			if err := p.loadMetaData(); err != nil {
				logger.Printf("Failed to load metadata: %s", err)
			}
		}
	}

	return p
}

// loadMetaData loads metadata from disk
func (p *ProxyLayer) loadMetaData() error {
	// Skip if meta store is not initialized
	if p.metaStore == nil {
		return nil
	}

	// Load metadata from disk
	meta, err := p.metaStore.LoadMetaData()
	if err != nil {
		return err
	}

	// Update in-memory tables and shard mappings
	p.mu.Lock()
	defer p.mu.Unlock()

	// Copy tables from metadata
	for name, info := range meta.Tables {
		p.tables[name] = info
	}

	// Copy shard mappings from metadata
	for name, mapping := range meta.ShardMappings {
		p.shardMappings[name] = mapping
	}

	return nil
}

// saveMetaData saves the current state to disk
func (p *ProxyLayer) saveMetaData() error {
	// Skip if meta store is not initialized
	if p.metaStore == nil {
		return nil
	}

	// Create a copy of the current state
	p.mu.RLock()
	meta := &MetaData{
		Tables:        make(map[string]TableInfo),
		ShardMappings: make(map[string]ShardMapping),
	}

	// Copy tables to metadata
	for name, info := range p.tables {
		meta.Tables[name] = info
	}

	// Copy shard mappings to metadata
	for name, mapping := range p.shardMappings {
		meta.ShardMappings[name] = mapping
	}
	p.mu.RUnlock()

	// Save metadata to disk
	return p.metaStore.SaveMetaData(meta)
}

// RegisterTable registers a table with the proxy layer
func (p *ProxyLayer) RegisterTable(tableName, shardKey, shardFunc string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if table already exists
	if _, exists := p.tables[tableName]; exists {
		return fmt.Errorf("table %s already registered", tableName)
	}

	// Register the table
	p.tables[tableName] = TableInfo{
		Name:      tableName,
		ShardKey:  shardKey,
		ShardFunc: shardFunc,
	}

	// Initialize shard mapping
	p.shardMappings[tableName] = ShardMapping{
		TableName: tableName,
		Shards:    []string{},
	}

	// Save changes to disk
	go p.saveMetaData()

	return nil
}

// AddTableToShard adds a table to a shard
func (p *ProxyLayer) AddTableToShard(tableName, shardID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if table exists
	if _, exists := p.tables[tableName]; !exists {
		return fmt.Errorf("table %s not registered", tableName)
	}

	// Check if shard exists
	_, err := p.server.GetShard(shardID)
	if err != nil {
		return fmt.Errorf("shard %s not found: %s", shardID, err)
	}

	// Update shard mapping
	mapping := p.shardMappings[tableName]
	for _, s := range mapping.Shards {
		if s == shardID {
			return nil // Shard already added
		}
	}
	mapping.Shards = append(mapping.Shards, shardID)
	p.shardMappings[tableName] = mapping

	// Save changes to disk
	go p.saveMetaData()

	return nil
}

// GetShardForKey determines which shard to use for a given key
func (p *ProxyLayer) GetShardForKey(tableName string, key interface{}) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Check if table exists
	tableInfo, exists := p.tables[tableName]
	if !exists {
		return "", fmt.Errorf("table %s not registered", tableName)
	}

	// Get shard mapping
	mapping, exists := p.shardMappings[tableName]
	if !exists || len(mapping.Shards) == 0 {
		return "", fmt.Errorf("no shards available for table %s", tableName)
	}

	// Determine shard based on sharding function
	var shardIndex int
	switch tableInfo.ShardFunc {
	case "hash":
		// Simple hash-based sharding
		h := fnv.New32a()
		h.Write([]byte(fmt.Sprintf("%v", key)))
		shardIndex = int(h.Sum32()) % len(mapping.Shards)
	case "mod":
		// Modulo-based sharding for numeric keys
		keyInt, ok := key.(int)
		if !ok {
			return "", fmt.Errorf("mod sharding requires numeric key")
		}
		shardIndex = keyInt % len(mapping.Shards)
	default:
		// Default to hash-based sharding
		h := fnv.New32a()
		h.Write([]byte(fmt.Sprintf("%v", key)))
		shardIndex = int(h.Sum32()) % len(mapping.Shards)
	}

	return mapping.Shards[shardIndex], nil
}

// RouteQuery routes a SQL query to the appropriate shard
func (p *ProxyLayer) RouteQuery(query string, args []interface{}) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Parse the query to extract table name and where clause
	tableName, whereClause, err := parseSQLQuery(query)
	if err != nil {
		return "", err
	}

	// Check if table exists
	tableInfo, exists := p.tables[tableName]
	if !exists {
		return "", fmt.Errorf("table %s not registered", tableName)
	}

	// Extract key value from where clause
	keyValue, err := extractKeyFromWhereClause(whereClause, tableInfo.ShardKey)
	if err != nil {
		// If we can't extract a specific key, use a default shard
		mapping, exists := p.shardMappings[tableName]
		if !exists || len(mapping.Shards) == 0 {
			return "", fmt.Errorf("no shards available for table %s", tableName)
		}
		// Use the first shard as default
		return mapping.Shards[0], nil
	}

	// Get shard for the key
	return p.GetShardForKey(tableName, keyValue)
}

// ExecuteQuery executes a query on the appropriate shard
func (p *ProxyLayer) ExecuteQuery(query string, args []interface{}) (interface{}, error) {
	// Route the query to determine the shard
	shardID, err := p.RouteQuery(query, args)
	if err != nil {
		return nil, err
	}

	// Get the shard
	shard, err := p.server.GetShard(shardID)
	if err != nil {
		return nil, err
	}

	// Execute the query on the shard
	if isSelectQuery(query) {
		// For SELECT queries, return rows
		rows, err := shard.DB.Query(query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		// Process the rows
		return processRows(rows)
	} else {
		// For non-SELECT queries, execute and return result
		result, err := shard.DB.Exec(query, args...)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
}

// RebalanceShard redistributes data across shards
func (p *ProxyLayer) RebalanceShard(tableName string) error {
	return p.RebalanceShardWithAlgorithm(tableName, p.defaultRebalanceAlg)
}

// RebalanceShardWithAlgorithm redistributes data across shards using the specified algorithm
func (p *ProxyLayer) RebalanceShardWithAlgorithm(tableName, algorithmName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if table exists
	tableInfo, exists := p.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not registered", tableName)
	}

	// Get shard mapping
	mapping, exists := p.shardMappings[tableName]
	if !exists || len(mapping.Shards) < 2 {
		return fmt.Errorf("not enough shards for rebalancing table %s", tableName)
	}

	// Get the rebalance algorithm
	alg, exists := p.rebalanceAlgs[algorithmName]
	if !exists {
		return fmt.Errorf("rebalance algorithm %s not found", algorithmName)
	}

	p.logger.Printf("Rebalancing table %s across shards %v using algorithm %s", tableName, mapping.Shards, alg.Name())

	// Apply the rebalance algorithm
	newMapping, err := alg.Rebalance(tableName, mapping, tableInfo, p.server)
	if err != nil {
		return fmt.Errorf("failed to rebalance table %s: %w", tableName, err)
	}

	// Update the shard mapping
	p.shardMappings[tableName] = newMapping
	
	// Save changes to disk
	go p.saveMetaData()

	return nil
}

// RemoveTable removes a table from the proxy layer
func (p *ProxyLayer) RemoveTable(tableName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if table exists
	_, exists := p.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not registered", tableName)
	}

	// Remove table from registry
	delete(p.tables, tableName)

	// Remove shard mapping
	delete(p.shardMappings, tableName)

	// Save changes to disk
	go p.saveMetaData()

	return nil
}

// RegisterRebalanceAlgorithm registers a rebalance algorithm
func (p *ProxyLayer) RegisterRebalanceAlgorithm(alg RebalanceAlgorithm) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.rebalanceAlgs[alg.Name()] = alg
	p.logger.Printf("Registered rebalance algorithm: %s", alg.Name())
}

// SetDefaultRebalanceAlgorithm sets the default rebalance algorithm
func (p *ProxyLayer) SetDefaultRebalanceAlgorithm(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if the algorithm exists
	_, exists := p.rebalanceAlgs[name]
	if !exists {
		return fmt.Errorf("rebalance algorithm %s not found", name)
	}

	p.defaultRebalanceAlg = name
	return nil
}

// GetAvailableRebalanceAlgorithms returns a list of available rebalance algorithms
func (p *ProxyLayer) GetAvailableRebalanceAlgorithms() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	algs := make([]string, 0, len(p.rebalanceAlgs))
	for name := range p.rebalanceAlgs {
		algs = append(algs, name)
	}

	return algs
}

// GetDefaultRebalanceAlgorithm returns the name of the default rebalance algorithm
func (p *ProxyLayer) GetDefaultRebalanceAlgorithm() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.defaultRebalanceAlg
}

// AutoRebalance periodically checks and rebalances shards
func (p *ProxyLayer) AutoRebalance() {
	p.mu.RLock()
	tables := make([]string, 0, len(p.tables))
	for tableName := range p.tables {
		tables = append(tables, tableName)
	}
	p.mu.RUnlock()

	// Rebalance each table
	for _, tableName := range tables {
		if err := p.RebalanceShard(tableName); err != nil {
			p.logger.Printf("Error rebalancing table %s: %s", tableName, err)
		}
	}
}

// Helper functions

// parseQuery extracts table name and where clause from a SQL query
func parseQuery(query string) (string, string, error) {
	// This is a simplified parser; a real implementation would use a proper SQL parser
	query = strings.ToLower(query)

	// Extract table name
	fromRegex := regexp.MustCompile(`from\s+([a-zA-Z0-9_]+)`)
	matches := fromRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		return "", "", errors.New("could not extract table name from query")
	}
	tableName := matches[1]

	// Extract where clause
	whereRegex := regexp.MustCompile(`where\s+(.+)`)
	matches = whereRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		return tableName, "", nil // No where clause
	}
	whereClause := matches[1]

	return tableName, whereClause, nil
}

// extractKeyFromWhereClause extracts the value of a specific key from a where clause
func extractKeyFromWhereClause(whereClause, keyName string) (interface{}, error) {
	if whereClause == "" {
		return nil, errors.New("no where clause provided")
	}

	// Look for patterns like "key_name = value" or "key_name IN (value)"
	keyRegex := regexp.MustCompile(fmt.Sprintf(`%s\s*=\s*([0-9]+|'[^']*')`, keyName))
	matches := keyRegex.FindStringSubmatch(whereClause)
	if len(matches) < 2 {
		return nil, fmt.Errorf("could not extract key %s from where clause", keyName)
	}

	keyValue := matches[1]
	// Remove quotes for string values
	if strings.HasPrefix(keyValue, "'") && strings.HasSuffix(keyValue, "'") {
		keyValue = keyValue[1 : len(keyValue)-1]
	}

	// Try to convert to integer if possible
	var intValue int
	if _, err := fmt.Sscanf(keyValue, "%d", &intValue); err == nil {
		return intValue, nil
	}

	// Return as string otherwise
	return keyValue, nil
}

// isSelectQuery determines if a query is a SELECT query
func isSelectQuery(query string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), "select")
}

// processRows processes SQL rows into a slice of maps
func processRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Prepare result
	var result []map[string]interface{}

	// Prepare values for each row
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Iterate through rows
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Create a map for this row
		entry := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				entry[col] = string(b)
			} else {
				entry[col] = val
			}
		}

		result = append(result, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// RoundRobinRebalance implements a round-robin rebalancing algorithm
type RoundRobinRebalance struct{}

// Name returns the name of the algorithm
func (r *RoundRobinRebalance) Name() string {
	return "round-robin"
}

// Rebalance redistributes data across shards using a round-robin approach
func (r *RoundRobinRebalance) Rebalance(tableName string, mapping ShardMapping, tableInfo TableInfo, server *Server) (ShardMapping, error) {
	// In a real implementation, this would:
	// 1. Analyze the current distribution of data
	// 2. Move data to achieve an even distribution
	
	// For this example, we'll just return the existing mapping
	// In a real implementation, you would modify the mapping based on data distribution
	return mapping, nil
}

// HashBasedRebalance implements a hash-based rebalancing algorithm
type HashBasedRebalance struct{}

// Name returns the name of the algorithm
func (h *HashBasedRebalance) Name() string {
	return "hash-based"
}

// Rebalance redistributes data across shards using a hash-based approach
func (h *HashBasedRebalance) Rebalance(tableName string, mapping ShardMapping, tableInfo TableInfo, server *Server) (ShardMapping, error) {
	// In a real implementation, this would:
	// 1. Calculate hash ranges for each shard
	// 2. Move data to achieve an even distribution based on hash values
	
	// For this example, we'll just return the existing mapping
	// In a real implementation, you would modify the mapping based on hash distribution
	return mapping, nil
}
