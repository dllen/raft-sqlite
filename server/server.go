package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	applyTimeout        = 10 * time.Second
)

// Command represents a SQL statement to be executed on the SQLite database
type Command struct {
	Query   string   `json:"query"`
	Args    []string `json:"args,omitempty"`
	Results []string `json:"results,omitempty"`
	ShardID string   `json:"shard_id,omitempty"`
}

// Shard represents a database shard
// Each shard has its own SQLite database connection
// This structure can be expanded to include additional shard-specific configurations
// such as connection pooling, caching, etc.
type Shard struct {
	ID   string
	DB   *sql.DB
	Path string
}

// NewShard creates a new shard with the given ID and database path
func NewShard(id, dbPath string) (*Shard, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database for shard %s: %s", id, err)
	}
	return &Shard{ID: id, DB: db, Path: dbPath}, nil
}

// Server represents a Raft node with multiple SQLite database shards
type Server struct {
	mu sync.RWMutex

	nodeID   string
	raftAddr string
	httpAddr string
	dbPath   string

	raft   *raft.Raft
	shards map[string]*Shard
	fsm    *FSM
	proxy  *ProxyLayer
	logger *log.Logger
}

// New creates a new server with shard support
func New(nodeID, dbPath, raftAddr, httpAddr string) *Server {
	s := &Server{
		nodeID:   nodeID,
		raftAddr: raftAddr,
		httpAddr: httpAddr,
		dbPath:   dbPath,
		shards:   make(map[string]*Shard),
		logger:   log.New(os.Stderr, "[server] ", log.LstdFlags),
	}
	
	// Initialize the proxy layer
	s.proxy = NewProxyLayer(s, log.New(os.Stderr, "[proxy] ", log.LstdFlags))
	
	return s
}

// AddShard adds a new shard to the server
func (s *Server) AddShard(shardID, dbPath string) error {
	shard, err := NewShard(shardID, dbPath)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shards[shardID] = shard
	return nil
}

// GetShard retrieves a shard by its ID
func (s *Server) GetShard(shardID string) (*Shard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	shard, exists := s.shards[shardID]
	if !exists {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}
	return shard, nil
}

// Start initializes and starts the server
func (s *Server) Start() error {
	// Initialize SQLite database for each shard
	for shardID, shard := range s.shards {
		db, err := sql.Open("sqlite3", shard.Path)
		if err != nil {
			return fmt.Errorf("failed to open SQLite database for shard %s: %s", shardID, err)
		}
		shard.DB = db
	}

	// Create Raft FSM
	s.fsm = &FSM{
		db:     s.shards["default"].DB, // Use the default shard's DB for FSM
		server: s, // Add reference to server for shard access
		logger: log.New(os.Stderr, "[fsm] ", log.LstdFlags),
		dbPath: s.dbPath,
	}

	// Setup Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.nodeID)

	// Setup Raft communication
	// Parse the Raft address into host and port
	host, port, err := net.SplitHostPort(s.raftAddr)
	if err != nil {
		return fmt.Errorf("failed to parse Raft address: %s", err)
	}

	// If the host is empty (e.g., ":12000"), use "localhost" for advertising
	if host == "" {
		host = "localhost"
	}

	// Create the bind address (what we listen on)
	bindAddr := fmt.Sprintf("%s:%s", host, port)
	// No need to resolve the bind address as a TCP address since we're not using it
	// Just use the string form directly in the transport creation

	// Create the advertise address (what other nodes connect to)
	advertiseAddr := fmt.Sprintf("%s:%s", host, port)
	advertiseTCPAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve advertise address: %s", err)
	}

	// Create the transport with both addresses
	transport, err := raft.NewTCPTransport(bindAddr, advertiseTCPAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create Raft transport: %s", err)
	}

	// Create the snapshot store
	snapshots, err := raft.NewFileSnapshotStore(
		filepath.Dir(s.dbPath),
		retainSnapshotCount,
		os.Stderr,
	)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %s", err)
	}

	// Create the log store and stable store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(filepath.Dir(s.dbPath), "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create log store: %s", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(filepath.Dir(s.dbPath), "raft-stable.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create stable store: %s", err)
	}

	// Instantiate the Raft system
	r, err := raft.NewRaft(config, s.fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create Raft instance: %s", err)
	}
	s.raft = r

	// Bootstrap the cluster if this is the first node
	if s.raft.State() != raft.Leader {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		s.raft.BootstrapCluster(configuration)
	}

	return nil
}

// Join joins an existing Raft cluster
func (s *Server) Join(leaderAddr string) error {
	// Construct URL for join request
	url := fmt.Sprintf("http://%s/join", leaderAddr)

	// Create join request
	data := map[string]string{
		"node_id":   s.nodeID,
		"raft_addr": s.raftAddr,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Send join request
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to join cluster: %s", string(body))
	}

	return nil
}

// Handler returns an HTTP handler for the server
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Handle join requests
	mux.HandleFunc("/join", s.handleJoin)

	// Handle SQL queries
	mux.HandleFunc("/execute", s.handleExecute)
	mux.HandleFunc("/query", s.handleQuery)

	// Handle table management
	mux.HandleFunc("/table/register", s.handleRegisterTable)
	mux.HandleFunc("/table/remove", s.handleRemoveTable)
	mux.HandleFunc("/table/add-to-shard", s.handleAddTableToShard)
	mux.HandleFunc("/table/rebalance", s.handleRebalanceTable)

	return mux
}

// handleJoin handles requests to join the cluster
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Parse join request
	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Failed to parse join request: %s", err)
		return
	}

	// Add the node to the cluster
	if s.raft == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "Raft not initialized")
		return
	}

	if s.raft.State() != raft.Leader {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "Not the leader")
		return
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to get Raft configuration: %s", err)
		return
	}

	// Check if node already exists
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(req.NodeID) || srv.Address == raft.ServerAddress(req.RaftAddr) {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	// Add the new node
	future := s.raft.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 0)
	if err := future.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to add voter: %s", err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleExecute handles SQL execution requests
func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Parse the SQL query
	var cmd Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Failed to parse SQL command: %s", err)
		return
	}

	// Check if auto-routing is requested
	autoRoute := r.URL.Query().Get("auto_route") == "true"
	
	var shardID string
	var shard *Shard
	var err error
	
	if autoRoute {
		// Extract table name from query for routing
		tableName, _, parseErr := parseQuery(cmd.Query)
		if parseErr != nil {
			http.Error(w, fmt.Sprintf("Failed to parse query for auto-routing: %s", parseErr), http.StatusBadRequest)
			return
		}
		
		// Convert string args to interface{}
		args := make([]interface{}, len(cmd.Args))
		for i, arg := range cmd.Args {
			args[i] = arg
		}
		
		// Try to route based on query
		shardID, err = s.proxy.RouteQuery(cmd.Query, args)
		if err != nil {
			// If routing fails, check if we have any shards for this table
			s.mu.RLock()
			mapping, exists := s.proxy.shardMappings[tableName]
			s.mu.RUnlock()
			
			if !exists || len(mapping.Shards) == 0 {
				http.Error(w, fmt.Sprintf("No shards available for table %s", tableName), http.StatusBadRequest)
				return
			}
			
			// Use the first shard as fallback
			shardID = mapping.Shards[0]
			s.logger.Printf("Auto-routing failed, using first shard %s: %s", shardID, err)
		}
		
		shard, err = s.GetShard(shardID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		
		s.logger.Printf("Auto-routed execution to shard %s", shardID)
	} else {
		// Manual routing - shard ID is passed as a query parameter
		shardID = r.URL.Query().Get("shard_id")
		if shardID == "" {
			http.Error(w, "shard_id is required when auto_route is not enabled", http.StatusBadRequest)
			return
		}

		shard, err = s.GetShard(shardID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
	}

	// Log the shard being used for this operation
	s.logger.Printf("Using shard %s for execute operation", shardID)

	// Check if Raft is initialized
	if s.raft == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "Raft not initialized")
		return
	}

	// Check if this node is the leader
	if s.raft.State() != raft.Leader {
		// Forward to leader if known
		leader := s.raft.Leader()
		if leader == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "No leader available")
			return
		}

		// Construct URL for forwarding, including the shard ID
		leaderURL := fmt.Sprintf("http://%s/execute?shard_id=%s", leader, shardID)
		cmdJSON, err := json.Marshal(cmd)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to marshal command: %s", err)
			return
		}
		resp, err := http.Post(leaderURL, "application/json", bytes.NewBuffer(cmdJSON))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to forward to leader: %s", err)
			return
		}
		defer resp.Body.Close()

		// Copy the response from the leader
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Set the shard ID in the command to ensure it's applied to the correct shard
	cmd.ShardID = shardID

	// Include shard information in logs for debugging
	s.logger.Printf("Executing command on shard %s with connection %v", shardID, shard.DB)

	// Apply the command to the Raft log
	cmdJSON, err := json.Marshal(cmd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to marshal command: %s", err)
		return
	}

	applyFuture := s.raft.Apply(cmdJSON, applyTimeout)
	if err := applyFuture.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to apply command: %s", err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleQuery handles read-only SQL queries
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Parse the SQL query from URL parameters
	query := r.URL.Query().Get("q")
	if query == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Missing query parameter 'q'")
		return
	}

	// Check if auto-routing is requested
	autoRoute := r.URL.Query().Get("auto_route") == "true"
	
	var shard *Shard
	var err error
	var shardID string
	
	if autoRoute {
		// Use the proxy layer to route the query
		tableName, _, parseErr := parseSQLQuery(query)
		if parseErr != nil {
			http.Error(w, fmt.Sprintf("Failed to parse query for auto-routing: %s", parseErr), http.StatusBadRequest)
			return
		}
		
		// Get the first shard for this table as a fallback
		s.mu.RLock()
		mapping, exists := s.proxy.shardMappings[tableName]
		s.mu.RUnlock()
		
		if !exists || len(mapping.Shards) == 0 {
			http.Error(w, fmt.Sprintf("No shards available for table %s", tableName), http.StatusBadRequest)
			return
		}
		
		// Try to route based on query
		shardID, routeErr := s.proxy.RouteQuery(query, nil)
		if routeErr != nil {
			// If routing fails, use the first shard
			shardID = mapping.Shards[0]
			s.logger.Printf("Auto-routing failed, using first shard %s: %s", shardID, routeErr)
		}
		
		shard, err = s.GetShard(shardID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		
		s.logger.Printf("Auto-routed query to shard %s", shardID)
	} else {
		// Manual routing - shard ID is passed as a query parameter
		shardID = r.URL.Query().Get("shard_id")
		if shardID == "" {
			http.Error(w, "shard_id is required when auto_route is not enabled", http.StatusBadRequest)
			return
		}

		shard, err = s.GetShard(shardID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
	}

	// Execute the query directly on the SQLite database
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Use the shard's database connection to execute the query
	rows, err := shard.DB.Query(query)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to execute query: %s", err)
		return
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to get column names: %s", err)
		return
	}

	// Prepare result
	var result []map[string]interface{}

	// Scan rows
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the slice of interface{}
		if err := rows.Scan(valuePtrs...); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to scan row: %s", err)
			return
		}

		// Create a map for this row
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}

		result = append(result, rowMap)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error iterating rows: %s", err)
		return
	}

	// Return the result as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to encode result: %s", err)
		return
	}
}

// handleRegisterTable handles table registration requests
func (s *Server) handleRegisterTable(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the request body
	var tableInfo struct {
		TableName string `json:"table_name"`
		ShardKey  string `json:"shard_key"`
		ShardFunc string `json:"shard_func"`
	}

	if err := json.NewDecoder(r.Body).Decode(&tableInfo); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid request body: %s", err)
		return
	}

	// Validate request
	if tableInfo.TableName == "" {
		http.Error(w, "table_name is required", http.StatusBadRequest)
		return
	}
	if tableInfo.ShardKey == "" {
		http.Error(w, "shard_key is required", http.StatusBadRequest)
		return
	}
	if tableInfo.ShardFunc == "" {
		tableInfo.ShardFunc = "hash" // Default to hash-based sharding
	}

	// Register the table
	err := s.proxy.RegisterTable(tableInfo.TableName, tableInfo.ShardKey, tableInfo.ShardFunc)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to register table: %s", err)
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table %s registered successfully", tableInfo.TableName)
}

// handleAddTableToShard handles adding a table to a shard
func (s *Server) handleAddTableToShard(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the request body
	var request struct {
		TableName string `json:"table_name"`
		ShardID   string `json:"shard_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid request body: %s", err)
		return
	}

	// Validate request
	if request.TableName == "" {
		http.Error(w, "table_name is required", http.StatusBadRequest)
		return
	}
	if request.ShardID == "" {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}

	// Add table to shard
	err := s.proxy.AddTableToShard(request.TableName, request.ShardID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to add table to shard: %s", err)
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table %s added to shard %s successfully", request.TableName, request.ShardID)
}

// handleRemoveTable handles removing a table from the proxy layer
func (s *Server) handleRemoveTable(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the request body
	var request struct {
		TableName string `json:"table_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid request body: %s", err)
		return
	}

	// Validate request
	if request.TableName == "" {
		http.Error(w, "table_name is required", http.StatusBadRequest)
		return
	}

	// Remove the table
	err := s.proxy.RemoveTable(request.TableName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to remove table: %s", err)
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table %s removed successfully", request.TableName)
}

// handleRebalanceTable handles table rebalancing requests
func (s *Server) handleRebalanceTable(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the request body
	var request struct {
		TableName string `json:"table_name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid request body: %s", err)
		return
	}

	// Validate request
	if request.TableName == "" {
		http.Error(w, "table_name is required", http.StatusBadRequest)
		return
	}

	// Rebalance the table
	var err error
	if request.AlgorithmName != "" {
		// Use specified algorithm
		err = s.proxy.RebalanceShardWithAlgorithm(request.TableName, request.AlgorithmName)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to rebalance table: %s", err)
			return
		}
	} else {
		// Use default algorithm
		err = s.proxy.RebalanceShard(request.TableName)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Failed to rebalance table: %s", err)
			return
		}
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Table %s rebalanced successfully", request.TableName)
}
