package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// TestProxyLayer tests the proxy layer functionality
func TestProxyLayer(t *testing.T) {
	// Create a server for testing
	s, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Initialize the proxy layer
	logger := log.New(os.Stderr, "[proxy-test] ", log.LstdFlags)
	proxy := NewProxyLayer(s, logger)

	// Test registering a table
	err := proxy.RegisterTable("users", "user_id", "hash")
	if err != nil {
		t.Fatalf("Failed to register table: %s", err)
	}

	// Verify the table was registered
	if _, exists := proxy.tables["users"]; !exists {
		t.Error("Expected table 'users' to be registered")
	}

	// Test adding a table to a shard
	err = proxy.AddTableToShard("users", "default")
	if err != nil {
		t.Fatalf("Failed to add table to shard: %s", err)
	}

	// Verify the table was added to the shard
	mapping, exists := proxy.shardMappings["users"]
	if !exists {
		t.Error("Expected shard mapping for 'users' to exist")
	}
	if len(mapping.Shards) != 1 || mapping.Shards[0] != "default" {
		t.Errorf("Expected 'users' to be mapped to 'default' shard, got %v", mapping.Shards)
	}

	// Test getting shard for key
	shardID, err := proxy.GetShardForKey("users", 123)
	if err != nil {
		t.Fatalf("Failed to get shard for key: %s", err)
	}
	if shardID != "default" {
		t.Errorf("Expected shard ID to be 'default', got '%s'", shardID)
	}

	// Test routing a query
	query := "SELECT * FROM users WHERE user_id = 123"
	shardID, err = proxy.RouteQuery(query, nil)
	if err != nil {
		t.Fatalf("Failed to route query: %s", err)
	}
	if shardID != "default" {
		t.Errorf("Expected query to be routed to 'default' shard, got '%s'", shardID)
	}
}

// TestRegisterTableHandler tests the table registration handler
func TestRegisterTableHandler(t *testing.T) {
	// Create a server for testing
	s, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a test HTTP request
	reqBody := map[string]string{
		"table_name": "users",
		"shard_key":  "user_id",
		"shard_func": "hash",
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("Failed to marshal request body: %s", err)
	}

	req := httptest.NewRequest("POST", "/table/register", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Call the handler
	s.handleRegisterTable(w, req)

	// Check the response
	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Verify the table was registered
	if _, exists := s.proxy.tables["users"]; !exists {
		t.Error("Expected table 'users' to be registered")
	}
}

// TestAddTableToShardHandler tests the add table to shard handler
func TestAddTableToShardHandler(t *testing.T) {
	// Create a server for testing
	s, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Register a table first
	err := s.proxy.RegisterTable("users", "user_id", "hash")
	if err != nil {
		t.Fatalf("Failed to register table: %s", err)
	}

	// Create a test HTTP request
	reqBody := map[string]string{
		"table_name": "users",
		"shard_id":   "default",
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("Failed to marshal request body: %s", err)
	}

	req := httptest.NewRequest("POST", "/table/add-to-shard", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Call the handler
	s.handleAddTableToShard(w, req)

	// Check the response
	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Verify the table was added to the shard
	mapping, exists := s.proxy.shardMappings["users"]
	if !exists {
		t.Error("Expected shard mapping for 'users' to exist")
	}
	if len(mapping.Shards) != 1 || mapping.Shards[0] != "default" {
		t.Errorf("Expected 'users' to be mapped to 'default' shard, got %v", mapping.Shards)
	}
}

// TestAutoRouting tests the auto routing functionality
func TestAutoRouting(t *testing.T) {
	// Create a server for testing
	s, tempDir, cleanup := setupTestServer(t)
	defer cleanup()

	// Add a second shard
	shard2Path := filepath.Join(tempDir, "shard2.db")
	err := s.AddShard("shard2", shard2Path)
	if err != nil {
		t.Fatalf("Failed to add second shard: %s", err)
	}

	// Register a table
	err = s.proxy.RegisterTable("users", "user_id", "hash")
	if err != nil {
		t.Fatalf("Failed to register table: %s", err)
	}

	// Add the table to both shards
	err = s.proxy.AddTableToShard("users", "default")
	if err != nil {
		t.Fatalf("Failed to add table to default shard: %s", err)
	}
	err = s.proxy.AddTableToShard("users", "shard2")
	if err != nil {
		t.Fatalf("Failed to add table to shard2: %s", err)
	}

	// Create test tables in both shards
	for _, shardID := range []string{"default", "shard2"} {
		shard, err := s.GetShard(shardID)
		if err != nil {
			t.Fatalf("Failed to get shard %s: %s", shardID, err)
		}

		// Create a test table
		_, err = shard.DB.Exec("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, name TEXT)")
		if err != nil {
			t.Fatalf("Failed to create test table in shard %s: %s", shardID, err)
		}

		// Insert some test data
		_, err = shard.DB.Exec(fmt.Sprintf("INSERT INTO users (user_id, name) VALUES (%d, 'User in %s')", 
			100+len(shardID), shardID))
		if err != nil {
			t.Fatalf("Failed to insert test data in shard %s: %s", shardID, err)
		}
	}

	// Test auto-routing for query
	req := httptest.NewRequest("GET", "/query?q=SELECT+*+FROM+users+WHERE+user_id=107&auto_route=true", nil)
	w := httptest.NewRecorder()

	// Call the handler
	s.handleQuery(w, req)

	// Check the response
	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Test auto-routing for execute
	cmdBody := Command{
		Query: "INSERT INTO users (user_id, name) VALUES (?, ?)",
		Args:  []string{"200", "New User"},
	}
	jsonData, err := json.Marshal(cmdBody)
	if err != nil {
		t.Fatalf("Failed to marshal command: %s", err)
	}

	req = httptest.NewRequest("POST", "/execute?auto_route=true", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()

	// Initialize the server's Raft component for the execute test
	// This is a simplified version - in a real test, you would use a proper mock
	s.fsm = &FSM{
		server: s,
		logger: s.logger,
	}

	// Call the handler
	s.handleExecute(w, req)

	// Since we don't have a real Raft instance, we expect a bad request response
	// because the command parsing would fail without a proper Raft setup
	resp = w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status code %d or %d, got %d", http.StatusBadRequest, http.StatusServiceUnavailable, resp.StatusCode)
	}
}
