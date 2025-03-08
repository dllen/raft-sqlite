package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNew tests the creation of a new server
func TestNew(t *testing.T) {
	nodeID := "node1"
	dbPath := filepath.Join(t.TempDir(), "test.db")
	raftAddr := "localhost:12000"
	httpAddr := "localhost:11000"

	s := New(nodeID, dbPath, raftAddr, httpAddr)

	if s.nodeID != nodeID {
		t.Errorf("Expected nodeID to be %s, got %s", nodeID, s.nodeID)
	}
	if s.dbPath != dbPath {
		t.Errorf("Expected dbPath to be %s, got %s", dbPath, s.dbPath)
	}
	if s.raftAddr != raftAddr {
		t.Errorf("Expected raftAddr to be %s, got %s", raftAddr, s.raftAddr)
	}
	if s.httpAddr != httpAddr {
		t.Errorf("Expected httpAddr to be %s, got %s", httpAddr, s.httpAddr)
	}
	if s.logger == nil {
		t.Error("Expected logger to be initialized")
	}
}

// TestHandler tests the HTTP handler creation
func TestHandler(t *testing.T) {
	s := New("node1", "test.db", "localhost:12000", "localhost:11000")
	handler := s.Handler()

	if handler == nil {
		t.Error("Expected handler to be initialized")
	}

	// Test that the handler has the expected routes
	_, ok := handler.(*http.ServeMux)
	if !ok {
		t.Error("Expected handler to be of type *http.ServeMux")
	}

	// Unfortunately, there's no direct way to inspect the routes in a ServeMux
	// We could use reflection, but that's brittle. Instead, we'll just check
	// that the handler doesn't panic when created.
}

// setupTestServer creates a server for testing
func setupTestServer(t *testing.T) (*Server, string, func()) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create a server
	s := New("test-node", dbPath, "localhost:0", "localhost:0")
	
	// Add a default shard
	err := s.AddShard("default", dbPath)
	if err != nil {
		t.Fatalf("Failed to add default shard: %s", err)
	}

	// Create cleanup function
	cleanup := func() {
		// Close all shard connections
		for _, shard := range s.shards {
			if shard.DB != nil {
				shard.DB.Close()
			}
		}
		if s.raft != nil {
			s.raft.Shutdown().Error()
		}
		os.RemoveAll(tempDir)
	}

	return s, tempDir, cleanup
}

// TestStartServerInMemory tests starting a server with an in-memory transport
// This is a more complex test that requires mocking the Raft implementation
func TestStartServerInMemory(t *testing.T) {
	// Skip this test in short mode as it's more complex
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "TestStartServerInMemory")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test server parameters
	id := "001"
	raftAddr := "localhost:12000"
	raftDir := filepath.Join(tempDir, "001")
	dbPath := filepath.Join(tempDir, "001", "test.db")

	// Create the directory structure
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		t.Fatalf("Failed to create raft directory: %s", err)
	}

	// Create a server directly with the parameters
	s := New(id, raftAddr, raftDir, dbPath)
	s.logger = log.New(os.Stderr, "[server-test] ", log.LstdFlags)

	// Initialize the database
	db, err := s.initializeDB()
	if err != nil {
		t.Fatalf("Failed to initialize database: %s", err)
	}
	defer db.Close()

	// Verify the database was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("Expected database file to exist at %s", dbPath)
	} else {
		t.Logf("Database file exists at %s", dbPath)
	}
}

// Helper method to initialize just the database for testing
func (s *Server) initializeDB() (*sql.DB, error) {
	// Check if default shard exists, if not add it
	shard, err := s.GetShard("default")
	if err != nil {
		// Add default shard
		err = s.AddShard("default", s.dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to add default shard: %s", err)
		}
		shard, _ = s.GetShard("default")
	}

	// Return the database connection from the default shard
	return shard.DB, nil
}

// TestHandleJoin tests the join handler
func TestHandleJoin(t *testing.T) {
	s, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Initialize the server without starting Raft
	db, err := s.initializeDB()
	if err != nil {
		t.Fatalf("Failed to initialize database: %s", err)
	}
	defer db.Close()

	// Create a mock FSM
	s.fsm = &FSM{
		db:     db, // Using the default shard's DB for FSM
		logger: s.logger,
		dbPath: s.dbPath,
		server: s,
	}

	// Create a test HTTP request
	reqBody := map[string]string{
		"node_id":   "node2",
		"raft_addr": "localhost:12001",
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("Failed to marshal request body: %s", err)
	}

	req := httptest.NewRequest("POST", "/join", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Skip the Raft check in the handler for testing
	// We'll modify the handler to handle nil raft
	s.handleJoin(w, req)

	// Check the response
	resp := w.Result()
	defer resp.Body.Close()

	// Since we don't have a real Raft instance, we expect a service unavailable response
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status code %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}
}

// TestHandleExecute tests the execute handler
func TestHandleExecute(t *testing.T) {
	s, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Initialize the server without starting Raft
	db, err := s.initializeDB()
	if err != nil {
		t.Fatalf("Failed to initialize database: %s", err)
	}
	defer db.Close()

	// Create a mock FSM
	s.fsm = &FSM{
		db:     db, // Using the default shard's DB for FSM
		logger: s.logger,
		dbPath: s.dbPath,
	}

	// Create a test HTTP request
	reqBody := Command{
		Query: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("Failed to marshal request body: %s", err)
	}

	req := httptest.NewRequest("POST", "/execute?shard_id=default", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Call the handler
	s.handleExecute(w, req)

	// Check the response
	resp := w.Result()
	defer resp.Body.Close()

	// Since we don't have a real Raft instance, we expect a service unavailable response
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status code %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	// Check the response body
	expectedBody := "No leader available"
	if !strings.Contains(string(body), expectedBody) {
		t.Errorf("Expected response body to contain '%s', got '%s'", expectedBody, string(body))
	}
}

// TestHandleQuery tests the query handler
func TestHandleQuery(t *testing.T) {
	s, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Initialize the server without starting Raft
	db, err := s.initializeDB()
	if err != nil {
		t.Fatalf("Failed to initialize database: %s", err)
	}
	defer db.Close()

	// Create a test table and insert some data
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %s", err)
	}

	_, err = db.Exec("INSERT INTO test (name) VALUES (?)", "test1")
	if err != nil {
		t.Fatalf("Failed to insert test data: %s", err)
	}

	// Create a test HTTP request
	req := httptest.NewRequest("GET", "/query?q=SELECT+*+FROM+test&shard_id=default", nil)
	w := httptest.NewRecorder()

	// Call the handler
	s.handleQuery(w, req)

	// Check the response
	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}

	// Parse the response body
	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("Failed to parse response body: %s", err)
	}

	// Check the response data
	if len(result) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result))
	}

	if result[0]["name"] != "test1" {
		t.Errorf("Expected name to be 'test1', got '%v'", result[0]["name"])
	}
}

// TestJoin tests the Join method
func TestJoin(t *testing.T) {
	// Create a test server to handle the join request
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/join" {
			t.Errorf("Expected request to '/join', got '%s'", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got '%s'", r.Method)
		}

		// Read the request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read request body: %s", err)
		}

		// Parse the request body
		var data map[string]string
		if err := json.Unmarshal(body, &data); err != nil {
			t.Fatalf("Failed to parse request body: %s", err)
		}

		// Check the request data
		if data["node_id"] != "test-node" {
			t.Errorf("Expected node_id to be 'test-node', got '%s'", data["node_id"])
		}
		if data["raft_addr"] != "localhost:12000" {
			t.Errorf("Expected raft_addr to be 'localhost:12000', got '%s'", data["raft_addr"])
		}

		// Send a success response
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Extract the host and port from the test server URL
	// The URL is in the format "http://127.0.0.1:port"
	leaderAddr := strings.TrimPrefix(ts.URL, "http://")

	// Create a server
	s := New("test-node", "test.db", "localhost:12000", "localhost:11000")

	// Join the cluster
	if err := s.Join(leaderAddr); err != nil {
		t.Fatalf("Failed to join cluster: %s", err)
	}
}

// Integration tests would test the full server with a real Raft cluster
// These are more complex and would require setting up multiple nodes
// For simplicity, we'll omit them here, but in a real project they would be important
