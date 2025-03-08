package client

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestNewClient tests the creation of a new client
func TestNewClient(t *testing.T) {
	serverAddr := "localhost:11000"
	client := New(serverAddr)

	if client.serverAddr != serverAddr {
		t.Errorf("Expected serverAddr to be %s, got %s", serverAddr, client.serverAddr)
	}
	if client.httpClient == nil {
		t.Error("Expected httpClient to be initialized")
	}
}

// TestExecute tests the Execute method
func TestExecute(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/execute" {
			t.Errorf("Expected request to '/execute', got '%s'", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST request, got '%s'", r.Method)
		}

		// Read the request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read request body: %s", err)
		}

		// Parse the request body
		var cmd Command
		if err := json.Unmarshal(body, &cmd); err != nil {
			t.Fatalf("Failed to parse request body: %s", err)
		}

		// Check the command
		if cmd.Query != "INSERT INTO test (name) VALUES (?)" {
			t.Errorf("Expected query to be 'INSERT INTO test (name) VALUES (?)', got '%s'", cmd.Query)
		}

		// Send a success response
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := New(server.URL[7:]) // Remove 'http://' from URL

	err := client.Execute("INSERT INTO test (name) VALUES (?)", "test1")
	if err != nil {
		t.Errorf("Expected no error, got %s", err)
	}
}

// TestQuery tests the Query method
func TestQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			t.Errorf("Expected request to '/query', got '%s'", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET request, got '%s'", r.Method)
		}

		// Send a mock response
		response := []map[string]interface{}{{"id": 1, "name": "test1"}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := New(server.URL[7:]) // Remove 'http://' from URL

	result, err := client.Query("SELECT * FROM test")
	if err != nil {
		t.Errorf("Expected no error, got %s", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result))
	}

	if result[0]["name"] != "test1" {
		t.Errorf("Expected name to be 'test1', got '%v'", result[0]["name"])
	}
}
