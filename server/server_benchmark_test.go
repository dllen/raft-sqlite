package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// BenchmarkServer benchmarks the server's response to execute requests
func BenchmarkServer(b *testing.B) {
	s := New("node1", "test.db", "localhost:12000", "localhost:11000")
	handler := s.Handler()

	// Create a test server
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Define the request payload
	payload := `{"query": "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)"}`

	// Run the benchmark
	for n := 0; n < b.N; n++ {
		resp, err := http.Post(ts.URL+"/execute", "application/json", strings.NewReader(payload))
		if err != nil {
			b.Fatalf("Failed to send request: %s", err)
		}
		resp.Body.Close()
	}
}

// BenchmarkQuery benchmarks the server's response to query requests
func BenchmarkQuery(b *testing.B) {
	s := New("node1", "test.db", "localhost:12000", "localhost:11000")
	handler := s.Handler()

	// Create a test server
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Run the benchmark
	for n := 0; n < b.N; n++ {
		resp, err := http.Get(ts.URL + "/query?q=SELECT+*+FROM+test")
		if err != nil {
			b.Fatalf("Failed to send request: %s", err)
		}
		resp.Body.Close()
	}
}
