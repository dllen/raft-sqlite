package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Client represents a client for the distributed SQLite database
type Client struct {
	serverAddr string
	httpClient *http.Client
}

// Command represents a SQL statement to be executed on the SQLite database
type Command struct {
	Query   string   `json:"query"`
	Args    []string `json:"args,omitempty"`
	Results []string `json:"results,omitempty"`
}

// New creates a new client
func New(serverAddr string) *Client {
	return &Client{
		serverAddr: serverAddr,
		httpClient: &http.Client{},
	}
}

// Execute executes a SQL statement on the database
func (c *Client) Execute(query string, args ...string) error {
	// Create command
	cmd := Command{
		Query: query,
		Args:  args,
	}

	// Marshal command to JSON
	cmdJSON, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %s", err)
	}

	// Send request to server
	resp, err := c.httpClient.Post(
		fmt.Sprintf("http://%s/execute", c.serverAddr),
		"application/json",
		bytes.NewBuffer(cmdJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to send request to server: %s", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned error: %s", string(body))
	}

	return nil
}

// Query executes a read-only SQL query on the database
func (c *Client) Query(query string) ([]map[string]interface{}, error) {
	// Encode query as URL parameter
	params := url.Values{}
	params.Add("q", query)

	// Send request to server
	resp, err := c.httpClient.Get(
		fmt.Sprintf("http://%s/query?%s", c.serverAddr, params.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to server: %s", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned error: %s", string(body))
	}

	// Parse response
	var result []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %s", err)
	}

	return result, nil
}
