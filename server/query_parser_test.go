package server

import (
	"testing"
)

func TestParseSQLQuery(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedTable  string
		expectedOp     string
		expectError    bool
	}{
		{
			name:           "Simple SELECT",
			query:          "SELECT * FROM users",
			expectedTable:  "users",
			expectedOp:     "select",
			expectError:    false,
		},
		{
			name:           "SELECT with WHERE",
			query:          "SELECT id, name FROM users WHERE id = 1",
			expectedTable:  "users",
			expectedOp:     "select",
			expectError:    false,
		},
		{
			name:           "Simple INSERT",
			query:          "INSERT INTO users (id, name) VALUES (1, 'John')",
			expectedTable:  "users",
			expectedOp:     "insert",
			expectError:    false,
		},
		{
			name:           "Simple UPDATE",
			query:          "UPDATE users SET name = 'Jane' WHERE id = 1",
			expectedTable:  "users",
			expectedOp:     "update",
			expectError:    false,
		},
		{
			name:           "Simple DELETE",
			query:          "DELETE FROM users WHERE id = 1",
			expectedTable:  "users",
			expectedOp:     "delete",
			expectError:    false,
		},
		{
			name:           "CREATE TABLE",
			query:          "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			expectedTable:  "users",
			expectedOp:     "create",
			expectError:    false,
		},
		{
			name:           "CREATE TABLE IF NOT EXISTS",
			query:          "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)",
			expectedTable:  "users",
			expectedOp:     "create",
			expectError:    false,
		},
		{
			name:           "ALTER TABLE",
			query:          "ALTER TABLE users ADD COLUMN email TEXT",
			expectedTable:  "users",
			expectedOp:     "alter",
			expectError:    false,
		},
		{
			name:           "DROP TABLE",
			query:          "DROP TABLE users",
			expectedTable:  "users",
			expectedOp:     "drop",
			expectError:    false,
		},
		{
			name:           "DROP TABLE IF EXISTS",
			query:          "DROP TABLE IF EXISTS users",
			expectedTable:  "users",
			expectedOp:     "drop",
			expectError:    false,
		},
		{
			name:           "Invalid query",
			query:          "INVALID SQL STATEMENT",
			expectedTable:  "",
			expectedOp:     "",
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableName, op, err := parseSQLQuery(tt.query)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if tableName != tt.expectedTable {
				t.Errorf("Expected table name %q, got %q", tt.expectedTable, tableName)
			}
			
			if op != tt.expectedOp {
				t.Errorf("Expected operation %q, got %q", tt.expectedOp, op)
			}
		})
	}
}

func TestExtractTableFromSelect(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedTable string
		expectError   bool
	}{
		{
			name:          "Simple SELECT",
			query:         "select * from users",
			expectedTable: "users",
			expectError:   false,
		},
		{
			name:          "SELECT with WHERE",
			query:         "select id, name from users where id = 1",
			expectedTable: "users",
			expectError:   false,
		},
		{
			name:          "SELECT with JOIN",
			query:         "select u.id, p.name from users u join profiles p on u.id = p.user_id",
			expectedTable: "users",
			expectError:   false,
		},
		{
			name:          "Invalid SELECT",
			query:         "select * where id = 1",
			expectedTable: "",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableName, err := extractTableFromSelect(tt.query)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if tableName != tt.expectedTable {
				t.Errorf("Expected table name %q, got %q", tt.expectedTable, tableName)
			}
		})
	}
}
