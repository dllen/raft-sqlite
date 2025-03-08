package server

import (
	"errors"
	"regexp"
	"strings"
)

// parseSQLQuery extracts the table name and operation type from an SQL query
// Returns: tableName, operationType, error
func parseSQLQuery(query string) (string, string, error) {
	// Normalize the query by removing extra whitespace and converting to lowercase
	normalizedQuery := strings.ToLower(strings.TrimSpace(query))
	
	// Extract the operation type (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.)
	var operation string
	if strings.HasPrefix(normalizedQuery, "select") {
		operation = "select"
	} else if strings.HasPrefix(normalizedQuery, "insert") {
		operation = "insert"
	} else if strings.HasPrefix(normalizedQuery, "update") {
		operation = "update"
	} else if strings.HasPrefix(normalizedQuery, "delete") {
		operation = "delete"
	} else if strings.HasPrefix(normalizedQuery, "create") {
		operation = "create"
	} else if strings.HasPrefix(normalizedQuery, "alter") {
		operation = "alter"
	} else if strings.HasPrefix(normalizedQuery, "drop") {
		operation = "drop"
	} else {
		return "", "", errors.New("unsupported SQL operation")
	}
	
	// Extract the table name based on the operation type
	var tableName string
	var err error
	
	switch operation {
	case "select":
		tableName, err = extractTableFromSelect(normalizedQuery)
	case "insert":
		tableName, err = extractTableFromInsert(normalizedQuery)
	case "update":
		tableName, err = extractTableFromUpdate(normalizedQuery)
	case "delete":
		tableName, err = extractTableFromDelete(normalizedQuery)
	case "create":
		tableName, err = extractTableFromCreate(normalizedQuery)
	case "alter":
		tableName, err = extractTableFromAlter(normalizedQuery)
	case "drop":
		tableName, err = extractTableFromDrop(normalizedQuery)
	}
	
	if err != nil {
		return "", "", err
	}
	
	return tableName, operation, nil
}

// extractTableFromSelect extracts the table name from a SELECT query
func extractTableFromSelect(query string) (string, error) {
	// Simple regex to match "from tablename" pattern
	re := regexp.MustCompile(`from\s+([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) < 2 {
		return "", errors.New("could not extract table name from SELECT query")
	}
	
	return matches[1], nil
}

// extractTableFromInsert extracts the table name from an INSERT query
func extractTableFromInsert(query string) (string, error) {
	// Simple regex to match "insert into tablename" pattern
	re := regexp.MustCompile(`insert\s+into\s+([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) < 2 {
		return "", errors.New("could not extract table name from INSERT query")
	}
	
	return matches[1], nil
}

// extractTableFromUpdate extracts the table name from an UPDATE query
func extractTableFromUpdate(query string) (string, error) {
	// Simple regex to match "update tablename" pattern
	re := regexp.MustCompile(`update\s+([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) < 2 {
		return "", errors.New("could not extract table name from UPDATE query")
	}
	
	return matches[1], nil
}

// extractTableFromDelete extracts the table name from a DELETE query
func extractTableFromDelete(query string) (string, error) {
	// Simple regex to match "delete from tablename" pattern
	re := regexp.MustCompile(`delete\s+from\s+([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) < 2 {
		return "", errors.New("could not extract table name from DELETE query")
	}
	
	return matches[1], nil
}

// extractTableFromCreate extracts the table name from a CREATE query
func extractTableFromCreate(query string) (string, error) {
	// Simple regex to match "create table tablename" pattern
	re := regexp.MustCompile(`create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) < 2 {
		return "", errors.New("could not extract table name from CREATE query")
	}
	
	return matches[1], nil
}

// extractTableFromAlter extracts the table name from an ALTER query
func extractTableFromAlter(query string) (string, error) {
	// Simple regex to match "alter table tablename" pattern
	re := regexp.MustCompile(`alter\s+table\s+([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) < 2 {
		return "", errors.New("could not extract table name from ALTER query")
	}
	
	return matches[1], nil
}

// extractTableFromDrop extracts the table name from a DROP query
func extractTableFromDrop(query string) (string, error) {
	// Simple regex to match "drop table tablename" pattern
	re := regexp.MustCompile(`drop\s+table\s+(?:if\s+exists\s+)?([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(query)
	
	if len(matches) < 2 {
		return "", errors.New("could not extract table name from DROP query")
	}
	
	return matches[1], nil
}
