package main

import (
	"fmt"
	"log"
	"time"

	"github.com/dllen/raft-sqlite/client"
)

func main() {
	// Create a client connected to the first node
	c := client.New("localhost:11000")

	// Create a table
	fmt.Println("Creating users table...")
	err := c.Execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		log.Fatalf("Failed to create table: %s", err)
	}

	// Insert some data
	fmt.Println("Inserting data...")
	users := []struct {
		name  string
		email string
	}{
		{"Alice", "alice@example.com"},
		{"Bob", "bob@example.com"},
		{"Charlie", "charlie@example.com"},
	}

	for _, user := range users {
		err := c.Execute("INSERT INTO users (name, email) VALUES (?, ?)", user.name, user.email)
		if err != nil {
			log.Fatalf("Failed to insert data: %s", err)
		}
	}

	// Query the data
	fmt.Println("Querying data...")
	results, err := c.Query("SELECT * FROM users")
	if err != nil {
		log.Fatalf("Failed to query data: %s", err)
	}

	// Print the results
	fmt.Println("Results:")
	for _, row := range results {
		fmt.Printf("ID: %v, Name: %v, Email: %v\n", row["id"], row["name"], row["email"])
	}

	// Create a client connected to another node (if running a cluster)
	fmt.Println("\nTrying to query from another node...")
	c2 := client.New("localhost:11001")
	
	// Wait a moment for replication
	time.Sleep(1 * time.Second)
	
	// Query from the second node
	results2, err := c2.Query("SELECT * FROM users")
	if err != nil {
		fmt.Printf("Failed to query from second node: %s\n", err)
		fmt.Println("This is expected if you're running only one node.")
	} else {
		fmt.Println("Results from second node:")
		for _, row := range results2 {
			fmt.Printf("ID: %v, Name: %v, Email: %v\n", row["id"], row["name"], row["email"])
		}
	}
}
