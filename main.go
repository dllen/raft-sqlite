package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/dllen/raft-sqlite/server"
)

// Command line defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
	DefaultJoinAddr = ""
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string
var dataDir string

func init() {
	flag.StringVar(&httpAddr, "http", DefaultHTTPAddr, "HTTP server bind address")
	flag.StringVar(&raftAddr, "raft", DefaultRaftAddr, "Raft server bind address")
	flag.StringVar(&joinAddr, "join", DefaultJoinAddr, "Address of Raft leader to join")
	flag.StringVar(&nodeID, "id", "", "Node ID (required)")
	flag.StringVar(&dataDir, "data", "", "Data directory (required)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Validate command line parameters
	if nodeID == "" {
		log.Fatal("Node ID not provided")
	}
	if dataDir == "" {
		log.Fatal("Data directory not provided")
	}

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		log.Fatalf("Failed to create data directory: %s", err.Error())
	}

	// Create the SQLite database path
	dbPath := filepath.Join(dataDir, "sqlite.db")

	// Create and start the server
	s := server.New(nodeID, dbPath, raftAddr, httpAddr)
	if err := s.Start(); err != nil {
		log.Fatalf("Failed to start server: %s", err.Error())
	}

	// If join address specified, attempt to join the cluster
	if joinAddr != "" {
		if err := s.Join(joinAddr); err != nil {
			log.Fatalf("Failed to join cluster at %s: %s", joinAddr, err.Error())
		}
	}

	log.Printf("Server started at HTTP address: %s, Raft address: %s", httpAddr, raftAddr)
	log.Fatal(http.ListenAndServe(httpAddr, s.Handler()))
}
