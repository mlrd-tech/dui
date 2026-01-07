// Copyright 2026 mlrd.tech, Inc.
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"flag"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	endpoint := flag.String("e", "", "DynamoDB endpoint (default: http://localhost:8000)")
	tableName := flag.String("t", "", "Table name to select on startup")
	flag.Parse()

	// Resolve endpoint: flag > env > default
	ep := *endpoint
	if ep == "" {
		ep = os.Getenv("DDB_ENDPOINT")
	}
	if ep == "" {
		ep = "http://localhost:8000"
	}

	db, err := NewDB(ep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to DynamoDB: %v\n", err)
		os.Exit(1)
	}

	m := NewModel(db, *tableName)
	p := tea.NewProgram(m, tea.WithAltScreen())

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running program: %v\n", err)
		os.Exit(1)
	}
}
