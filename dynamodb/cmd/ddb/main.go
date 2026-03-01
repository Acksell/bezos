// ddb is a unified CLI for DynamoDB development tools.
//
// # Installation
//
//	go install github.com/acksell/bezos/dynamodb/cmd/ddb@latest
//
// # Commands
//
//	ddb gen    Generate type-safe key constructors and schema files
//	ddb ui     Start the local debugging UI
//
// # Quick Start
//
// Register indexes with indices.Add and add a go:generate directive:
//
//	//go:generate ddb gen
//
//	var _ = indices.Add(index.PrimaryIndex[User]{...})
//
// Generate code and schema files:
//
//	go generate ./...
//
// Or regenerate all packages from CLI:
//
//	ddb gen
//
// Start the UI:
//
//	ddb ui --db ./data
//	ddb ui --memory
package main

import (
	"fmt"
	"os"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	// Remove the subcommand from args so flag parsing works
	os.Args = append([]string{os.Args[0]}, os.Args[2:]...)

	var err error
	switch cmd {
	case "gen", "generate":
		err = runGen()
	case "ui", "serve":
		err = runUI()
	case "help", "-h", "--help":
		printUsage()
		return
	case "version", "-v", "--version":
		fmt.Printf("ddb version %s\n", version)
		return
	default:
		fmt.Fprintf(os.Stderr, "ddb: unknown command %q\n\n", cmd)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "ddb %s: %v\n", cmd, err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`ddb - DynamoDB development tools

Usage:
  ddb <command> [flags]

Commands:
  gen     Generate type-safe key constructors and schema files
  ui      Start the local debugging UI

Examples:
  # Add to your entity package and run go generate:
  //go:generate ddb gen
  go generate ./...

  # Start UI with local database:
  ddb ui --db ./data

  # Start UI with in-memory database:
  ddb ui --memory

Configuration (optional):
  Create ddb.ui.yaml for UI defaults:

    dataDir: ./data    # database directory
    port: 3070         # UI server port

Run 'ddb <command> --help' for more information on a command.`)
}
