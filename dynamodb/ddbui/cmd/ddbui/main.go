// ddbui starts a local debugging UI for DynamoDB data stored in ddbstore.
//
// # Installation
//
//	go install github.com/acksell/bezos/dynamodb/ddbui/cmd/ddbui@latest
//
// # Usage
//
// Start the UI server with your schema files:
//
//	ddbui --schema ./schema_*.yaml --port 8080
//
// This starts a web server at http://localhost:8080 with an in-memory database.
// To persist data, provide a database path:
//
//	ddbui --schema ./schema_*.yaml --db ./data --port 8080
//
// # Flags
//
//	-schema string
//	    	Glob pattern for schema YAML files (required)
//	-db string
//	    	Path to BadgerDB database (empty for in-memory)
//	-port int
//	    	HTTP port to listen on (default 8080)
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/acksell/bezos/dynamodb/ddbui"
)

func main() {
	var (
		schemaPattern = flag.String("schema", "", "glob pattern for schema YAML files (required)")
		dbPath        = flag.String("db", "", "path to BadgerDB database (empty for in-memory)")
		port          = flag.Int("port", 8080, "HTTP port to listen on")
	)
	flag.Parse()

	if *schemaPattern == "" {
		fmt.Fprintln(os.Stderr, "ddbui: --schema flag is required")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "  ddbui --schema ./schema_*.yaml [--db ./data] [--port 8080]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Generate schema files with ddbgen:")
		fmt.Fprintln(os.Stderr, "  ddbgen --schema")
		os.Exit(1)
	}

	server, err := ddbui.NewServer(ddbui.ServerConfig{
		Port:          *port,
		DBPath:        *dbPath,
		SchemaPattern: *schemaPattern,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ddbui: %v\n", err)
		os.Exit(1)
	}

	if err := server.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "ddbui: %v\n", err)
		os.Exit(1)
	}
}
