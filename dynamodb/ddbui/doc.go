// Package ddbui provides a local debugging UI for DynamoDB data stored in ddbstore.
//
// It serves a web interface that allows users to:
//   - View table structures and entity schemas
//   - Browse, query, and scan data
//   - Create, update, and delete items
//   - Visualize key patterns and GSI mappings
//
// # Usage
//
// Install the CLI:
//
//	go install github.com/acksell/bezos/dynamodb/ddbui/cmd/ddbui@latest
//
// Start the UI server:
//
//	ddbui --schema ./schema_*.yaml --port 8080
//
// This will start a web server at http://localhost:8080 with an in-memory database.
// To persist data, provide a database path:
//
//	ddbui --schema ./schema_*.yaml --db ./data --port 8080
package ddbui
