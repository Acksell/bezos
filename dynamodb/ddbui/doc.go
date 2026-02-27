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
//	go install github.com/acksell/bezos/dynamodb/cmd/ddb@latest
//
// Start the UI server (auto-discovers schema files):
//
//	ddb ui
//
// This will start a web server at http://localhost:3070 with data stored in .ddb/data/.
//
// Use in-memory mode for ephemeral data:
//
//	ddb ui --memory
//
// Configure defaults via ddb.yaml in your project root:
//
//	schemaDir: ./schemas
//	dataDir: .ddb/data
//	port: 3070
package ddbui
