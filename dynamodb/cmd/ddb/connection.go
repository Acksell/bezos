package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbiface"
	"github.com/acksell/bezos/dynamodb/ddbstore"
	"github.com/acksell/bezos/dynamodb/ddbui"
	"github.com/acksell/bezos/dynamodb/schema"
)

// ConnectionFlags holds the common flags for connecting to DynamoDB.
// Used by get, query, scan, and ui commands.
type ConnectionFlags struct {
	AWS      *bool
	Region   *string
	Profile  *string
	Endpoint *string
	DB       *string
	Memory   *bool
}

// RegisterConnectionFlags adds the standard connection flags to a FlagSet.
// Defaults to AWS mode (--aws is implicitly true unless --db or --memory is set).
func RegisterConnectionFlags(fs *flag.FlagSet) *ConnectionFlags {
	return &ConnectionFlags{
		AWS:      fs.Bool("aws", false, "connect to real AWS DynamoDB (default when no --db or --memory)"),
		Region:   fs.String("region", "", "AWS region"),
		Profile:  fs.String("profile", "", "AWS profile name"),
		Endpoint: fs.String("endpoint", "", "custom DynamoDB endpoint URL"),
		DB:       fs.String("db", "", "path to local database directory"),
		Memory:   fs.Bool("memory", false, "use in-memory database"),
	}
}

// IsLocal returns true if the user explicitly requested a local or in-memory store.
func (cf *ConnectionFlags) IsLocal() bool {
	return *cf.DB != "" || *cf.Memory
}

// IsAWS returns true if the connection should use AWS.
// Defaults to true unless --db or --memory is set.
func (cf *ConnectionFlags) IsAWS() bool {
	if cf.IsLocal() {
		return false
	}
	return true // Default to AWS
}

// Connect creates a DynamoDB client based on the connection flags.
// Returns the client, a cleanup function, and any error.
// The cleanup function must be called when done (it's a no-op for AWS clients).
func (cf *ConnectionFlags) Connect(ctx context.Context, schemas []schema.Schema) (ddbiface.ReadWriteClient, func(), error) {
	if cf.IsLocal() {
		return cf.connectLocal(schemas)
	}
	return cf.connectAWS(ctx)
}

func (cf *ConnectionFlags) connectAWS(ctx context.Context) (ddbiface.ReadWriteClient, func(), error) {
	opts := AWSOptions{
		Region:   *cf.Region,
		Profile:  *cf.Profile,
		Endpoint: *cf.Endpoint,
	}
	client, err := createAWSClient(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	return client, func() {}, nil
}

func (cf *ConnectionFlags) connectLocal(schemas []schema.Schema) (ddbiface.ReadWriteClient, func(), error) {
	tableDefs := ddbui.TableDefinitionsFromSchemas(schemas...)

	storeOpts := ddbstore.StoreOptions{
		Path:     *cf.DB,
		InMemory: *cf.Memory || *cf.DB == "",
	}
	store, err := ddbstore.New(storeOpts, tableDefs...)
	if err != nil {
		return nil, nil, fmt.Errorf("creating store: %w", err)
	}
	return store, func() { store.Close() }, nil
}
