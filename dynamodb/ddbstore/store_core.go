package ddbstore

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/dgraph-io/badger/v4"
)

// Store is a DynamoDB-compatible store backed by BadgerDB.
// It provides full ACID guarantees and supports all major DynamoDB operations.
type Store struct {
	db     *badger.DB
	tables map[string]*tableSchema
}

type tableSchema struct {
	definition table.TableDefinition
	gsis       map[string]*gsiSchema
}

func (t *tableSchema) encodeKey(pk table.PrimaryKey) ([]byte, error) {
	return encodeBadgerKey(t.definition.Name, "", pk)
}

type gsiSchema struct {
	tableName  string
	definition table.GSIDefinition
}

func (g *gsiSchema) encodeKey(pk table.PrimaryKey) ([]byte, error) {
	return encodeBadgerKey(g.tableName, g.definition.Name, pk)
}

// StoreOptions configures the BadgerDB store.
type StoreOptions struct {
	// Path to the database directory. If empty, uses in-memory mode.
	Path string
	// InMemory forces in-memory mode even if Path is set.
	InMemory bool
	// Logger for BadgerDB. If nil, logging is disabled.
	Logger badger.Logger
}

// New creates a new BadgerDB-backed DynamoDB store.
func New(opts StoreOptions, defs ...table.TableDefinition) (*Store, error) {
	badgerOpts := badger.DefaultOptions(opts.Path)

	if opts.Path == "" || opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	}

	if opts.Logger != nil {
		badgerOpts = badgerOpts.WithLogger(opts.Logger)
	} else {
		badgerOpts = badgerOpts.WithLogger(nil)
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("open badger db: %w", err)
	}

	tables := make(map[string]*tableSchema)
	for _, def := range defs {
		schema := &tableSchema{
			definition: def,
			gsis:       make(map[string]*gsiSchema),
		}

		for _, gsiDef := range def.GSIs {
			gsiSch := &gsiSchema{
				tableName:  def.Name,
				definition: gsiDef,
			}
			schema.gsis[gsiDef.Name] = gsiSch
		}

		tables[def.Name] = schema
	}

	return &Store{
		db:     db,
		tables: tables,
	}, nil
}

// Close closes the BadgerDB database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) getTable(tableName *string) (*tableSchema, error) {
	if tableName == nil {
		return nil, fmt.Errorf("table name is required")
	}
	schema, ok := s.tables[*tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", *tableName)
	}
	return schema, nil
}

// Used in query/scan to get the appropriate key encoder based on table and index name.
func (s *Store) getBadgerKeyEncoder(tableName *string, indexName *string) (*badgerKeyEncoder, error) {
	schema, err := s.getTable(tableName)
	if err != nil {
		return nil, err
	}
	if indexName == nil || *indexName == "" {
		return &badgerKeyEncoder{
			tableName: schema.definition.Name,
			indexName: "",
			keyDefs:   schema.definition.KeyDefinitions,
		}, nil
	}
	gsi, ok := schema.gsis[*indexName]
	if !ok {
		return nil, fmt.Errorf("GSI not found: %s", *indexName)
	}
	return &badgerKeyEncoder{
		tableName: gsi.tableName,
		indexName: gsi.definition.Name,
		keyDefs:   gsi.definition.KeyDefinitions,
	}, nil
}
