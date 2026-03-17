package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTable creates a new table in the store.
func (s *Store) CreateTable(_ context.Context, params *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.TableName == nil || *params.TableName == "" {
		return nil, fmt.Errorf("TableName is required")
	}
	if len(params.KeySchema) == 0 {
		return nil, fmt.Errorf("KeySchema is required")
	}
	if len(params.AttributeDefinitions) == 0 {
		return nil, fmt.Errorf("AttributeDefinitions is required")
	}

	// Build attribute type lookup from AttributeDefinitions.
	attrTypes := make(map[string]table.KeyKind, len(params.AttributeDefinitions))
	for _, ad := range params.AttributeDefinitions {
		if ad.AttributeName == nil {
			continue
		}
		kind, err := sdkScalarToKeyKind(ad.AttributeType)
		if err != nil {
			return nil, err
		}
		attrTypes[*ad.AttributeName] = kind
	}

	// Parse table key schema.
	keyDefs, err := parseKeySchema(params.KeySchema, attrTypes)
	if err != nil {
		return nil, err
	}

	def := table.TableDefinition{
		Name:           *params.TableName,
		KeyDefinitions: keyDefs,
	}

	// Parse GSIs.
	for _, gsi := range params.GlobalSecondaryIndexes {
		if gsi.IndexName == nil {
			continue
		}
		gsiKeyDefs, err := parseKeySchema(gsi.KeySchema, attrTypes)
		if err != nil {
			return nil, fmt.Errorf("GSI %s: %w", *gsi.IndexName, err)
		}
		def.GSIs = append(def.GSIs, table.GSIDefinition{
			Name:           *gsi.IndexName,
			KeyDefinitions: gsiKeyDefs,
		})
	}

	// Register the table.
	schema := &tableSchema{
		definition: def,
		gsis:       make(map[string]*gsiSchema),
	}
	for _, gsiDef := range def.GSIs {
		schema.gsis[gsiDef.Name] = &gsiSchema{
			tableName:  def.Name,
			definition: gsiDef,
		}
	}

	s.mu.Lock()
	if _, exists := s.tables[*params.TableName]; exists {
		s.mu.Unlock()
		return nil, &types.ResourceInUseException{
			Message: aws.String(fmt.Sprintf("Table already exists: %s", *params.TableName)),
		}
	}
	s.tables[*params.TableName] = schema
	s.mu.Unlock()

	desc := buildTableDescription(schema)
	return &dynamodb.CreateTableOutput{
		TableDescription: &desc,
	}, nil
}

// parseKeySchema converts AWS SDK KeySchemaElements into the internal PrimaryKeyDefinition.
func parseKeySchema(ks []types.KeySchemaElement, attrTypes map[string]table.KeyKind) (table.PrimaryKeyDefinition, error) {
	var def table.PrimaryKeyDefinition
	for _, elem := range ks {
		if elem.AttributeName == nil {
			continue
		}
		kind, ok := attrTypes[*elem.AttributeName]
		if !ok {
			return def, fmt.Errorf("attribute %q in KeySchema not found in AttributeDefinitions", *elem.AttributeName)
		}
		switch elem.KeyType {
		case types.KeyTypeHash:
			def.PartitionKey = table.KeyDef{Name: *elem.AttributeName, Kind: kind}
		case types.KeyTypeRange:
			def.SortKey = table.KeyDef{Name: *elem.AttributeName, Kind: kind}
		default:
			return def, fmt.Errorf("unknown key type: %s", elem.KeyType)
		}
	}
	if def.PartitionKey.Name == "" {
		return def, fmt.Errorf("HASH key is required in KeySchema")
	}
	return def, nil
}

// sdkScalarToKeyKind converts an AWS SDK ScalarAttributeType to the internal KeyKind.
func sdkScalarToKeyKind(sat types.ScalarAttributeType) (table.KeyKind, error) {
	switch sat {
	case types.ScalarAttributeTypeS:
		return table.KeyKindS, nil
	case types.ScalarAttributeTypeN:
		return table.KeyKindN, nil
	case types.ScalarAttributeTypeB:
		return table.KeyKindB, nil
	default:
		return "", fmt.Errorf("unsupported attribute type: %s", sat)
	}
}
