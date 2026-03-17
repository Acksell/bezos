package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DescribeTable returns information about a table.
func (s *Store) DescribeTable(_ context.Context, params *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.TableName == nil || *params.TableName == "" {
		return nil, fmt.Errorf("TableName is required")
	}

	s.mu.RLock()
	schema, ok := s.tables[*params.TableName]
	s.mu.RUnlock()
	if !ok {
		return nil, &types.ResourceNotFoundException{
			Message: aws.String(fmt.Sprintf("Requested resource not found: Table: %s not found", *params.TableName)),
		}
	}

	desc := buildTableDescription(schema)
	return &dynamodb.DescribeTableOutput{
		Table: &desc,
	}, nil
}

// buildTableDescription converts internal tableSchema to the AWS SDK TableDescription.
func buildTableDescription(schema *tableSchema) types.TableDescription {
	def := schema.definition

	// Build KeySchema.
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: aws.String(def.KeyDefinitions.PartitionKey.Name),
			KeyType:       types.KeyTypeHash,
		},
	}
	if def.KeyDefinitions.SortKey.Name != "" {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(def.KeyDefinitions.SortKey.Name),
			KeyType:       types.KeyTypeRange,
		})
	}

	// Collect all unique attribute definitions from table + GSI keys.
	attrSet := make(map[string]table.KeyKind)
	attrSet[def.KeyDefinitions.PartitionKey.Name] = def.KeyDefinitions.PartitionKey.Kind
	if def.KeyDefinitions.SortKey.Name != "" {
		attrSet[def.KeyDefinitions.SortKey.Name] = def.KeyDefinitions.SortKey.Kind
	}
	for _, gsi := range def.GSIs {
		attrSet[gsi.KeyDefinitions.PartitionKey.Name] = gsi.KeyDefinitions.PartitionKey.Kind
		if gsi.KeyDefinitions.SortKey.Name != "" {
			attrSet[gsi.KeyDefinitions.SortKey.Name] = gsi.KeyDefinitions.SortKey.Kind
		}
	}

	var attrDefs []types.AttributeDefinition
	for name, kind := range attrSet {
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: aws.String(name),
			AttributeType: keyKindToSDKScalar(kind),
		})
	}

	// Build GSI descriptions.
	var gsiDescs []types.GlobalSecondaryIndexDescription
	for _, gsi := range def.GSIs {
		gsiKS := []types.KeySchemaElement{
			{
				AttributeName: aws.String(gsi.KeyDefinitions.PartitionKey.Name),
				KeyType:       types.KeyTypeHash,
			},
		}
		if gsi.KeyDefinitions.SortKey.Name != "" {
			gsiKS = append(gsiKS, types.KeySchemaElement{
				AttributeName: aws.String(gsi.KeyDefinitions.SortKey.Name),
				KeyType:       types.KeyTypeRange,
			})
		}
		gsiDescs = append(gsiDescs, types.GlobalSecondaryIndexDescription{
			IndexName:   aws.String(gsi.Name),
			KeySchema:   gsiKS,
			IndexStatus: types.IndexStatusActive,
			Projection: &types.Projection{
				ProjectionType: types.ProjectionTypeAll,
			},
		})
	}

	desc := types.TableDescription{
		TableName:            aws.String(def.Name),
		TableStatus:          types.TableStatusActive,
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		BillingModeSummary: &types.BillingModeSummary{
			BillingMode: types.BillingModePayPerRequest,
		},
	}
	if len(gsiDescs) > 0 {
		desc.GlobalSecondaryIndexes = gsiDescs
	}

	return desc
}

// keyKindToSDKScalar converts the internal KeyKind to an AWS SDK ScalarAttributeType.
func keyKindToSDKScalar(kind table.KeyKind) types.ScalarAttributeType {
	switch kind {
	case table.KeyKindS:
		return types.ScalarAttributeTypeS
	case table.KeyKindN:
		return types.ScalarAttributeTypeN
	case table.KeyKindB:
		return types.ScalarAttributeTypeB
	default:
		return types.ScalarAttributeTypeS
	}
}
