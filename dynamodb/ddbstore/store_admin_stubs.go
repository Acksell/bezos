package ddbstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DeleteTable is not yet implemented.
func (s *Store) DeleteTable(_ context.Context, _ *dynamodb.DeleteTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	return nil, fmt.Errorf("DeleteTable: not implemented")
}

// ListTables is not yet implemented.
func (s *Store) ListTables(_ context.Context, _ *dynamodb.ListTablesInput, _ ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return nil, fmt.Errorf("ListTables: not implemented")
}

// UpdateTable is not yet implemented.
func (s *Store) UpdateTable(_ context.Context, _ *dynamodb.UpdateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	return nil, fmt.Errorf("UpdateTable: not implemented")
}

// UpdateTimeToLive is not yet implemented.
func (s *Store) UpdateTimeToLive(_ context.Context, _ *dynamodb.UpdateTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error) {
	return nil, fmt.Errorf("UpdateTimeToLive: not implemented")
}
