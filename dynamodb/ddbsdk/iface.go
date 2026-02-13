package ddbsdk

import (
	"context"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type AWSDynamoClientV2 interface {
	BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

type IO interface {
	Writer
	Reader
}

type Writer interface {
	NewTx(...TxOption) Txer
	NewBatch(...BatchOption) Batcher

	PutItem(context.Context, PutItemAction) error
	UpdateItem(context.Context, UpdateItemAction) error
	DeleteItem(context.Context, DeleteItemAction) error
}

type Reader interface {
	NewQuery(table.TableDefinition, KeyCondition) Querier
	NewGet(...GetOption) Getter
}

type Txer interface {
	AddAction(Action)
	Commit(context.Context) error
}

type Batcher interface {
	AddAction(...BatchAction) error
	Exec(context.Context) (ExecResult, error)
	ExecAndRetry(context.Context) error
}

type Querier interface {
	Next(context.Context) (*QueryResult, error)
	QueryAll(context.Context) (*QueryResult, error)
}

type Getter interface {
	// GetItem retrieves a single item from DynamoDB.
	GetItem(context.Context, GetItemRequest) (Item, error)
	// GetItemsTx retrieves multiple items.
	// Serializable isolation.
	// Maximum 100 items per transaction (DynamoDB limit).
	// Each item can have its own projection since items may have different schemas.
	GetItemsTx(context.Context, ...GetItemRequest) ([]Item, error)
	// GetItemsBatch retrieves multiple items using BatchGetItem.
	// Not serializable isolation.
	// Handles unprocessed keys by retrying automatically.
	// Maximum 100 items per batch (DynamoDB limit).
	//
	// Note: BatchGetItem applies projection per-table, so all items from the same table
	// will use the projection from the first item encountered for that table.
	GetItemsBatch(context.Context, ...GetItemRequest) ([]Item, error)
}

// Item represents a raw DynamoDB item as returned from Get operations.
// Callers should use attributevalue.UnmarshalMap to convert to their struct.
type Item = map[string]types.AttributeValue
