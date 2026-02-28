package ddbsdk

import (
	"context"

	"github.com/acksell/bezos/dynamodb/ddbiface"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// AWSDynamoClientV2 is an alias for ddbiface.AWSDynamoClientV2.
// Deprecated: Import from github.com/acksell/bezos/dynamodb/ddbiface instead.
type AWSDynamoClientV2 = ddbiface.AWSDynamoClientV2

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
	NewQuery(QueryBuilder) *Querier
	NewLookup(...GetOption) Getter
}

type Txer interface {
	AddAction(...Action)
	Commit(context.Context) error
}

type Batcher interface {
	// AddAction appends one or more actions to the batch.
	// Batch only supports Put and Delete actions, without conditions.
	AddAction(...BatchAction)
	Exec(context.Context) (ExecResult, error)
	ExecAndRetry(context.Context) error
}

// ConsistentReads are enabled by default.
// To use EventuallyConsistent reads, add the WithEventualConsistency option.
type Getter interface {
	// GetItem retrieves a single item from DynamoDB.
	// Serializable isolation.
	GetItem(context.Context, GetItemRequest) (Item, error)
	// GetItemsTx retrieves multiple items.
	// Serializable isolation.
	// Maximum 100 items per transaction (DynamoDB limit).
	// Each item can have its own projection since items may have different schemas.
	GetItemsTx(context.Context, ...GetItemRequest) ([]Item, error)
	// GetItemsBatch retrieves multiple items using BatchGetItem.
	//
	// As a batch unit, not serializable isolation. Only read-committed isolation.
	// On a per-item basis, it is serializable.
	// Translation:
	// If there's a concurrent transaction write request in-flight,
	// it's possible that you'll be able to read the new state of
	// some of the items and the old state of the other items.
	// If you need better isolation guarantees, use GetItemsTx.
	//
	// Maximum 100 items per batch (DynamoDB limit).
	GetItemsBatch(context.Context, ...GetItemRequest) ([]Item, error)
}

// Item represents a raw DynamoDB item as returned from Get operations.
// Callers should use attributevalue.UnmarshalMap to convert to their struct.
type Item = map[string]types.AttributeValue
