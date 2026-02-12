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

func New(awsddb AWSDynamoClientV2) IO {
	return &Client{
		awsddb: awsddb,
	}
}

type Client struct {
	awsddb AWSDynamoClientV2
}

var _ IO = &Client{}

func (c *Client) NewTx(opts ...TxOption) Txer {
	return NewTxer(c.awsddb, opts...)
}

func (c *Client) NewBatch(opts ...BatchOption) Batcher {
	return NewBatcher(c.awsddb, opts...)
}

func (c *Client) NewQuery(table table.TableDefinition, kc KeyCondition) Querier {
	return NewQuerier(c.awsddb, table, kc)
}

func (c *Client) NewGet(opts ...GetOption) Getter {
	return &getter{
		awsddb: c.awsddb,
	}
}

type IO interface {
	Writer
	Reader
}

type Writer interface {
	NewTx(...TxOption) Txer
	NewBatch(...BatchOption) Batcher

	PutItem(context.Context, *Put) error
	UpdateItem(context.Context, *UnsafeUpdate) error
	DeleteItem(context.Context, *Delete) error
}

type Txer interface {
	AddAction(Action)
	Commit(context.Context) error
}

type Batcher interface {
	AddAction(context.Context, Action) error
	Write(context.Context) error
}

type Reader interface {
	NewQuery(table.TableDefinition, KeyCondition) Querier
	NewGet(...GetOption) Getter
}

type Querier interface {
	Next(context.Context) (*QueryResult, error)
	QueryAll(context.Context) (*QueryResult, error)
}

// Item represents a raw DynamoDB item as returned from Get operations.
// Callers should use attributevalue.UnmarshalMap to convert to their struct.
type Item = map[string]types.AttributeValue

type Getter interface {
	GetItem(context.Context, LookupItem) (Item, error)
	GetItemsTx(context.Context, ...LookupItem) ([]Item, error)
	GetItemsBatch(context.Context, ...LookupItem) ([]Item, error)
}
