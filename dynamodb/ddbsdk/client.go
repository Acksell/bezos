package ddbsdk

import (
	"bezos/dynamodb/table"
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

func (c *Client) NewQuery(table table.TableDefinition, kc KeyCondition, opts ...QueryOption) Querier {
	return NewQuerier(c.awsddb, table, kc, opts...)
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
}

type Txer interface {
	AddAction(context.Context, Action) error
	Commit(context.Context) error
}

type Batcher interface {
	AddAction(context.Context, Action) error
	Write(context.Context) error
}

type Reader interface {
	NewQuery(table.TableDefinition, KeyCondition, ...QueryOption) Querier
	NewGet(...GetOption) Getter
}

type Querier interface {
	Next(context.Context) (*QueryResult, error)
	QueryAll(context.Context) (*QueryResult, error)
}

type Getter interface {
	Lookup(context.Context, ItemIdentifier) (DynamoEntity, error)
	TxLookupMany(context.Context, ...ItemIdentifier) ([]DynamoEntity, error)
	BatchLookupMany(context.Context, ...ItemIdentifier) ([]DynamoEntity, error)
}
