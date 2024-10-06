package normddb

import (
	"context"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type getter struct {
	ddb *dynamodbv2.Client

	opts getOpts
}

var _ Getter = &getter{}

func NewGetter(ddb *dynamodbv2.Client) *getter {
	return &getter{
		ddb: ddb,
	}
}

type ItemIdentifier struct {
	Table string
	Key   PrimaryKey
}

func (g *getter) Lookup(ctx context.Context, item ItemIdentifier) (DynamoEntity, error) {
	//? implement this
	return nil, nil
}

func (g *getter) TxLookupMany(ctx context.Context, items ...ItemIdentifier) ([]DynamoEntity, error) {
	//? implement this
	return nil, nil
}

func (g *getter) BatchLookupMany(ctx context.Context, items ...ItemIdentifier) ([]DynamoEntity, error) {
	//? implement this
	return nil, nil
}

// todo
// ?figure out if GetOption applies equally to Lookup, BatchLookupMany, AND TxLookupMany.
// ?Are there options that only apply for one of them? If so, then maybe this grouping doesn't make sense and a BatchReader and TxReader makes more sense?
type GetOption func(*getOpts)

type getOpts struct {
}
