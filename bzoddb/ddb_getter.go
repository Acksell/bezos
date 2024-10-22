package bzoddb

import (
	"bezos/bzoddb/table"
	"context"
)

type getter struct {
	awsddb AWSDynamoClientV2

	opts getOpts
}

var _ Getter = &getter{}

func NewGetter(ddb AWSDynamoClientV2, opts ...GetOption) *getter {
	g := &getter{
		awsddb: ddb,
	}
	for _, opt := range opts {
		opt(&g.opts)
	}
	return g
}

type ItemIdentifier struct {
	Table string
	Key   table.PrimaryKey
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
