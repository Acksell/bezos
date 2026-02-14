package ddbsdk

import (
	"github.com/acksell/bezos/dynamodb/table"
)

func New(awsddb AWSDynamoClientV2) IO {
	return &Client{
		awsddb: awsddb,
	}
}

type Client struct {
	awsddb AWSDynamoClientV2
}

var _ IO = &Client{}

// NewTx creates a new transaction. Add actions and commit the transaction.
func (c *Client) NewTx(opts ...TxOption) Txer {
	return NewTx(c.awsddb, opts...)
}

// NewBatch creates a new write-batch. Add actions and execute the batch writes.
func (c *Client) NewBatch(opts ...BatchOption) Batcher {
	return NewBatcher(c.awsddb, opts...)
}

// NewQuery creates a new querier for partition-based queries.
//
// Options: [WithEventuallyConsistentReads], [WithDescending], [WithPageSize], [WithGSI], [WithProjection], [WithFilter]
func (c *Client) NewQuery(table table.TableDefinition, kc KeyCondition, opts ...QueryOption) Querier {
	return NewQuerier(c.awsddb, table, kc, opts...)
}

// NewLookup creates a new getter for direct lookups by primary key.
//
// Options: [WithEventualConsistency]
func (c *Client) NewLookup(opts ...GetOption) Getter {
	return NewGetter(c.awsddb, opts...)
}
