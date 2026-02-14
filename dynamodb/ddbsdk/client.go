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

func (c *Client) NewQuery(table table.TableDefinition, kc KeyCondition) Querier {
	return NewQuerier(c.awsddb, table, kc)
}

// NewLookup creates a new getter for direct lookups by primary key.
//
// Options: [WithEventuallyConsistentReads]
func (c *Client) NewLookup(opts ...GetOption) Getter {
	return &getter{
		awsddb: c.awsddb,
	}
}
