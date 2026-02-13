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

func (c *Client) NewTx(opts ...TxOption) Txer {
	return NewTx(c.awsddb, opts...)
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
