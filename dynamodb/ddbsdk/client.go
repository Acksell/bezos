package ddbsdk

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

// NewQuery creates a new querier.
//
// Configure with method chaining: Descending(), PageSize(n), Projection(...), Filter(...), EventuallyConsistent().
//
// See [QueryBuilder] for how to build queries.
func (c *Client) NewQuery(qb QueryBuilder) *Querier {
	return NewQuerier(c.awsddb, qb)
}

// NewLookup creates a new getter for direct lookups by primary key.
//
// Options: [WithEventualConsistency]
func (c *Client) NewLookup(opts ...GetOption) Getter {
	return NewGetter(c.awsddb, opts...)
}
