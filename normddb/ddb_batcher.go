package normddb

import "context"

func NewBatcher(ddb AWSDynamoClientV2, opts ...BatchOption) *batcher {
	b := &batcher{
		awsddb: ddb,
	}
	for _, opt := range opts {
		opt(&b.opts)
	}
	return b
}

// todo implement
type batcher struct {
	awsddb AWSDynamoClientV2
	opts   batchOpts
}

var _ Batcher = &batcher{}

func (b *batcher) NewBatch(ctx context.Context, opts ...BatchOption) {
	//todo implement this
	return
}

func (b *batcher) AddAction(ctx context.Context, a Action) error {
	//todo implement this
	return nil
}

func (b *batcher) Write(ctx context.Context) error {
	//todo implement this
	return nil
}

type BatchOption func(*batchOpts)

type batchOpts struct {
}
