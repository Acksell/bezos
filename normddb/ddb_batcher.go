package normddb

import "context"

func NewBatcher() *batcher {
	return &batcher{}
}

// todo implement
type batcher struct {
	opts batchOpts
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
