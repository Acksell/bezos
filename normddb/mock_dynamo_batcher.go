package normddb

import "context"

type mockBatcher struct {
	opts  batchOpts
	store *mockStore
}

var _ Batcher = &mockBatcher{}

func (db *mockBatcher) AddAction(ctx context.Context, a Action) error {
	// todo implement this
	return nil
}

func (db *mockBatcher) Write(ctx context.Context) error {
	//todo implement this
	return nil
}
