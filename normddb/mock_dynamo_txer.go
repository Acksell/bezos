package normddb

import "context"

type mockTxer struct {
	opts  txOpts
	store *mockStore
}

var _ Txer = &mockTxer{}

func (db *mockTxer) AddAction(ctx context.Context, a Action) error {
	// todo implement this
	return nil
}

// Commit executes the buffered actions, and stores their result in memory.
func (db *mockTxer) Commit(ctx context.Context) error {
	//todo implement this
	return nil
}
