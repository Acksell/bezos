package ddbsdk

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewBatcher(ddb AWSDynamoClientV2, opts ...BatchOption) *batcher {
	b := &batcher{
		awsddb:  ddb,
		pending: make(map[string][]types.WriteRequest),
	}
	for _, opt := range opts {
		opt(&b.opts)
	}
	// Default exponential backoff: 50ms base, 2x multiplier, 5s cap, full jitter
	if b.opts.backoff == nil {
		b.opts.backoff = DefaultBackoff
	}
	return b
}

type batcher struct {
	awsddb AWSDynamoClientV2
	opts   batchOpts

	pending map[string][]types.WriteRequest
	retries int
}

var _ Batcher = &batcher{}

// AddAction adds BatchActions (Put or Delete) to the batch.
// Returns error if an action with the same table+primarykey already exists,
// or if the action has a condition expression set.
func (b *batcher) AddAction(actions ...BatchAction) error {
	for _, a := range actions {
		tableName := *a.TableName()

		var req types.WriteRequest
		var err error
		switch act := a.(type) {
		case *Put:
			req, err = act.ToBatchWriteRequest()
		case *Delete:
			req, err = act.ToBatchWriteRequest()
		default:
			return fmt.Errorf("unsupported action type: %T", a)
		}
		if err != nil {
			return err
		}

		// Check for duplicate key
		newKey := extractKey(req)
		for _, existing := range b.pending[tableName] {
			existingKey := extractKey(existing)
			if keysEqual(newKey, existingKey) {
				return fmt.Errorf("duplicate action for table %s", tableName)
			}
		}

		b.pending[tableName] = append(b.pending[tableName], req)
	}
	return nil
}

// Exec attempts to write all pending items once (no retries).
// Returns ExecResult with any unprocessed items.
func (b *batcher) Exec(ctx context.Context) (ExecResult, error) {
	if len(b.pending) == 0 {
		return ExecResult{Retries: b.retries}, nil
	}

	res, err := b.awsddb.BatchWriteItem(ctx, &dynamodbv2.BatchWriteItemInput{
		RequestItems: b.pending,
	})
	if err != nil {
		return ExecResult{
			Unprocessed: b.pending,
			Retries:     b.retries,
		}, fmt.Errorf("batch write failed: %w", err)
	}

	b.pending = res.UnprocessedItems
	b.retries++

	return ExecResult{
		Unprocessed: b.pending,
		Retries:     b.retries,
	}, nil
}

// ExecAndRetry writes all pending items, retrying until complete or limits exceeded.
// At least one of [WithMaxRetries] or [WithTimeout] must be configured.
// Uses exponential backoff by default (50ms, 100ms, 200ms, ...), override with [WithBackoff].
//
// Example:
//
//	batch := client.NewBatch(ddbsdk.WithMaxRetries(5))
//	batch.AddAction(putUser, putOrder, deleteOldItem)
//	if err := batch.ExecAndRetry(ctx); err != nil {
//	    return err
//	}
func (b *batcher) ExecAndRetry(ctx context.Context) error {
	if b.opts.maxRetries == 0 && b.opts.timeout == 0 {
		return fmt.Errorf("ExecAndRetry requires WithMaxRetries or WithTimeout to be configured")
	}
	if b.opts.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, b.opts.timeout)
		defer cancel()
	}
	for {
		res, err := b.Exec(ctx)
		if err != nil {
			return err
		}
		if res.Done() {
			return nil
		}
		if b.opts.maxRetries > 0 && res.Retries >= b.opts.maxRetries {
			return fmt.Errorf("max retries (%d) exceeded: %d items unprocessed", b.opts.maxRetries, countRequests(b.pending))
		}
		if b.opts.backoff != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(b.opts.backoff(res.Retries)):
			}
		}
	}
}

// extractKey gets the key attributes from a WriteRequest.
func extractKey(wr types.WriteRequest) map[string]types.AttributeValue {
	if wr.PutRequest != nil {
		return wr.PutRequest.Item
	}
	if wr.DeleteRequest != nil {
		return wr.DeleteRequest.Key
	}
	return nil
}

// keysEqual checks if two key maps have the same key attribute values.
func keysEqual(a, b map[string]types.AttributeValue) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !attributeValuesEqual(av, bv) {
			return false
		}
	}
	return true
}

// attributeValuesEqual compares two AttributeValues.
func attributeValuesEqual(a, b types.AttributeValue) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		if bv, ok := b.(*types.AttributeValueMemberS); ok {
			return av.Value == bv.Value
		}
	case *types.AttributeValueMemberN:
		if bv, ok := b.(*types.AttributeValueMemberN); ok {
			return av.Value == bv.Value
		}
	case *types.AttributeValueMemberB:
		if bv, ok := b.(*types.AttributeValueMemberB); ok {
			return string(av.Value) == string(bv.Value)
		}
	}
	return false
}

func countRequests(m map[string][]types.WriteRequest) int {
	var n int
	for _, reqs := range m {
		n += len(reqs)
	}
	return n
}

// ExecResult contains the result of a Write operation.
type ExecResult struct {
	Unprocessed map[string][]types.WriteRequest
	Retries     int
}

// Done returns true if all items were successfully processed.
func (r ExecResult) Done() bool {
	return len(r.Unprocessed) == 0
}

// Err returns nil if Done(), otherwise returns an error.
func (r ExecResult) Err() error {
	if r.Done() {
		return nil
	}
	return fmt.Errorf("batch incomplete: %d items unprocessed after %d retries", countRequests(r.Unprocessed), r.Retries)
}

type BatchOption func(*batchOpts)

// BackoffFunc returns the duration to wait before retry attempt n.
type BackoffFunc func(attempt int) time.Duration

// WithMaxRetries sets the maximum number of retry attempts for [ExecAndRetry].
func WithMaxRetries(n int) BatchOption {
	return func(o *batchOpts) {
		o.maxRetries = n
	}
}

// WithTimeout sets a timeout for [ExecAndRetry].
func WithTimeout(d time.Duration) BatchOption {
	return func(o *batchOpts) {
		o.timeout = d
	}
}

// WithBackoff sets a custom backoff function for [ExecAndRetry].
func WithCustomBackoff(fn BackoffFunc) BatchOption {
	return func(o *batchOpts) {
		o.backoff = fn
	}
}

// WithExponentialBackoff sets exponential backoff for [ExecAndRetry].
// See [ExponentialBackoff] for details.
func WithExponentialBackoff(base time.Duration, multiplier float64, cap time.Duration) BatchOption {
	return WithCustomBackoff(ExponentialBackoff(base, multiplier, cap))
}

// ExponentialBackoff returns a capped exponential backoff with full jitter.
// Wait time is: rand(0, min(cap, base * multiplier^attempt))
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func ExponentialBackoff(base time.Duration, multiplier float64, cap time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		factor := 1.0
		for i := 0; i < attempt; i++ {
			factor *= multiplier
		}
		backoff := time.Duration(float64(base) * factor)
		if backoff > cap {
			backoff = cap
		}
		// Full jitter: random duration between 0 and backoff
		return time.Duration(rand.Int64N(int64(backoff)))
	}
}

// DefaultBackoff is [ExponentialBackoff] with 50ms base, 2x multiplier, 5s cap.
var DefaultBackoff = ExponentialBackoff(50*time.Millisecond, 2.0, 5*time.Second)

type batchOpts struct {
	maxRetries int
	timeout    time.Duration
	backoff    BackoffFunc
}
