package ddbsdk

import (
	"context"
	"errors"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewTxer(ddb AWSDynamoClientV2, opts ...TxOption) Txer {
	tx := &txer{
		awsddb:  ddb,
		actions: make(map[table.PrimaryKey]Action),
	}
	for _, opt := range opts {
		opt(&tx.opts)
	}
	return tx
}

type txer struct {
	awsddb AWSDynamoClientV2

	opts txOpts

	actions map[table.PrimaryKey]Action
	errs    []error // errors from AddAction, checked in Commit

	// once stackCounter goes from 1 to 0, the transaction is committed to database.
	// incremented on Start(), decremented on Commit()
	stackCounter int // todo remove stackcounter implementation
}

// todo remove Start function and stackcounter implementation
func (tx *txer) Start(ctx context.Context, opts ...TxOption) {
	for _, opt := range opts {
		opt(&tx.opts)
	}
	tx.stackCounter++
}

func (tx *txer) AddAction(a Action) {
	if _, found := tx.actions[a.PrimaryKey()]; found {
		tx.errs = append(tx.errs, fmt.Errorf("an action already exists for primary key: %v", a.PrimaryKey()))
		return
	}
	tx.actions[a.PrimaryKey()] = a
}

// If a transaction already started in this context, commit in this context does nothing, since it's not the outer-most transaction.
// todo: add return value
func (tx *txer) Commit(ctx context.Context) error {
	if len(tx.errs) > 0 {
		return errors.Join(tx.errs...)
	}
	tx.stackCounter--
	if tx.stackCounter < 0 {
		return fmt.Errorf("too many commits, no started transaction")
	}
	if tx.stackCounter != 0 {
		return nil
	}
	switch len(tx.actions) {
	case 0:
		return nil
	case 1:
		// use operation directly instead of TransactWriteItems, to avoid transactional overhead
		for _, update := range tx.actions {
			switch a := update.(type) {
			case *Put:
				put, err := a.ToPutItem()
				if err != nil {
					return fmt.Errorf("failed to convert put to put item: %w", err)
				}
				_, err = tx.awsddb.PutItem(ctx, put)
				if err != nil {
					return fmt.Errorf("failed to put item: %w", err)
				}
			case *UnsafeUpdate:
				update, err := a.ToUpdateItem()
				if err != nil {
					return fmt.Errorf("failed to convert update to update item: %w", err)
				}
				_, err = tx.awsddb.UpdateItem(ctx, update)
				if err != nil {
					return fmt.Errorf("failed to update item: %w", err)
				}
			case *Delete:
				delete, err := a.ToDeleteItem()
				if err != nil {
					return fmt.Errorf("failed to convert delete to delete item: %w", err)
				}
				_, err = tx.awsddb.DeleteItem(ctx, delete)
				if err != nil {
					return fmt.Errorf("failed to delete item: %w", err)
				}
			default:
				return fmt.Errorf("unknown operation type: %T", a)
			}
		}
	default:
		txInputs := make([]types.TransactWriteItem, 0)
		for _, update := range tx.actions {
			twi, err := toTransactWriteItem(update)
			if err != nil {
				return fmt.Errorf("failed to convert action to transact write item: %w", err)
			}
			txInputs = append(txInputs, twi)
		}
		params := &dynamodbv2.TransactWriteItemsInput{
			TransactItems:      txInputs,
			ClientRequestToken: &tx.opts.idempotencyToken,
		}
		_, err := tx.awsddb.TransactWriteItems(ctx, params)
		if err != nil {
			return fmt.Errorf("failed to transact write items: %w", err)
		}
	}
	return nil
}

// no point to extract into the interface and pollute the public interface. Doesn't save much readability.
func toTransactWriteItem(action Action) (types.TransactWriteItem, error) {
	switch a := action.(type) {
	case *Put:
		return a.ToTransactWriteItem()
	case *UnsafeUpdate:
		return a.ToTransactWriteItem()
	case *Delete:
		return a.ToTransactWriteItem()
	default:
		return types.TransactWriteItem{}, fmt.Errorf("unknown operation type: %T", a)
	}
}

type TxOption func(*txOpts) *txOpts

type txOpts struct {
	idempotencyToken string
}

// IdempotencyTokens last for 10 minutes according to AWS documentation.
// If used after that, the request will be treated as new.
// Therefore, use with care.
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
func WithIdempotencyToken(token string) TxOption {
	return func(opts *txOpts) *txOpts {
		opts.idempotencyToken = token
		return opts
	}
}
