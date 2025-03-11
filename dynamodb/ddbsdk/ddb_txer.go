package bzoddb

import (
	"bezos/dynamodb/table"
	"context"
	"fmt"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewTxer(ddb AWSDynamoClientV2, opts ...TxOption) Txer {
	tx := &txer{
		awsddb:  ddb,
		actions: make(map[string]map[table.PrimaryKey]Action),
	}
	for _, opt := range opts {
		opt(&tx.opts)
	}
	return tx
}

type txer struct {
	awsddb AWSDynamoClientV2

	opts txOpts

	// errors from AddAction can be returned when calling Commit().
	// This is to enable a nicer API where you don't have to check for errors after each AddAction call.
	errs     []error
	nactions int
	// todo scope per table as well
	// Lookup of all actions per table and item.
	// Only one action per item is allowed in a transaction.
	actions map[string]map[table.PrimaryKey]Action
}

func (tx *txer) addError(err error) error {
	tx.errs = append(tx.errs, err)
	return fmt.Errorf("failed to get primary key: %w", err)
}

// AddAction stages the action for the commit.
// Handling the error is optional, the call to Commit() will return these errors later.
func (tx *txer) AddAction(ctx context.Context, a Action) error {
	pk, err := a.PrimaryKey()
	if err != nil {
		return tx.addError(err)
	}
	if a.TableName() == nil {
		return tx.addError(fmt.Errorf("missing table name for action %T on pk %v", a, pk))
	}
	_, found := tx.actions[*a.TableName()]
	if !found {
		tx.actions[*a.TableName()] = make(map[table.PrimaryKey]Action)
	}
	if _, found := tx.actions[*a.TableName()][pk]; found {
		return tx.addError(fmt.Errorf("an action already exists in table %q for primary key %v", *a.TableName(), pk))
	}
	tx.nactions++
	tx.actions[*a.TableName()][pk] = a // todo test
	return nil
}

// todo: add return value
func (tx *txer) Commit(ctx context.Context) error {
	switch tx.nactions {
	case 0:
		return nil
	case 1:
		// use operation directly instead of TransactWriteItems, to avoid transactional overhead
		for _, tableActions := range tx.actions {
			for _, action := range tableActions {
				switch a := action.(type) {
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
		}
	default:
		txInputs := make([]types.TransactWriteItem, 0)
		for _, tableActions := range tx.actions {
			for _, action := range tableActions {
				twi, err := toTransactWriteItem(action)
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
