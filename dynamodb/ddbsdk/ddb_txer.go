package ddbsdk

import (
	"context"
	"errors"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// actionKey uniquely identifies an item across tables
type actionKey struct {
	tableName  string
	primaryKey table.PrimaryKey
}

func NewTx(ddb AWSDynamoClientV2, opts ...TxOption) Txer {
	tx := &txer{
		awsddb:  ddb,
		actions: make(map[actionKey]Action),
	}
	for _, opt := range opts {
		opt(&tx.opts)
	}
	return tx
}

type txer struct {
	awsddb AWSDynamoClientV2

	opts txOpts

	actions map[actionKey]Action
	errs    []error // errors from AddAction, checked in Commit
}

func (tx *txer) AddAction(a Action) {
	key := actionKey{tableName: *a.TableName(), primaryKey: a.PrimaryKey()}
	if _, found := tx.actions[key]; found {
		tx.errs = append(tx.errs, fmt.Errorf("an action already exists for table %s, primary key: %v", *a.TableName(), a.PrimaryKey()))
		return
	}
	tx.actions[key] = a
}

func (tx *txer) Commit(ctx context.Context) error {
	if len(tx.errs) > 0 {
		return errors.Join(tx.errs...)
	}
	switch len(tx.actions) {
	case 0:
		return nil
	case 1:
		// use operation directly instead of TransactWriteItems, to avoid transactional overhead
		for _, update := range tx.actions {
			switch a := update.(type) {
			case PutItemAction:
				put, err := a.ToPutItem()
				if err != nil {
					return fmt.Errorf("failed to convert put to put item: %w", err)
				}
				_, err = tx.awsddb.PutItem(ctx, put)
				if err != nil {
					return fmt.Errorf("failed to put item: %w", err)
				}
			case UpdateItemAction:
				update, err := a.ToUpdateItem()
				if err != nil {
					return fmt.Errorf("failed to convert update to update item: %w", err)
				}
				_, err = tx.awsddb.UpdateItem(ctx, update)
				if err != nil {
					return fmt.Errorf("failed to update item: %w", err)
				}
			case DeleteItemAction:
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
			twi, err := update.ToTransactWriteItem()
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

type txWriteItem interface {
	ToTransactWriteItem() (types.TransactWriteItem, error)
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
