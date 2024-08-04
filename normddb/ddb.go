package normddb

import (
	"context"
	"fmt"
	"norm"
	"time"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Action interface {
	TableName() *string
	PrimaryKey() PrimaryKey
}

type Put struct {
	Table  TableDefinition
	Index  Index
	Entity DynamoEntity
	Key    PrimaryKey

	ttlExpiry *time.Time

	c expression2.ConditionBuilder
}

// UnsafeUpdate is called unsafe because it does not require the user to
// check the invariants of the entity they're modifying. The safety of the
// operation relies solely on the user doing careful validations before committing.
type UnsafeUpdate struct {
	Table  TableDefinition
	Key    PrimaryKey
	Fields map[string]UpdateOp

	ttlExpiry          *time.Time
	allowNonIdempotent bool

	u expression2.UpdateBuilder
	c expression2.ConditionBuilder
}

type Delete struct {
	Table TableDefinition
	Key   PrimaryKey

	c expression2.ConditionBuilder
}

type Transaction struct {
	ddb *dynamodbv2.Client //? separate the client from this type and instead use different custom ddb client?

	idempotencyToken string

	actions map[PrimaryKey]Action

	// once stackCounter goes from 1 to 0, the transaction is committed to database.
	// incremented on Start(), decremented on Commit()
	stackCounter int

	// options TransactionOptions // todo
}

// IdempotencyTokens last for 10 minutes according to AWS documentation.
// If used after that, the request will be treated as new.
// Therefore, use with care.
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
func (t *Transaction) WithIdempotencyToken(token string) *Transaction {
	if t.stackCounter > 0 {
		panic("cannot change idempotency token after transaction started")
	}
	t.idempotencyToken = token
	return t
}

type TransactionFactory struct {
	Client *dynamodbv2.Client
}

func (f *TransactionFactory) New() *Transaction {
	return NewTransaction(f.Client)
}

func NewTransaction(ddb *dynamodbv2.Client) *Transaction {
	return &Transaction{
		ddb:     ddb,
		actions: make(map[PrimaryKey]Action),
	}
}

func (t *Transaction) Start(ctx context.Context) {
	t.stackCounter++
}

func (t *Transaction) AddAction(ctx context.Context, a Action) error {
	if _, found := t.actions[a.PrimaryKey()]; found {
		//todo TEST this
		return fmt.Errorf("an action already exists for primary key: %v", a.PrimaryKey())
	}
	t.actions[a.PrimaryKey()] = a
	return nil
}

// If a transaction already started in this context, commit in this context does nothing, since it's not the outer-most transaction.
// todo: add return value
func (t *Transaction) Commit(ctx context.Context) error {
	t.stackCounter--
	if t.stackCounter < 0 {
		return fmt.Errorf("too many commits, no started transaction")
	}
	if t.stackCounter != 0 {
		return nil
	}
	switch len(t.actions) {
	case 0:
		return nil
	case 1:
		// use operation directly instead of TransactWriteItems, to avoid transactional overhead
		for _, update := range t.actions {
			switch a := update.(type) {
			case *Put:
				put, err := a.ToPutItem()
				if err != nil {
					return fmt.Errorf("failed to convert put to put item: %w", err)
				}
				_, err = t.ddb.PutItem(ctx, put)
				if err != nil {
					return fmt.Errorf("failed to put item: %w", err)
				}
			case *UnsafeUpdate:
				update, err := a.ToUpdateItem()
				if err != nil {
					return fmt.Errorf("failed to convert update to update item: %w", err)
				}
				_, err = t.ddb.UpdateItem(ctx, update)
				if err != nil {
					return fmt.Errorf("failed to update item: %w", err)
				}
			case *Delete:
				delete, err := a.ToDeleteItem()
				if err != nil {
					return fmt.Errorf("failed to convert delete to delete item: %w", err)
				}
				_, err = t.ddb.DeleteItem(ctx, delete)
				if err != nil {
					return fmt.Errorf("failed to delete item: %w", err)
				}
			default:
				return fmt.Errorf("unknown operation type: %T", a)
			}
		}
	default:
		txInputs := make([]types.TransactWriteItem, 0)
		for _, update := range t.actions {
			twi, err := toTransactWriteItem(update)
			if err != nil {
				return fmt.Errorf("failed to convert action to transact write item: %w", err)
			}
			txInputs = append(txInputs, twi)
		}
		params := &dynamodbv2.TransactWriteItemsInput{
			TransactItems:      txInputs,
			ClientRequestToken: &t.idempotencyToken,
		}
		_, err := t.ddb.TransactWriteItems(ctx, params)
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

// todo is this the spec or the runtime interface?
type DynamoEntity interface {
	norm.Entity
	SchemaName() string

	// PrimaryKey() PrimaryKey // there is no common index for all entities, except maybe the direct lookup by ID index?
	// GSIKeys() []PrimaryKey

	// DefaultTTL() time.Duration

	IsValid() error
	// Lock() LockingStrategy

	// Schema in order to validate that the field-specific operations are valid.
	// Schema() EntitySchema
}
