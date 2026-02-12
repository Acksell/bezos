package ddbsdk

import (
	"context"
	"fmt"
	"time"

	"github.com/acksell/bezos/dynamodb/table"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c *Client) UpdateItem(ctx context.Context, u *UnsafeUpdate) error {
	update, err := u.ToUpdateItem()
	if err != nil {
		return fmt.Errorf("failed to convert update to update item: %w", err)
	}
	_, err = c.awsddb.UpdateItem(ctx, update)
	if err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}
	return nil
}

func NewUnsafeUpdate(table table.TableDefinition, pk table.PrimaryKey) *UnsafeUpdate {
	return &UnsafeUpdate{
		Table: table,
		Key:   pk,
	}
}

func (u *UnsafeUpdate) TableName() *string {
	return &u.Table.Name
}

func (u *UnsafeUpdate) PrimaryKey() table.PrimaryKey {
	return u.Key
}

func (u *UnsafeUpdate) AddOp(op UpdateOp) *UnsafeUpdate {
	if u.Fields == nil {
		u.Fields = make(map[string]UpdateOp)
	}
	if _, ok := u.Fields[op.Field()]; ok {
		panic(fmt.Sprintf("adding operation: field %s already exists in update of type %T", op.Field(), op))
	}
	u.Fields[op.Field()] = op
	return u
}

func (u *UnsafeUpdate) RefreshTTL(expiry time.Time) *UnsafeUpdate {
	u.ttlExpiry = &expiry
	return u
}

func (u *UnsafeUpdate) WithCondition(c expression2.ConditionBuilder) *UnsafeUpdate {
	u.c = u.c.And(c)
	return u
}

// WithRawUpdate allows you to use dynamo expressions directly.
// However, it is recommended to use the methods provided by this package
// to ensure that the update is idempotent and conforms to the schema.
//
// Any AddOp calls should be made after this method. If you use AddOp together with this
// there may be fields that conflict. This is not surfaced until the update is built.
func (u *UnsafeUpdate) WithRawUpdate(up expression2.UpdateBuilder) *UnsafeUpdate {
	if u.Fields != nil {
		panic("cannot use WithRawUpdate after AddOp")
	}
	u.u = up
	return u
}

// WithAccidentalIdempotency allows non-idempotent updates to be executed.
// These updates are not recommended as they can lead to data inconsistencies or prevent reprocessing.
// Use this method only if you are sure that what you're doing is OK. Consider a redesign if possible.
//
// Operations that are non-idempotent, and thus can only be accidentally idempotent are:
// - AddNumberOp
// - AppendToListOp
//
// It is called AccidentalIdempotency because a ClientRequestToken could be
// used to make each request idempotent, but they only last 10 minutes in Dynamo.
// Hence if you are using an idempotency token that is valid longer than that,
// for example an event ID, then idempotency is not guaranteed.
//
// If you're using lists, consider using sets instead if possible.
// If you're using counters, consider recording the unique increments instead.
func (u *UnsafeUpdate) WithAccidentalIdempotency() *UnsafeUpdate {
	u.allowNonIdempotent = true
	return u
}

func (u *UnsafeUpdate) Build() (expression2.Expression, error) {
	if u.ttlExpiry != nil {
		// todo inject time.Now dependency instead
		u.u = u.u.Set(expression2.Name(u.Table.TimeToLiveKey), expression2.Value(ttlDDB(*u.ttlExpiry)))
	}
	if u.Fields != nil {
		for _, op := range u.Fields {
			if !u.allowNonIdempotent && !op.IsIdempotent() {
				return expression2.Expression{}, fmt.Errorf("can't apply non-idempotent operation unless explicitly allowed: %T", op)
			}
			u.u = op.Apply(u.u)
		}
	}
	b := expression2.NewBuilder()
	b.WithCondition(u.c)
	b.WithUpdate(u.u)
	e, err := b.Build()
	if err != nil {
		return expression2.Expression{}, fmt.Errorf("build: %w", err)
	}
	return e, nil
}

func (u *UnsafeUpdate) ToUpdateItem() (*dynamodbv2.UpdateItemInput, error) {
	e, err := u.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build update: %w", err)
	}
	return &dynamodbv2.UpdateItemInput{
		TableName:                 u.TableName(),
		Key:                       u.PrimaryKey().DDB(),
		UpdateExpression:          e.Update(),
		ConditionExpression:       e.Condition(),
		ExpressionAttributeValues: e.Values(),
		ExpressionAttributeNames:  e.Names(),
	}, nil
}

func (u *UnsafeUpdate) ToTransactWriteItem() (types.TransactWriteItem, error) {
	e, err := u.Build()
	if err != nil {
		return types.TransactWriteItem{}, fmt.Errorf("failed to build update: %w", err)
	}
	return types.TransactWriteItem{
		Update: &types.Update{
			TableName:                 u.TableName(),
			Key:                       u.PrimaryKey().DDB(),
			UpdateExpression:          e.Update(),
			ConditionExpression:       e.Condition(),
			ExpressionAttributeValues: e.Values(),
			ExpressionAttributeNames:  e.Names(),
		},
	}, nil
}
