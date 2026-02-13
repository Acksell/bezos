package ddbsdk

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewDelete(table table.TableDefinition, pk table.PrimaryKey) *Delete {
	return &Delete{
		Table: table,
		Key:   pk,
	}
}

func (d *Delete) TableName() *string {
	return &d.Table.Name
}

func (d *Delete) PrimaryKey() table.PrimaryKey {
	return d.Key
}

func (d *Delete) WithCondition(c expression2.ConditionBuilder) *DeleteWithCondition {
	d.c = c
	return &DeleteWithCondition{del: d}
}

func (d *Delete) Build() (expression2.Expression, error) {
	b := expression2.NewBuilder()
	if d.c.IsSet() {
		b = b.WithCondition(d.c)
	}
	e, err := b.Build()
	if err != nil {
		return expression2.Expression{}, fmt.Errorf("build: %w", err)
	}
	return e, nil
}

func (d *Delete) ToDeleteItem() (*dynamodbv2.DeleteItemInput, error) {
	e, err := d.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build delete: %w", err)
	}
	return &dynamodbv2.DeleteItemInput{
		TableName:                 d.TableName(),
		Key:                       d.PrimaryKey().DDB(),
		ConditionExpression:       e.Condition(),
		ExpressionAttributeValues: e.Values(),
		ExpressionAttributeNames:  e.Names(),
	}, nil
}

func (d *Delete) ToTransactWriteItem() (types.TransactWriteItem, error) {
	e, err := d.Build()
	if err != nil {
		return types.TransactWriteItem{}, fmt.Errorf("failed to build delete: %w", err)
	}
	return types.TransactWriteItem{
		Delete: &types.Delete{
			TableName:                 d.TableName(),
			Key:                       d.PrimaryKey().DDB(),
			ConditionExpression:       e.Condition(),
			ExpressionAttributeValues: e.Values(),
			ExpressionAttributeNames:  e.Names(),
		},
	}, nil
}

// batchWritable implements BatchAction.
func (d *Delete) batchWritable() {}

// ToBatchWriteRequest converts the Delete to a WriteRequest for BatchWriteItem.
func (d *Delete) ToBatchWriteRequest() (types.WriteRequest, error) {
	return types.WriteRequest{
		DeleteRequest: &types.DeleteRequest{
			Key: d.PrimaryKey().DDB(),
		},
	}, nil
}

// DeleteWithCondition methods - delegates to the underlying Delete

func (d *DeleteWithCondition) TableName() *string {
	return d.del.TableName()
}

func (d *DeleteWithCondition) PrimaryKey() table.PrimaryKey {
	return d.del.PrimaryKey()
}

// WithCondition adds an additional condition expression (AND).
func (d *DeleteWithCondition) WithCondition(c expression2.ConditionBuilder) *DeleteWithCondition {
	if d.del.c.IsSet() {
		d.del.c = d.del.c.And(c)
	} else {
		d.del.c = c
	}
	return d
}

func (d *DeleteWithCondition) Build() (expression2.Expression, error) {
	return d.del.Build()
}

func (d *DeleteWithCondition) ToDeleteItem() (*dynamodbv2.DeleteItemInput, error) {
	return d.del.ToDeleteItem()
}

func (d *DeleteWithCondition) ToTransactWriteItem() (types.TransactWriteItem, error) {
	return d.del.ToTransactWriteItem()
}
