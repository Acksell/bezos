package ddbsdk

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c *Client) DeleteItem(ctx context.Context, d *Delete) error {
	del, err := d.ToDeleteItem()
	if err != nil {
		return fmt.Errorf("failed to convert delete to delete item: %w", err)
	}
	_, err = c.awsddb.DeleteItem(ctx, del)
	if err != nil {
		return fmt.Errorf("failed to delete item: %w", err)
	}
	return nil
}

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

func (d *Delete) WithCondition(c expression2.ConditionBuilder) *Delete {
	if d.c.IsSet() {
		d.c = d.c.And(c)
		return d
	}
	d.c = c
	return d
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
