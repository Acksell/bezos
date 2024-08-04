package normddb

import (
	"fmt"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewDelete(table TableDefinition, pk PrimaryKey) *Delete {
	return &Delete{
		Table: table,
		Key:   pk,
	}
}

func (d *Delete) TableName() *string {
	return &d.Table.Name
}

func (d *Delete) PrimaryKey() PrimaryKey {
	return d.Key
}

func (d *Delete) WithCondition(c expression2.ConditionBuilder) *Delete {
	d.c = c
	return d
}

func (d *Delete) Build() (expression2.Expression, error) {
	b := expression2.NewBuilder()
	b.WithCondition(d.c)
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
