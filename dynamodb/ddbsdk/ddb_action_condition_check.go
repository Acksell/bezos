package ddbsdk

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NewConditionCheck creates a ConditionCheck action.
// A ConditionCheck asserts a condition on an existing item without modifying it.
// It can only be used within a transaction (TransactWriteItems).
//
// The condition is required — a ConditionCheck without a condition is meaningless.
func NewConditionCheck(table table.TableDefinition, pk table.PrimaryKey, condition expression2.ConditionBuilder) *ConditionCheck {
	return &ConditionCheck{
		Table: table,
		Key:   pk,
		c:     condition,
	}
}

func (cc *ConditionCheck) TableName() *string {
	return &cc.Table.Name
}

func (cc *ConditionCheck) PrimaryKey() table.PrimaryKey {
	return cc.Key
}

// WithCondition adds an additional condition expression (AND).
func (cc *ConditionCheck) WithCondition(c expression2.ConditionBuilder) *ConditionCheck {
	cc.c = cc.c.And(c)
	return cc
}

func (cc *ConditionCheck) Build() (expression2.Expression, error) {
	b := expression2.NewBuilder().WithCondition(cc.c)
	e, err := b.Build()
	if err != nil {
		return expression2.Expression{}, fmt.Errorf("build: %w", err)
	}
	return e, nil
}

func (cc *ConditionCheck) ToTransactWriteItem() (types.TransactWriteItem, error) {
	e, err := cc.Build()
	if err != nil {
		return types.TransactWriteItem{}, fmt.Errorf("failed to build condition check: %w", err)
	}
	return types.TransactWriteItem{
		ConditionCheck: &types.ConditionCheck{
			TableName:                 cc.TableName(),
			Key:                       cc.PrimaryKey().DDB(),
			ConditionExpression:       e.Condition(),
			ExpressionAttributeValues: e.Values(),
			ExpressionAttributeNames:  e.Names(),
		},
	}, nil
}
