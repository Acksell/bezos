package ddbsdk

import (
	"fmt"
	"time"

	"github.com/acksell/bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// See NewSafePut and NewUnsafePut instead for the public API.
func newPut(table table.TableDefinition, key table.PrimaryKey, e DynamoEntity) *Put {
	return &Put{
		Table:  table,
		Key:    key,
		Entity: e,
	}
}

func (p *Put) TableName() *string {
	return &p.Table.Name
}

func (p *Put) PrimaryKey() table.PrimaryKey {
	return p.Key
}

func (p *Put) WithTTL(expiry time.Time) *Put {
	p.ttlExpiry = &expiry
	return p
}

// WithCondition adds a condition expression and returns a PutWithCondition.
// PutWithCondition cannot be used with BatchWriteItem.
func (p *Put) WithCondition(c expression2.ConditionBuilder) *PutWithCondition {
	p.c = c
	return &PutWithCondition{put: p}
}

func (p *Put) Build() (expression2.Expression, map[string]types.AttributeValue, error) {
	entity, err := attributevalue.MarshalMap(p.Entity)
	if err != nil {
		return expression2.Expression{}, nil, fmt.Errorf("failed to marshal entity to dynamodb map: %w", err)
	}
	b := expression2.NewBuilder()
	if p.c.IsSet() {
		b = b.WithCondition(p.c)
	}
	if p.ttlExpiry != nil {
		entity[p.Table.TimeToLiveKey] = ttlDDB(*p.ttlExpiry)
	}
	// for _, k := range p.Table.GSIKeys() {
	// 	entity[k.Names.PartitionKeyName] = &types.AttributeValueMemberS{Value: k.Values.PartitionKey}
	// 	entity[k.Names.SortKeyName] = &types.AttributeValueMemberS{Value: k.Values.SortKey}
	// }
	exp, err := b.Build()
	if err != nil {
		return expression2.Expression{}, nil, fmt.Errorf("build: %w", err)
	}
	return exp, entity, nil
}

func (p *Put) ToPutItem() (*dynamodbv2.PutItemInput, error) {
	e, entity, err := p.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build put: %w", err)
	}
	return &dynamodbv2.PutItemInput{
		TableName:                 p.TableName(),
		Item:                      entity,
		ConditionExpression:       e.Condition(),
		ExpressionAttributeValues: e.Values(),
		ExpressionAttributeNames:  e.Names(),
	}, nil
}

func (p *Put) ToTransactWriteItem() (types.TransactWriteItem, error) {
	e, entity, err := p.Build()
	if err != nil {
		return types.TransactWriteItem{}, fmt.Errorf("failed to build put: %w", err)
	}
	return types.TransactWriteItem{
		Put: &types.Put{
			TableName:                 p.TableName(),
			Item:                      entity,
			ConditionExpression:       e.Condition(),
			ExpressionAttributeValues: e.Values(),
			ExpressionAttributeNames:  e.Names(),
		},
	}, nil
}

// batchWritable implements BatchAction.
func (p *Put) batchWritable() {}

// ToBatchWriteRequest converts the Put to a WriteRequest for BatchWriteItem.
func (p *Put) ToBatchWriteRequest() (types.WriteRequest, error) {
	_, entity, err := p.Build()
	if err != nil {
		return types.WriteRequest{}, fmt.Errorf("failed to build put: %w", err)
	}
	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: entity,
		},
	}, nil
}

// PutWithCondition methods - delegates to the underlying Put

func (p *PutWithCondition) TableName() *string {
	return p.put.TableName()
}

func (p *PutWithCondition) PrimaryKey() table.PrimaryKey {
	return p.put.PrimaryKey()
}

func (p *PutWithCondition) WithTTL(expiry time.Time) *PutWithCondition {
	p.put.WithTTL(expiry)
	return p
}

// WithCondition adds an additional condition expression (AND).
func (p *PutWithCondition) WithCondition(c expression2.ConditionBuilder) *PutWithCondition {
	if p.put.c.IsSet() {
		p.put.c = p.put.c.And(c)
	} else {
		p.put.c = c
	}
	return p
}

func (p *PutWithCondition) Build() (expression2.Expression, map[string]types.AttributeValue, error) {
	return p.put.Build()
}

func (p *PutWithCondition) ToPutItem() (*dynamodbv2.PutItemInput, error) {
	return p.put.ToPutItem()
}

func (p *PutWithCondition) ToTransactWriteItem() (types.TransactWriteItem, error) {
	return p.put.ToTransactWriteItem()
}
