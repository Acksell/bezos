package normddb

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// todo make private and only allow people to interact via indices?
func NewPut(table TableDescription, key PrimaryKey, e DynamoEntity) *Put {
	return &Put{
		Table:  table,
		Key:    key,
		Entity: e,
	}
}

func (p *Put) TableName() *string {
	return &p.Table.Name
}

func (p *Put) PrimaryKey() PrimaryKey {
	return p.Key
}

func (p *Put) WithTTL(expiry time.Time) *Put {
	p.ttlExpiry = &expiry
	return p
}

func (p *Put) WithCondition(c expression2.ConditionBuilder) *Put {
	p.c = p.c.And(c)
	return p
}

func (p *Put) Build() (expression2.Expression, map[string]types.AttributeValue, error) {
	entity, err := attributevalue.MarshalMap(p.Entity)
	if err != nil {
		return expression2.Expression{}, nil, fmt.Errorf("failed to marshal entity to dynamodb map: %w", err)
	}
	b := expression2.NewBuilder()
	b.WithCondition(p.c)
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
