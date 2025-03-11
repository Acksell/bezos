package bzoddb

import (
	"bezos/dynamodb/table"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func newPut(index table.PrimaryIndexDefinition, e DynamoEntity) *Put {
	return &Put{
		Index:  index,
		Entity: e,
	}
}

func (p *Put) TableName() *string {
	return &p.Index.Table.Name
}

func (p *Put) toDoc() (map[string]types.AttributeValue, error) {
	if p.doc != nil {
		return p.doc, nil
	}
	doc, err := attributevalue.MarshalMap(p.Entity)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal entity to dynamodb map: %w", err)
	}
	p.doc = doc
	return doc, nil
}

func (p *Put) PrimaryKey() (table.PrimaryKey, error) {
	doc, err := p.toDoc()
	if err != nil {
		return table.PrimaryKey{}, fmt.Errorf("failed to marshal entity to dynamodb map: %w", err)
	}
	return p.Index.PrimaryKey(doc)
}

func (p *Put) WithTTL(expiry time.Time) *Put {
	p.ttlExpiry = &expiry
	return p
}

func (p *Put) WithCondition(c expression2.ConditionBuilder) *Put {
	if p.c.IsSet() {
		p.c = p.c.And(c)
		return p
	}
	p.c = c
	return p
}

func (p *Put) Build() (expression2.Expression, map[string]types.AttributeValue, error) {
	if err := p.Entity.IsValid(); err != nil {
		return expression2.Expression{}, nil, fmt.Errorf("entity %q is not valid: %w", p.Entity.GetID(), err)
	}
	doc, err := p.toDoc()
	if err != nil {
		return expression2.Expression{}, nil, fmt.Errorf("convert entity to ddb doc: %w", err)
	}
	b := expression2.NewBuilder()
	b.WithCondition(p.c)
	if p.ttlExpiry != nil {
		doc[p.Index.Table.TimeToLiveKey] = ttlDDB(*p.ttlExpiry)
	}
	pk, err := p.Index.PrimaryKey(doc)
	if err != nil {
		return expression2.Expression{}, nil, fmt.Errorf("failed to get primary key: %w", err)
	}
	key := pk.DDB()
	for k, v := range key {
		if _, ok := doc[k]; ok {
			return expression2.Expression{}, nil, fmt.Errorf("key attribute %q is already in the entity document", k)
		}
		doc[k] = v
	}
	for _, secondIdx := range p.Index.Table.Projections {
		secondPk, err := secondIdx.PrimaryKey(doc)
		if err != nil {
			return expression2.Expression{}, nil, fmt.Errorf("failed to get secondary index primary key: %w", err)
		}
		for k, v := range secondPk.DDB() {
			if _, ok := doc[k]; ok {
				return expression2.Expression{}, nil, fmt.Errorf("key attribute %q is already in the entity document", k)
			}
			doc[k] = v
		}
	}
	// for _, k := range p.Table.GSIKeys() {
	// 	entity[k.Names.PartitionKeyName] = &types.AttributeValueMemberS{Value: k.Values.PartitionKey}
	// 	entity[k.Names.SortKeyName] = &types.AttributeValueMemberS{Value: k.Values.SortKey}
	// }
	exp, err := b.Build()
	if err != nil {
		return expression2.Expression{}, nil, fmt.Errorf("build: %w", err)
	}
	return exp, doc, nil
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
