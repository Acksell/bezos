package ddbsdk

import (
	"bezos/dynamodb/table"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// type QueryPattern string

// const (
// 	PartitionKeyPagination = "pkPagination"
// 	SortedPagination       = "skPagination"
// 	DirectLookup           = "directLookup"
// )

type querier struct {
	awsddb AWSDynamoClientV2

	table   table.TableDefinition
	keyCond KeyCondition

	//internal, not exposed to user
	lastCursor map[string]types.AttributeValue

	opts queryOptions
}

type queryOptions struct {
	// default to consistent reads
	// because if you don't know what you're doing you may introduce race conditions.
	eventuallyConsistent bool
	pageSize             int32
	descending           bool
	indexName            *string
	filter               expression2.ConditionBuilder
}

const defaultPageSize = 10

type KeyCondition struct {
	partition any
	strategy  SortKeyStrategy
}

func NewKeyCondition(partition any, strategy SortKeyStrategy) KeyCondition {
	return KeyCondition{
		partition: partition,
		strategy:  strategy,
	}
}

// todo make first two arguments here part of the dynamodb client interface. ddb Clients should be able to return a Querier and a Getter.
//
// todo: replace TableDefinition argument with "Index" interface that has "Table()" method, and "IndexName()" method.
// The base table can export methods like "Primary()", "GSI_1()" that return the index.
// Then you can call them via "artifacts.Table.GSI_1()"
func NewQuerier(ddb AWSDynamoClientV2, table table.TableDefinition, kc KeyCondition, opts ...QueryOption) *querier {
	q := &querier{
		awsddb:  ddb,
		table:   table,
		keyCond: kc,
		opts: queryOptions{
			pageSize: defaultPageSize,
		},
	}
	for _, opt := range opts {
		opt(&q.opts)
	}
	return q
}

type QueryResult struct {
	Entities []DynamoEntity
	IsDone   bool
}

func (q *querier) Next(ctx context.Context) (*QueryResult, error) {
	b := expression2.NewBuilder()
	key := expression2.KeyEqual(expression2.Key(q.table.KeyDefinitions.PartitionKey.Name), expression2.Value(q.keyCond.partition))
	if q.keyCond.strategy != nil {
		key.And(q.keyCond.strategy(q.table.KeyDefinitions.SortKey.Name))
	}
	b.WithKeyCondition(key)

	if q.opts.filter.IsSet() {
		b = b.WithFilter(q.opts.filter)
	}

	expr, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression: %w", err)
	}

	res, err := q.awsddb.Query(ctx, &dynamodbv2.QueryInput{
		TableName:                 &q.table.Name,
		IndexName:                 q.opts.indexName,
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(), // todo implement
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConsistentRead:            ptr(!q.opts.eventuallyConsistent),
		Limit:                     ptr(q.opts.pageSize),
		ScanIndexForward:          ptr(q.opts.descending),
		ExclusiveStartKey:         q.lastCursor,
	})
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	out := make([]DynamoEntity, 0, len(res.Items))
	for _, v := range res.Items {
		var e DynamoEntity
		if err := attributevalue.UnmarshalMap(v, &e); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entity: %w", err)
		}
		out = append(out, e)
	}

	q.lastCursor = res.LastEvaluatedKey
	return &QueryResult{
		Entities: out,
		IsDone:   res.LastEvaluatedKey == nil,
	}, nil
}

func (q *querier) QueryAll(ctx context.Context) (*QueryResult, error) {
	//? implement this
	return nil, nil
}

type QueryOption func(*queryOptions)

func (q *querier) WithEventuallyConsistentReads() *querier {
	q.opts.eventuallyConsistent = true
	return q
}

func (q *querier) WithDescending() *querier {
	q.opts.descending = true
	return q
}

func (q *querier) WithPageSize(limit int) *querier {
	q.opts.pageSize = int32(limit)
	return q
}

func (q *querier) WithGSI(indexName string) *querier {
	q.opts.indexName = &indexName
	return q
}

// Filter based on entity type.
//
// todo queries on indices should pass entity filters by default to its query constructor
// todo add more filters
func (q *querier) WithEntityFilter(typ string) *querier {
	q.opts.filter = q.opts.filter.And(expression2.Equal(expression2.Name("meta.type"), expression2.Value(typ)))
	return q
}

var Table = table.TableDefinition{
	Name: "test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	TimeToLiveKey: "ttl",
	GSIs: []table.TableDefinition{
		{
			Name: "byName",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "name", Kind: table.KeyKindS},
			},
		},
	},
}

func _() {
	var ddb *dynamodbv2.Client

	q := NewQuerier(ddb, Table, NewKeyCondition("state#123", BeginsWith("lol"))).
		WithDescending().
		WithPageSize(10).
		WithEntityFilter("ExampleEntity")

	ctx := context.Background()
	res, err := q.Next(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(res.Entities))
}

type SortKeyStrategy func(skName string) expression2.KeyConditionBuilder

// Equals signals that all documents that are equal to the provided key's sort value should
// be returned.
func Equals[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyEqual(expression2.Key(skName), expression2.Value(v))
	}
}

// BeginsWith signals that all documents that start with the provided key's sort value should
// be returned.
func BeginsWith(prefix string) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyBeginsWith(expression2.Key(skName), prefix)
	}
}

// Between signals that all documents with a sort value between the two sort values of the provided
// keys should be used.
func BetweenQuery[T any](start, end T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyBetween(
			expression2.Key(skName),
			expression2.Value(start),
			expression2.Value(end),
		)
	}
}

func GreaterThanQuery[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyGreaterThan(expression2.Key(skName), expression2.Value(v))
	}
}

func GreaterThanEqualQuery[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyGreaterThanEqual(expression2.Key(skName), expression2.Value(v))
	}
}

func KeyLessThanQuery[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyLessThan(expression2.Key(skName), expression2.Value(v))
	}
}

func KeyLessThanEqualQuery[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyLessThanEqual(expression2.Key(skName), expression2.Value(v))
	}
}

func ptr[T any](v T) *T {
	return &v
}
