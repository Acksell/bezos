package normddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type QueryPattern string

const (
	PartitionKeyPagination = "pkPagination"
	SortedPagination       = "skPagination"
	DirectLookup           = "directLookup"
)

type Getter struct {
	ddb *dynamodbv2.Client
}

func NewGet(ddb *dynamodbv2.Client, table TableDefinition, pk PrimaryKey) *Getter {
	return &Getter{
		ddb: ddb,
	}
}

func (g *Getter) Lookup(ctx context.Context) (DynamoEntity, error) {
	//? implement this
	return nil, nil
}

func (g *Getter) LookupMany(ctx context.Context) (DynamoEntity, error) {
	//? implement this
	return nil, nil
}

type Querier struct {
	ddb *dynamodbv2.Client //? separate the client from this type and instead use different custom ddb client?

	table       TableDefinition
	startCursor Cursor

	//internal, not exposed to user
	lastCursor map[string]types.AttributeValue

	opts *queryOptions
}

type queryOptions struct {
	// default to consistent reads
	// because if you don't know what you're doing you may introduce race conditions.
	eventuallyConsistent bool
	pageSize             int32
	scanForward          bool
	filter               expression2.ConditionBuilder
}

const defaultPageSize = 10

type Cursor struct {
	partition any
	strategy  SortKeyStrategy // is it really a cursor, since it doesn't change when scrolling?
}

func NewCursor(partition any, strategy SortKeyStrategy) Cursor {
	return Cursor{
		partition: partition,
		strategy:  strategy,
	}
}

// todo make first two arguments here part of the dynamodb client interface. ddb Clients should be able to return a Querier and a Getter.
func NewQuerier(ddb *dynamodbv2.Client, table TableDefinition, cursor Cursor) *Querier {
	return &Querier{
		ddb: ddb,
		opts: &queryOptions{
			pageSize: defaultPageSize,
		},
	}
}

type QueryResult struct {
	Entities []DynamoEntity
	IsDone   bool
}

func (q *Querier) Next(ctx context.Context) (*QueryResult, error) {
	b := expression.NewBuilder()
	key := expression2.KeyEqual(expression2.Key(q.table.KeyDefinitions.PartitionKey.Name), expression2.Value(q.startCursor.partition))
	if q.startCursor.strategy != nil {
		key.And(q.startCursor.strategy(q.table.KeyDefinitions.SortKey.Name))
	}
	b.WithKeyCondition(key)

	if q.opts.filter.IsSet() {
		b = b.WithFilter(q.opts.filter)
	}

	expr, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression: %w", err)
	}

	res, err := q.ddb.Query(ctx, &dynamodbv2.QueryInput{
		TableName:                 &q.table.Name,
		IndexName:                 nil, // todo, if passing an index (not a table) that is a GSI, this is populated
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(), // todo implement
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConsistentRead:            ptr(!q.opts.eventuallyConsistent),
		Limit:                     ptr(q.opts.pageSize),
		ScanIndexForward:          ptr(q.opts.scanForward),
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

func (q *Querier) QueryAll(ctx context.Context) (*QueryResult, error) {
	//? implement this
	return nil, nil
}

func (q *Querier) WithEventuallyConsistentReads() *Querier {
	q.opts.eventuallyConsistent = true
	return q
}

func (q *Querier) WithScanForward() *Querier {
	q.opts.scanForward = true
	return q
}

func (q *Querier) WithPageSize(limit int) *Querier {
	q.opts.pageSize = int32(limit)
	return q
}

// Filter based on entity type.
// queries on indices can pass entity filters by default to its query constructor
// todo add more filters
func (q *Querier) WithEntityFilter(typ string) *Querier {
	q.opts.filter = q.opts.filter.And(expression2.Equal(expression2.Name("meta.type"), expression2.Value(typ)))
	return q
}

var Table = TableDefinition{
	Name: "test-table",
	KeyDefinitions: PrimaryKeyDefinition{
		PartitionKey: KeyDef{"pk", KeyKindS},
		SortKey:      KeyDef{"sk", KeyKindS},
	},
	TimeToLiveKey: "ttl",
	GSIKeys: []PrimaryKeyDefinition{
		{
			PartitionKey: KeyDef{"gsi1pk", KeyKindS},
			SortKey:      KeyDef{"gsi1sk", KeyKindS},
		},
	},
}

func main() {
	var ddb *dynamodbv2.Client

	q := NewQuerier(ddb, Table, NewCursor("state#123", BeginsWith("lol"))).
		WithScanForward().
		WithPageSize(10).
		WithEntityFilter("exampleentity")

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
