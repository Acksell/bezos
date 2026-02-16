package ddbsdk

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoKeyValue constrains the types that can be used as DynamoDB key values.
// This prevents passing structs or other invalid types as partition keys.
type DynamoKeyValue interface {
	~string | ~int | ~int64 | ~float64 | ~[]byte
}

// QueryDef itself is a QueryBuilder, so you can:
// #1 Pass a QueryDef{} struct literal directly.
//
//	db.NewQuery(ddbsdk.QueryDef{
//	   Table:    usersTable,
//	   Partition: "USER#Ninja123",
//	   SortKey:  ddbsdk.BeginsWith("PROFILE#"),
//	}).Descending().PageSize(20)
//
// #2 Use [QueryPartition] and chain [QueryDef.OnIndex] and [QueryDef.WithSKCondition]:
//
//	// Create a QueryDef for partition key "USER#Ninja123"
//	q := QueryPartition(usersTable, "USER#Ninja123").
//		OnIndex("GSI1").
//		WithSKCondition(BeginsWith("PROFILE#"))
//	db.NewQuery(q).Descending().PageSize(20)
//
// #3 Use an external library that builds queries.
//
// For example, using code generated from ddbgen:
//
//	q := UserIndex.QueryPartition("Ninja123").ProfileBeginsWith("")
//	db.NewQuery(q).Descending().PageSize(20)
type QueryBuilder interface {
	Build() QueryDef
}

// QueryDef defines a query against a DynamoDB table or GSI.
// It holds the table, optional index name, partition key value, and optional sort key condition.
//
// Use [QueryPartition] to create a QueryDef, then optionally chain [QueryDef.OnIndex]
// and [QueryDef.WithSKCondition] to configure the query.
//
// Typically, generated code wraps these calls to provide type-safe, format-aware query methods.
//
// Fields are exported to allow struct literal initialization as an escape hatch.
type QueryDef struct {
	Table     table.TableDefinition
	IndexName *string
	Partition any
	SortKey   SortKeyCondition
}

// Build returns itself, implementing [QueryBuilder].
func (qd QueryDef) Build() QueryDef { return qd }

// QueryPartition creates a QueryDef for a partition key query on the given table.
// The partition value is constrained to valid DynamoDB key types.
func QueryPartition[P DynamoKeyValue](t table.TableDefinition, partition P) QueryDef {
	return QueryDef{
		Table:     t,
		Partition: partition,
	}
}

// OnIndex sets the GSI name to query. Returns a new QueryDef.
func (qd QueryDef) OnIndex(name string) QueryDef {
	qd.IndexName = &name
	return qd
}

// WithSKCondition sets the sort key condition for the query. Returns a new QueryDef.
func (qd QueryDef) WithSKCondition(sk SortKeyCondition) QueryDef {
	qd.SortKey = sk
	return qd
}

// Querier executes DynamoDB queries with configurable options.
// Create with [Client.NewQuery] or [NewQuerier], then configure with method chaining.
type Querier struct {
	awsddb AWSDynamoClientV2

	queryDef QueryDef

	//internal, not exposed to user
	lastCursor map[string]types.AttributeValue

	// Options set via method chaining
	eventuallyConsistent bool
	pageSize             int32
	descending           bool
	filter               expression2.ConditionBuilder
	projectionAttributes []string
}

const defaultPageSize = 10

// NewQuerier creates a Querier from a QueryBuilder.
func NewQuerier(ddb AWSDynamoClientV2, qb QueryBuilder) *Querier {
	return &Querier{
		awsddb:   ddb,
		queryDef: qb.Build(),
		pageSize: defaultPageSize,
	}
}

// EventuallyConsistent enables eventually consistent reads.
// By default, reads are strongly consistent.
func (q *Querier) EventuallyConsistent() *Querier {
	q.eventuallyConsistent = true
	return q
}

// Descending returns results in descending sort key order.
// By default, results are returned in ascending order.
func (q *Querier) Descending() *Querier {
	q.descending = true
	return q
}

// PageSize sets the maximum number of items to return per page.
// Default is 10.
func (q *Querier) PageSize(limit int) *Querier {
	q.pageSize = int32(limit)
	return q
}

// Projection limits the attributes returned in the response.
// Only the specified attributes will be retrieved from DynamoDB.
func (q *Querier) Projection(attrs ...string) *Querier {
	q.projectionAttributes = attrs
	return q
}

// Filter applies a filter expression to the query results.
// Filter expressions are applied after the query but before results are returned.
// Note: filtered items still consume read capacity.
func (q *Querier) Filter(filter expression2.ConditionBuilder) *Querier {
	q.filter = filter
	return q
}

// QueryResult holds the results of a query page.
type QueryResult struct {
	Items  []Item
	IsDone bool
}

func (q *Querier) Next(ctx context.Context) (*QueryResult, error) {
	b := expression2.NewBuilder()

	// Get the appropriate key definitions (GSI or primary table)
	keyDef := q.queryDef.Table.KeyDefinitions
	if q.queryDef.IndexName != nil {
		for _, gsi := range q.queryDef.Table.GSIs {
			if gsi.Name == *q.queryDef.IndexName {
				keyDef = gsi.KeyDefinitions
				break
			}
		}
	}

	pkName := keyDef.PartitionKey.Name
	key := expression2.KeyEqual(expression2.Key(pkName), expression2.Value(q.queryDef.Partition))
	if q.queryDef.SortKey != nil {
		skName := keyDef.SortKey.Name
		key = key.And(q.queryDef.SortKey(skName))
	}
	b = b.WithKeyCondition(key)

	if q.filter.IsSet() {
		b = b.WithFilter(q.filter)
	}

	if len(q.projectionAttributes) > 0 {
		var proj expression2.ProjectionBuilder
		for i, attr := range q.projectionAttributes {
			if i == 0 {
				proj = expression2.NamesList(expression2.Name(attr))
			} else {
				proj = proj.AddNames(expression2.Name(attr))
			}
		}
		b = b.WithProjection(proj)
	}

	expr, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query expression: %w", err)
	}

	res, err := q.awsddb.Query(ctx, &dynamodbv2.QueryInput{
		TableName:                 &q.queryDef.Table.Name,
		IndexName:                 q.queryDef.IndexName,
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConsistentRead:            ptr(!q.eventuallyConsistent),
		Limit:                     ptr(q.pageSize),
		ScanIndexForward:          ptr(!q.descending),
		ExclusiveStartKey:         q.lastCursor,
	})
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	q.lastCursor = res.LastEvaluatedKey
	return &QueryResult{
		Items:  res.Items,
		IsDone: res.LastEvaluatedKey == nil,
	}, nil
}

func (q *Querier) QueryAll(ctx context.Context) (*QueryResult, error) {
	var allItems []Item
	for {
		res, err := q.Next(ctx)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, res.Items...)
		if res.IsDone {
			break
		}
	}
	return &QueryResult{
		Items:  allItems,
		IsDone: true,
	}, nil
}
