package ddbsdk

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

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

var _ Querier = &querier{}

type queryOptions struct {
	// default to consistent reads
	// because if you don't know what you're doing you may introduce race conditions.
	eventuallyConsistent bool
	pageSize             int32
	descending           bool
	indexName            *string
	filter               expression2.ConditionBuilder
	projectionAttributes []string
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
func NewQuerier(ddb AWSDynamoClientV2, table table.TableDefinition, kc KeyCondition) *querier {
	return &querier{
		awsddb:  ddb,
		table:   table,
		keyCond: kc,
		opts: queryOptions{
			pageSize: defaultPageSize,
		},
	}
}

type QueryResult struct {
	Items  []Item
	IsDone bool
}

func (q *querier) Next(ctx context.Context) (*QueryResult, error) {
	b := expression2.NewBuilder()
	key := expression2.KeyEqual(expression2.Key(q.table.KeyDefinitions.PartitionKey.Name), expression2.Value(q.keyCond.partition))
	if q.keyCond.strategy != nil {
		key = key.And(q.keyCond.strategy(q.table.KeyDefinitions.SortKey.Name))
	}
	b = b.WithKeyCondition(key)

	if q.opts.filter.IsSet() {
		b = b.WithFilter(q.opts.filter)
	}

	if len(q.opts.projectionAttributes) > 0 {
		var proj expression2.ProjectionBuilder
		for i, attr := range q.opts.projectionAttributes {
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
		TableName:                 &q.table.Name,
		IndexName:                 q.opts.indexName,
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
		ConsistentRead:            ptr(!q.opts.eventuallyConsistent),
		Limit:                     ptr(q.opts.pageSize),
		ScanIndexForward:          ptr(!q.opts.descending),
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

func (q *querier) QueryAll(ctx context.Context) (*QueryResult, error) {
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

// WithProjection limits the attributes returned in the response.
// Only the specified attributes will be retrieved from DynamoDB.
func (q *querier) WithProjection(attrs ...string) *querier {
	q.opts.projectionAttributes = attrs
	return q
}
