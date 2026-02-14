package ddbsdk

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	dynamodbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type getter struct {
	awsddb AWSDynamoClientV2

	opts getOpts
}

var _ Getter = &getter{}

func NewGetter(ddb AWSDynamoClientV2, opts ...GetOption) *getter {
	g := &getter{
		awsddb: ddb,
	}
	for _, opt := range opts {
		opt(&g.opts)
	}
	return g
}

// GetItemRequest identifies an item to retrieve with optional projection.
// Projection is per-item since different items may have different schemas.
type GetItemRequest struct {
	Table      table.TableDefinition
	Key        table.PrimaryKey
	Projection []string // Optional: limits which attributes are returned
}

// GetItem retrieves a single item from DynamoDB using GetItem.
func (g *getter) GetItem(ctx context.Context, item GetItemRequest) (Item, error) {
	input := &dynamodbv2.GetItemInput{
		TableName:      &item.Table.Name,
		Key:            item.Key.DDB(),
		ConsistentRead: ptr(!g.opts.eventuallyConsistent),
	}

	if err := applyProjectionToGetInput(input, item.Projection); err != nil {
		return nil, fmt.Errorf("failed to apply projection: %w", err)
	}

	res, err := g.awsddb.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get item failed: %w", err)
	}

	if res.Item == nil {
		return nil, nil
	}

	return res.Item, nil
}

// GetItemsTx retrieves multiple items transactionally using TransactGetItems.
// All items are retrieved atomically - either all succeed or all fail.
// Maximum 100 items per transaction (DynamoDB limit).
// Each item can have its own projection since items may have different schemas.
func (g *getter) GetItemsTx(ctx context.Context, items ...GetItemRequest) ([]Item, error) {
	if len(items) == 0 {
		return nil, nil
	}

	if len(items) > 100 {
		return nil, fmt.Errorf("transact get items limited to 100 items, got %d", len(items))
	}

	transactItems := make([]types.TransactGetItem, 0, len(items))
	for _, item := range items {
		get := &types.Get{
			TableName: &item.Table.Name,
			Key:       item.Key.DDB(),
		}

		if err := applyProjectionToGet(get, item.Projection); err != nil {
			return nil, fmt.Errorf("failed to apply projection: %w", err)
		}

		transactItems = append(transactItems, types.TransactGetItem{Get: get})
	}

	res, err := g.awsddb.TransactGetItems(ctx, &dynamodbv2.TransactGetItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return nil, fmt.Errorf("transact get items failed: %w", err)
	}

	return extractItemsFromResponses(res.Responses), nil
}

// Todo1 improve retrying API
// Todo2: also improve the projection api: BatchGetItem applies projection per-table, so all items from the same table use the projection from the first item encountered for that table.
func (g *getter) GetItemsBatch(ctx context.Context, items ...GetItemRequest) ([]Item, error) {
	if len(items) == 0 {
		return nil, nil
	}

	if len(items) > 100 {
		return nil, fmt.Errorf("batch get items limited to 100 items, got %d", len(items))
	}

	requestItems, err := g.buildBatchRequestItems(items)
	if err != nil {
		return nil, err
	}

	var allItems []Item

	for len(requestItems) > 0 {
		res, err := g.awsddb.BatchGetItem(ctx, &dynamodbv2.BatchGetItemInput{
			RequestItems: requestItems,
		})
		if err != nil {
			return nil, fmt.Errorf("batch get item failed: %w", err)
		}

		for _, tableItems := range res.Responses {
			allItems = append(allItems, tableItems...)
		}

		requestItems = res.UnprocessedKeys
	}

	return allItems, nil
}

func (g *getter) buildBatchRequestItems(items []GetItemRequest) (map[string]types.KeysAndAttributes, error) {
	requestItems := make(map[string]types.KeysAndAttributes)

	for _, item := range items {
		tableName := item.Table.Name

		keysAndAttrs, exists := requestItems[tableName]
		if !exists {
			keysAndAttrs = types.KeysAndAttributes{
				ConsistentRead: ptr(!g.opts.eventuallyConsistent),
			}

			// For BatchGetItem, projection is per-table, use first item's projection
			if err := applyProjectionToKeysAndAttributes(&keysAndAttrs, item.Projection); err != nil {
				return nil, fmt.Errorf("failed to apply projection: %w", err)
			}
		}

		keysAndAttrs.Keys = append(keysAndAttrs.Keys, item.Key.DDB())
		requestItems[tableName] = keysAndAttrs
	}

	return requestItems, nil
}

func applyProjectionToGetInput(input *dynamodbv2.GetItemInput, projection []string) error {
	if len(projection) == 0 {
		return nil
	}

	expr, err := buildProjectionExpression(projection)
	if err != nil {
		return err
	}

	input.ProjectionExpression = expr.Projection()
	input.ExpressionAttributeNames = expr.Names()
	return nil
}

func applyProjectionToGet(get *types.Get, projection []string) error {
	if len(projection) == 0 {
		return nil
	}

	expr, err := buildProjectionExpression(projection)
	if err != nil {
		return err
	}

	get.ProjectionExpression = expr.Projection()
	get.ExpressionAttributeNames = expr.Names()
	return nil
}

func applyProjectionToKeysAndAttributes(keysAndAttrs *types.KeysAndAttributes, projection []string) error {
	if len(projection) == 0 {
		return nil
	}

	expr, err := buildProjectionExpression(projection)
	if err != nil {
		return err
	}

	keysAndAttrs.ProjectionExpression = expr.Projection()
	keysAndAttrs.ExpressionAttributeNames = expr.Names()
	return nil
}

func buildProjectionExpression(attributes []string) (expression2.Expression, error) {
	if len(attributes) == 0 {
		return expression2.Expression{}, nil
	}

	var proj expression2.ProjectionBuilder
	for i, attr := range attributes {
		if i == 0 {
			proj = expression2.NamesList(expression2.Name(attr))
		} else {
			proj = proj.AddNames(expression2.Name(attr))
		}
	}

	return expression2.NewBuilder().WithProjection(proj).Build()
}

func extractItemsFromResponses(responses []types.ItemResponse) []Item {
	items := make([]Item, 0, len(responses))
	for _, resp := range responses {
		items = append(items, resp.Item)
	}
	return items
}

// GetOption configures the getter behavior.
// Options apply to all getter methods: GetItem, GetItemsTx, and GetItemsBatch.
type GetOption func(*getOpts)

type getOpts struct {
	// Note: TransactGetItems always uses serializable isolation.
	eventuallyConsistent bool
}

// WithEventualConsistency enables eventually consistent reads for lookups.
// By default, reads are strongly consistent.
// Note: This option has no effect on GetItemsTx, which always uses serializable isolation.
func WithEventualConsistency() GetOption {
	return func(o *getOpts) {
		o.eventuallyConsistent = true
	}
}
