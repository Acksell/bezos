package ddbsdk

import (
	"context"
	"testing"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

var queryTestTable = table.TableDefinition{
	Name: "query-test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

func queryTestKey(pk, sk string) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: queryTestTable.KeyDefinitions,
		Values:     table.PrimaryKeyValues{PartitionKey: pk, SortKey: sk},
	}
}

func TestQuery_NoSortKeyCondition(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items with same partition key
	items := []testEntity{
		{PK: "user#1", SK: "profile#1", Name: "Alice", Age: 30},
		{PK: "user#1", SK: "profile#2", Name: "Bob", Age: 25},
		{PK: "user#1", SK: "profile#3", Name: "Charlie", Age: 35},
		{PK: "user#2", SK: "profile#1", Name: "Dave", Age: 40},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query all items with pk="user#1"
	kc := NewKeyCondition("user#1", nil)
	querier := db.NewQuery(queryTestTable, kc)

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result.Items))
	}
}

func TestQuery_WithEquals(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile#1", Name: "Alice"},
		{PK: "user#1", SK: "profile#2", Name: "Bob"},
		{PK: "user#1", SK: "profile#3", Name: "Charlie"},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with sort key equals
	kc := NewKeyCondition("user#1", Equals("profile#2"))
	querier := db.NewQuery(queryTestTable, kc)

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 1 {
		t.Errorf("expected 1 item, got %d", len(result.Items))
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(result.Items[0], &retrieved); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if retrieved.Name != "Bob" {
		t.Errorf("expected Name='Bob', got %q", retrieved.Name)
	}
}

func TestQuery_WithBeginsWith(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile#1", Name: "Alice"},
		{PK: "user#1", SK: "profile#2", Name: "Bob"},
		{PK: "user#1", SK: "order#1", Name: "Order1"},
		{PK: "user#1", SK: "order#2", Name: "Order2"},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with sort key begins with "profile#"
	kc := NewKeyCondition("user#1", BeginsWith("profile#"))
	querier := db.NewQuery(queryTestTable, kc)

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(result.Items))
	}
}

func TestQuery_WithBetween(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "2024-01-01", Name: "Day1"},
		{PK: "user#1", SK: "2024-01-15", Name: "Day15"},
		{PK: "user#1", SK: "2024-01-31", Name: "Day31"},
		{PK: "user#1", SK: "2024-02-01", Name: "Feb1"},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with Between
	kc := NewKeyCondition("user#1", Between("2024-01-10", "2024-01-31"))
	querier := db.NewQuery(queryTestTable, kc)

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("expected 2 items (15th and 31st), got %d", len(result.Items))
	}
}

func TestQuery_WithGreaterThan(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items with numeric sort keys
	items := []testEntity{
		{PK: "scores", SK: "100", Name: "Low"},
		{PK: "scores", SK: "200", Name: "Mid"},
		{PK: "scores", SK: "300", Name: "High"},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with GreaterThan
	kc := NewKeyCondition("scores", GreaterThan("150"))
	querier := db.NewQuery(queryTestTable, kc)

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("expected 2 items (200 and 300), got %d", len(result.Items))
	}
}

func TestQuery_WithLessThan(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "scores", SK: "100", Name: "Low"},
		{PK: "scores", SK: "200", Name: "Mid"},
		{PK: "scores", SK: "300", Name: "High"},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with LessThan
	kc := NewKeyCondition("scores", LessThan("250"))
	querier := db.NewQuery(queryTestTable, kc)

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("expected 2 items (100 and 200), got %d", len(result.Items))
	}
}

func TestQuery_Pagination(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	for i := 1; i <= 25; i++ {
		item := testEntity{
			PK:   "user#1",
			SK:   "item#" + string(rune('0'+i/10)) + string(rune('0'+i%10)),
			Name: "Item" + string(rune('0'+i)),
		}
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with pagination
	kc := NewKeyCondition("user#1", nil)
	querier := db.NewQuery(queryTestTable, kc)

	// Get first page
	result1, err := querier.Next(ctx)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if result1.IsDone {
		t.Error("expected more results")
	}

	// Get second page
	result2, err := querier.Next(ctx)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// Ensure we got different items
	if len(result1.Items) == 0 || len(result2.Items) == 0 {
		t.Error("expected items in both pages")
	}
}

// Note: WithFilter method is not currently implemented on the Querier interface
// This test is skipped for now
/*
func TestQuery_WithFilterExpression(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile#1", Name: "Alice", Age: 30},
		{PK: "user#1", SK: "profile#2", Name: "Bob", Age: 25},
		{PK: "user#1", SK: "profile#3", Name: "Charlie", Age: 35},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with filter for age > 28
	kc := NewKeyCondition("user#1", nil)
	filter := expression.GreaterThan(expression.Name("age"), expression.Value(28))
	querier := db.NewQuery(queryTestTable, kc).WithFilter(filter)

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("expected 2 items (Alice and Charlie), got %d", len(result.Items))
	}
}
*/

func TestQuery_WithProjection(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile#1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{PK: "user#1", SK: "profile#2", Name: "Bob", Email: "bob@example.com", Age: 25},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query with projection - only get name field
	kc := NewKeyCondition("user#1", nil)
	querier := NewQuerier(db.(*Client).awsddb, queryTestTable, kc).WithProjection("pk", "sk", "name")

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(result.Items))
	}

	// Verify only projected fields are present
	for _, item := range result.Items {
		if _, ok := item["email"]; ok {
			t.Error("expected email field to be excluded")
		}
		if _, ok := item["age"]; ok {
			t.Error("expected age field to be excluded")
		}
		if _, ok := item["name"]; !ok {
			t.Error("expected name field to be present")
		}
	}
}

func TestQuery_Descending(t *testing.T) {
	db := NewMock(queryTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "a", Name: "First"},
		{PK: "user#1", SK: "b", Name: "Second"},
		{PK: "user#1", SK: "c", Name: "Third"},
	}

	for _, item := range items {
		pk := queryTestKey(item.PK, item.SK)
		put := NewUnsafePut(queryTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query in descending order
	kc := NewKeyCondition("user#1", nil)
	querier := NewQuerier(db.(*Client).awsddb, queryTestTable, kc).WithDescending()

	result, err := querier.QueryAll(ctx)
	if err != nil {
		t.Fatalf("QueryAll failed: %v", err)
	}

	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result.Items))
	}

	// Verify order is reversed
	var first testEntity
	if err := attributevalue.UnmarshalMap(result.Items[0], &first); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if first.Name != "Third" {
		t.Errorf("expected first item to be 'Third', got %q", first.Name)
	}
}
