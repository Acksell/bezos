package ddbcli

import (
	"testing"

	"github.com/acksell/bezos/dynamodb/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildKey_SimpleString(t *testing.T) {
	key, err := BuildKey("USER#{id}", map[string]string{"id": "string"}, map[string]string{"id": "abc123"})
	require.NoError(t, err)
	assert.Equal(t, "USER#abc123", key)
}

func TestBuildKey_Constant(t *testing.T) {
	key, err := BuildKey("PROFILE", map[string]string{}, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "PROFILE", key)
}

func TestBuildKey_MultipleFields(t *testing.T) {
	key, err := BuildKey(
		"TENANT#{tenantID}#ORDER#{orderID}",
		map[string]string{"tenantID": "string", "orderID": "string"},
		map[string]string{"tenantID": "t1", "orderID": "o1"},
	)
	require.NoError(t, err)
	assert.Equal(t, "TENANT#t1#ORDER#o1", key)
}

func TestBuildKey_Int64(t *testing.T) {
	key, err := BuildKey(
		"MSG#{sequenceNum}",
		map[string]string{"sequenceNum": "int64"},
		map[string]string{"sequenceNum": "42"},
	)
	require.NoError(t, err)
	assert.Equal(t, "MSG#42", key)
}

func TestBuildKey_Int64WithPadding(t *testing.T) {
	key, err := BuildKey(
		"EVENT#{timestamp:unixnano:%020d}",
		map[string]string{"timestamp": "time.Time"},
		map[string]string{"timestamp": "2024-01-01T00:00:00Z"},
	)
	require.NoError(t, err)
	assert.Equal(t, "EVENT#01704067200000000000", key)
}

func TestBuildKey_MissingField(t *testing.T) {
	_, err := BuildKey("USER#{id}", map[string]string{"id": "string"}, map[string]string{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required field")
}

func TestBuildKey_InvalidInt(t *testing.T) {
	_, err := BuildKey(
		"MSG#{seq}",
		map[string]string{"seq": "int64"},
		map[string]string{"seq": "not-a-number"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected integer")
}

func TestBuildKey_EmptyPattern(t *testing.T) {
	key, err := BuildKey("", map[string]string{}, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "", key)
}

func TestRequiredParams(t *testing.T) {
	e := schema.Entity{
		Type:                "Order",
		PartitionKeyPattern: "TENANT#{tenantID}",
		SortKeyPattern:      "ORDER#{orderID}",
		Fields: []schema.Field{
			{Name: "TenantID", Tag: "tenantID", Type: "string"},
			{Name: "OrderID", Tag: "orderID", Type: "string"},
			{Name: "Amount", Tag: "amount", Type: "int"},
		},
	}

	params := RequiredParams(e)
	require.Len(t, params, 2)
	assert.Equal(t, "tenantID", params[0].Name)
	assert.Equal(t, "string", params[0].Type)
	assert.Equal(t, "partitionKey", params[0].Source)
	assert.Equal(t, "orderID", params[1].Name)
	assert.Equal(t, "string", params[1].Type)
	assert.Equal(t, "sortKey", params[1].Source)
}

func TestRequiredParams_ConstantSK(t *testing.T) {
	e := schema.Entity{
		Type:                "User",
		PartitionKeyPattern: "USER#{id}",
		SortKeyPattern:      "PROFILE",
		Fields: []schema.Field{
			{Name: "UserID", Tag: "id", Type: "string"},
		},
	}

	params := RequiredParams(e)
	require.Len(t, params, 1)
	assert.Equal(t, "id", params[0].Name)
}

func TestFindEntity(t *testing.T) {
	schemas := []schema.Schema{
		{
			Tables: []schema.Table{
				{
					Name: "users",
					Entities: []schema.Entity{
						{Type: "User", PartitionKeyPattern: "USER#{id}"},
					},
				},
			},
		},
	}

	match, ok := FindEntity(schemas, "User")
	require.True(t, ok)
	assert.Equal(t, "User", match.Entity.Type)
	assert.Equal(t, "users", match.Table.Name)

	// Case insensitive
	match, ok = FindEntity(schemas, "user")
	require.True(t, ok)
	assert.Equal(t, "User", match.Entity.Type)

	_, ok = FindEntity(schemas, "NonExistent")
	assert.False(t, ok)
}

func TestLiteralPrefix(t *testing.T) {
	assert.Equal(t, "USER#", LiteralPrefix("USER#{id}"))
	assert.Equal(t, "PROFILE", LiteralPrefix("PROFILE"))
	assert.Equal(t, "", LiteralPrefix("{id}"))
	assert.Equal(t, "", LiteralPrefix(""))
	assert.Equal(t, "TENANT#", LiteralPrefix("TENANT#{tenantID}"))
}

func TestGSIParams(t *testing.T) {
	e := schema.Entity{
		Type:                "User",
		PartitionKeyPattern: "USER#{id}",
		SortKeyPattern:      "PROFILE",
		Fields: []schema.Field{
			{Name: "UserID", Tag: "id", Type: "string"},
			{Name: "Email", Tag: "email", Type: "string"},
		},
		GSIMappings: []schema.GSIMapping{
			{
				GSI:              "GSI1",
				PartitionPattern: "EMAIL#{email}",
				SortPattern:      "USER#{id}",
			},
		},
	}

	pk, sk, err := GSIParams(e, "GSI1")
	require.NoError(t, err)
	require.Len(t, pk, 1)
	assert.Equal(t, "email", pk[0].Name)
	require.Len(t, sk, 1)
	assert.Equal(t, "id", sk[0].Name)

	_, _, err = GSIParams(e, "GSI99")
	require.Error(t, err)
}

func TestBuildKey_TimeUnix(t *testing.T) {
	key, err := BuildKey(
		"TS#{ts:unix}",
		map[string]string{"ts": "time.Time"},
		map[string]string{"ts": "2024-01-01T00:00:00Z"},
	)
	require.NoError(t, err)
	assert.Equal(t, "TS#1704067200", key)
}

func TestBuildKey_TimeUnixFromTimestamp(t *testing.T) {
	// Pass a raw unix timestamp
	key, err := BuildKey(
		"TS#{ts:unix}",
		map[string]string{"ts": "time.Time"},
		map[string]string{"ts": "1704067200"},
	)
	require.NoError(t, err)
	assert.Equal(t, "TS#1704067200", key)
}
