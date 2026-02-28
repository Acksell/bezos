package ddbui

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/acksell/bezos/dynamodb/ddbstore"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// APIHandler provides REST API endpoints for DynamoDB operations.
type APIHandler struct {
	store  *ddbstore.Store
	schema *LoadedSchema
}

// NewAPIHandler creates a new API handler.
func NewAPIHandler(store *ddbstore.Store, schema *LoadedSchema) *APIHandler {
	return &APIHandler{
		store:  store,
		schema: schema,
	}
}

// RegisterRoutes registers all API routes on the given mux.
func (h *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/tables", h.listTables)
	mux.HandleFunc("GET /api/tables/{table}", h.getTable)
	mux.HandleFunc("GET /api/tables/{table}/items", h.scanItems)
	mux.HandleFunc("POST /api/tables/{table}/items", h.putItem)
	mux.HandleFunc("GET /api/tables/{table}/items/{pk}", h.getItem)
	mux.HandleFunc("GET /api/tables/{table}/items/{pk}/{sk}", h.getItemWithSK)
	mux.HandleFunc("DELETE /api/tables/{table}/items/{pk}", h.deleteItem)
	mux.HandleFunc("DELETE /api/tables/{table}/items/{pk}/{sk}", h.deleteItemWithSK)
	mux.HandleFunc("POST /api/tables/{table}/items/bulk-delete", h.bulkDeleteItems)
	mux.HandleFunc("POST /api/tables/{table}/query", h.queryItems)
	mux.HandleFunc("POST /api/tables/{table}/gsi/{gsi}/query", h.queryGSI)
}

// listTables returns all available tables and their schemas.
func (h *APIHandler) listTables(w http.ResponseWriter, r *http.Request) {
	tables := make([]map[string]any, 0, len(h.schema.Tables))
	for name, sf := range h.schema.Tables {
		tables = append(tables, map[string]any{
			"name":         name,
			"partitionKey": sf.Table.PartitionKey,
			"sortKey":      sf.Table.SortKey,
			"gsiCount":     len(sf.Table.GSIs),
			"entityCount":  len(sf.Entities),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": tables})
}

// getTable returns full schema for a specific table.
func (h *APIHandler) getTable(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	sf, ok := h.schema.Tables[tableName]
	if !ok {
		writeError(w, http.StatusNotFound, "table not found: "+tableName)
		return
	}
	writeJSON(w, http.StatusOK, sf)
}

// scanItems returns all items in a table with pagination.
func (h *APIHandler) scanItems(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if _, ok := h.schema.Tables[tableName]; !ok {
		writeError(w, http.StatusNotFound, "table not found: "+tableName)
		return
	}

	limit := parseIntParam(r, "limit", 25)
	lastKeyB64 := r.URL.Query().Get("lastKey")

	input := &dynamodb.ScanInput{
		TableName: &tableName,
		Limit:     int32Ptr(int32(limit)),
	}

	if lastKeyB64 != "" {
		lastKey, err := decodeLastKey(lastKeyB64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid lastKey: "+err.Error())
			return
		}
		input.ExclusiveStartKey = lastKey
	}

	output, err := h.store.Scan(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "scan failed: "+err.Error())
		return
	}

	items := convertItemsToJSON(output.Items)
	resp := map[string]any{
		"items": items,
		"count": len(items),
	}

	if output.LastEvaluatedKey != nil {
		resp["lastKey"] = encodeLastKey(output.LastEvaluatedKey)
	}

	writeJSON(w, http.StatusOK, resp)
}

// getItem retrieves a single item by partition key only.
func (h *APIHandler) getItem(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	pk := r.PathValue("pk")
	h.doGetItem(w, r, tableName, pk, "")
}

// getItemWithSK retrieves a single item by partition key and sort key.
func (h *APIHandler) getItemWithSK(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	pk := r.PathValue("pk")
	sk := r.PathValue("sk")
	h.doGetItem(w, r, tableName, pk, sk)
}

func (h *APIHandler) doGetItem(w http.ResponseWriter, r *http.Request, tableName, pk, sk string) {
	sf, ok := h.schema.Tables[tableName]
	if !ok {
		writeError(w, http.StatusNotFound, "table not found: "+tableName)
		return
	}

	key := buildKey(sf, pk, sk)
	input := &dynamodb.GetItemInput{
		TableName: &tableName,
		Key:       key,
	}

	output, err := h.store.GetItem(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get item failed: "+err.Error())
		return
	}

	if output.Item == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"item": convertItemToJSON(output.Item)})
}

// deleteItem removes an item by partition key only.
func (h *APIHandler) deleteItem(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	pk := r.PathValue("pk")
	h.doDeleteItem(w, r, tableName, pk, "")
}

// deleteItemWithSK removes an item by partition and sort key.
func (h *APIHandler) deleteItemWithSK(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	pk := r.PathValue("pk")
	sk := r.PathValue("sk")
	h.doDeleteItem(w, r, tableName, pk, sk)
}

func (h *APIHandler) doDeleteItem(w http.ResponseWriter, r *http.Request, tableName, pk, sk string) {
	sf, ok := h.schema.Tables[tableName]
	if !ok {
		writeError(w, http.StatusNotFound, "table not found: "+tableName)
		return
	}

	key := buildKey(sf, pk, sk)
	input := &dynamodb.DeleteItemInput{
		TableName: &tableName,
		Key:       key,
	}

	_, err := h.store.DeleteItem(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "delete item failed: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

// BulkDeleteRequest is the JSON request body for bulk delete.
type BulkDeleteRequest struct {
	Keys []BulkDeleteKey `json:"keys"`
}

// BulkDeleteKey represents a single key to delete.
type BulkDeleteKey struct {
	PK string `json:"pk"`
	SK string `json:"sk,omitempty"`
}

// bulkDeleteItems deletes multiple items in a single request.
func (h *APIHandler) bulkDeleteItems(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	sf, ok := h.schema.Tables[tableName]
	if !ok {
		writeError(w, http.StatusNotFound, "table not found: "+tableName)
		return
	}

	var req BulkDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Keys) == 0 {
		writeError(w, http.StatusBadRequest, "no keys provided")
		return
	}

	if len(req.Keys) > 25 {
		writeError(w, http.StatusBadRequest, "maximum 25 items per bulk delete")
		return
	}

	// Build batch write request
	writeRequests := make([]types.WriteRequest, len(req.Keys))
	for i, k := range req.Keys {
		key := buildKey(sf, k.PK, k.SK)
		writeRequests[i] = types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: key,
			},
		}
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	}

	_, err := h.store.BatchWriteItem(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "bulk delete failed: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": len(req.Keys),
	})
}

// PutItemRequest is the JSON request body for putting an item.
type PutItemRequest struct {
	Item map[string]any `json:"item"`
}

// putItem creates or replaces an item.
func (h *APIHandler) putItem(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	if _, ok := h.schema.Tables[tableName]; !ok {
		writeError(w, http.StatusNotFound, "table not found: "+tableName)
		return
	}

	var req PutItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	item := convertJSONToItem(req.Item)
	input := &dynamodb.PutItemInput{
		TableName: &tableName,
		Item:      item,
	}

	_, err := h.store.PutItem(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "put item failed: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// QueryRequest is the JSON request body for querying items.
type QueryRequest struct {
	KeyConditionExpression    string         `json:"keyConditionExpression"`
	FilterExpression          string         `json:"filterExpression,omitempty"`
	ExpressionAttributeNames  map[string]string `json:"expressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"expressionAttributeValues,omitempty"`
	Limit                     int            `json:"limit,omitempty"`
	ScanIndexForward          *bool          `json:"scanIndexForward,omitempty"`
	LastKey                   string         `json:"lastKey,omitempty"`
}

// queryItems queries the primary index.
func (h *APIHandler) queryItems(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	h.doQuery(w, r, tableName, nil)
}

// queryGSI queries a Global Secondary Index.
func (h *APIHandler) queryGSI(w http.ResponseWriter, r *http.Request) {
	tableName := r.PathValue("table")
	gsiName := r.PathValue("gsi")
	h.doQuery(w, r, tableName, &gsiName)
}

func (h *APIHandler) doQuery(w http.ResponseWriter, r *http.Request, tableName string, indexName *string) {
	if _, ok := h.schema.Tables[tableName]; !ok {
		writeError(w, http.StatusNotFound, "table not found: "+tableName)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read body: "+err.Error())
		return
	}

	var req QueryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.KeyConditionExpression == "" {
		writeError(w, http.StatusBadRequest, "keyConditionExpression is required")
		return
	}

	limit := int32(25)
	if req.Limit > 0 {
		limit = int32(req.Limit)
	}

	input := &dynamodb.QueryInput{
		TableName:                 &tableName,
		IndexName:                 indexName,
		KeyConditionExpression:    &req.KeyConditionExpression,
		ExpressionAttributeNames:  req.ExpressionAttributeNames,
		ExpressionAttributeValues: convertExpressionValues(req.ExpressionAttributeValues),
		Limit:                     &limit,
		ScanIndexForward:          req.ScanIndexForward,
	}

	if req.FilterExpression != "" {
		input.FilterExpression = &req.FilterExpression
	}

	if req.LastKey != "" {
		lastKey, err := decodeLastKey(req.LastKey)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid lastKey: "+err.Error())
			return
		}
		input.ExclusiveStartKey = lastKey
	}

	output, err := h.store.Query(context.Background(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed: "+err.Error())
		return
	}

	items := convertItemsToJSON(output.Items)
	resp := map[string]any{
		"items": items,
		"count": len(items),
	}

	if output.LastEvaluatedKey != nil {
		resp["lastKey"] = encodeLastKey(output.LastEvaluatedKey)
	}

	writeJSON(w, http.StatusOK, resp)
}

// Helper functions

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

func parseIntParam(r *http.Request, name string, defaultVal int) int {
	s := r.URL.Query().Get(name)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

func int32Ptr(v int32) *int32 {
	return &v
}

// buildKey creates a DynamoDB key from partition key and optional sort key.
func buildKey(sf *SchemaFile, pk, sk string) map[string]types.AttributeValue {
	key := make(map[string]types.AttributeValue)
	key[sf.Table.PartitionKey.Name] = &types.AttributeValueMemberS{Value: pk}
	if sk != "" && sf.Table.SortKey != nil {
		key[sf.Table.SortKey.Name] = &types.AttributeValueMemberS{Value: sk}
	}
	return key
}

// convertItemToJSON converts a DynamoDB item to JSON-friendly format.
func convertItemToJSON(item map[string]types.AttributeValue) map[string]any {
	result := make(map[string]any)
	for k, v := range item {
		result[k] = attributeValueToJSON(v)
	}
	return result
}

// convertItemsToJSON converts multiple DynamoDB items to JSON-friendly format.
func convertItemsToJSON(items []map[string]types.AttributeValue) []map[string]any {
	result := make([]map[string]any, len(items))
	for i, item := range items {
		result[i] = convertItemToJSON(item)
	}
	return result
}

// attributeValueToJSON converts a single AttributeValue to JSON-friendly format.
func attributeValueToJSON(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		// Try to parse as int, fall back to float
		if i, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.Value, 64); err == nil {
			return f
		}
		return v.Value
	case *types.AttributeValueMemberB:
		return base64.StdEncoding.EncodeToString(v.Value)
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, elem := range v.Value {
			list[i] = attributeValueToJSON(elem)
		}
		return list
	case *types.AttributeValueMemberM:
		m := make(map[string]any)
		for k, elem := range v.Value {
			m[k] = attributeValueToJSON(elem)
		}
		return m
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberBS:
		list := make([]string, len(v.Value))
		for i, b := range v.Value {
			list[i] = base64.StdEncoding.EncodeToString(b)
		}
		return list
	default:
		return nil
	}
}

// convertJSONToItem converts a JSON map to DynamoDB item.
func convertJSONToItem(data map[string]any) map[string]types.AttributeValue {
	result := make(map[string]types.AttributeValue)
	for k, v := range data {
		if av := jsonToAttributeValue(v); av != nil {
			result[k] = av
		}
	}
	return result
}

// jsonToAttributeValue converts a JSON value to AttributeValue.
func jsonToAttributeValue(v any) types.AttributeValue {
	if v == nil {
		return &types.AttributeValueMemberNULL{Value: true}
	}
	switch val := v.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: val}
	case float64:
		// JSON numbers are always float64
		if val == float64(int64(val)) {
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(val), 10)}
		}
		return &types.AttributeValueMemberN{Value: strconv.FormatFloat(val, 'f', -1, 64)}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: val}
	case []any:
		list := make([]types.AttributeValue, len(val))
		for i, elem := range val {
			list[i] = jsonToAttributeValue(elem)
		}
		return &types.AttributeValueMemberL{Value: list}
	case map[string]any:
		m := make(map[string]types.AttributeValue)
		for k, elem := range val {
			if av := jsonToAttributeValue(elem); av != nil {
				m[k] = av
			}
		}
		return &types.AttributeValueMemberM{Value: m}
	default:
		return nil
	}
}

// convertExpressionValues converts JSON expression values to DynamoDB format.
func convertExpressionValues(values map[string]any) map[string]types.AttributeValue {
	if values == nil {
		return nil
	}
	result := make(map[string]types.AttributeValue)
	for k, v := range values {
		if av := jsonToAttributeValue(v); av != nil {
			result[k] = av
		}
	}
	return result
}

// encodeLastKey encodes a DynamoDB key as base64 JSON for pagination.
func encodeLastKey(key map[string]types.AttributeValue) string {
	data := convertItemToJSON(key)
	b, _ := json.Marshal(data)
	return base64.URLEncoding.EncodeToString(b)
}

// decodeLastKey decodes a base64 JSON pagination key.
func decodeLastKey(encoded string) (map[string]types.AttributeValue, error) {
	b, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	var data map[string]any
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, err
	}
	return convertJSONToItem(data), nil
}

// DetectEntityType attempts to match an item's key patterns to known entities.
func (h *APIHandler) DetectEntityType(tableName string, item map[string]any) string {
	sf, ok := h.schema.Tables[tableName]
	if !ok {
		return ""
	}

	pkName := sf.Table.PartitionKey.Name
	skName := ""
	if sf.Table.SortKey != nil {
		skName = sf.Table.SortKey.Name
	}

	pk, _ := item[pkName].(string)
	sk, _ := item[skName].(string)

	for _, entity := range sf.Entities {
		if matchesPattern(pk, entity.PartitionKeyPattern) {
			if skName == "" || matchesPattern(sk, entity.SortKeyPattern) {
				return entity.Type
			}
		}
	}
	return ""
}

// matchesPattern checks if a value matches a key pattern (simple prefix match).
func matchesPattern(value, pattern string) bool {
	if pattern == "" {
		return true
	}
	// Extract prefix before first {
	idx := strings.Index(pattern, "{")
	if idx == -1 {
		return value == pattern
	}
	prefix := pattern[:idx]
	return strings.HasPrefix(value, prefix)
}
