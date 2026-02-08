package ddbstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Key encoding for BadgerDB that supports proper lexicographic ordering.
// Key format: [tablePrefix][separator][partitionKey][separator][sortKey]
//
// For GSIs: [tablePrefix][$gsi:][gsiName][separator][partitionKey][separator][sortKey]
//
// The separator byte (0x00) is used to separate components.
// Keys are encoded to preserve sort order for all DynamoDB key types (S, N, B).

const (
	keySeparator byte = 0x00
	gsiMarker         = "$gsi:"
)

// Key type markers for encoding
const (
	keyTypeString byte = 'S'
	keyTypeNumber byte = 'N'
	keyTypeBinary byte = 'B'
)

// KeyEncoder handles encoding and decoding of DynamoDB keys for BadgerDB.
type KeyEncoder struct {
	tableName string
	gsiName   string // empty for main table
	keyDef    table.PrimaryKeyDefinition
}

// NewKeyEncoder creates a new encoder for the given table.
func NewKeyEncoder(tableName string, keyDef table.PrimaryKeyDefinition) *KeyEncoder {
	return &KeyEncoder{
		tableName: tableName,
		keyDef:    keyDef,
	}
}

// NewGSIKeyEncoder creates a new encoder for a GSI.
func NewGSIKeyEncoder(tableName, gsiName string, keyDef table.PrimaryKeyDefinition) *KeyEncoder {
	return &KeyEncoder{
		tableName: tableName,
		gsiName:   gsiName,
		keyDef:    keyDef,
	}
}

// EncodeKey encodes a primary key into a BadgerDB key.
func (e *KeyEncoder) EncodeKey(pk table.PrimaryKey) ([]byte, error) {
	var buf bytes.Buffer

	// Write table prefix
	buf.WriteString(e.tableName)

	// Write GSI marker if applicable
	if e.gsiName != "" {
		buf.WriteString(gsiMarker)
		buf.WriteString(e.gsiName)
	}
	buf.WriteByte(keySeparator)

	// Encode partition key
	pkBytes, err := encodeKeyValue(pk.Values.PartitionKey, pk.Definition.PartitionKey.Kind)
	if err != nil {
		return nil, fmt.Errorf("encode partition key: %w", err)
	}
	buf.Write(pkBytes)
	buf.WriteByte(keySeparator)

	// Encode sort key if present
	if pk.Definition.SortKey.Name != "" {
		skBytes, err := encodeKeyValue(pk.Values.SortKey, pk.Definition.SortKey.Kind)
		if err != nil {
			return nil, fmt.Errorf("encode sort key: %w", err)
		}
		buf.Write(skBytes)
	}

	return buf.Bytes(), nil
}

// EncodePartitionKeyPrefix returns a prefix for scanning all items with a given partition key.
func (e *KeyEncoder) EncodePartitionKeyPrefix(partitionKey any) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString(e.tableName)
	if e.gsiName != "" {
		buf.WriteString(gsiMarker)
		buf.WriteString(e.gsiName)
	}
	buf.WriteByte(keySeparator)

	pkBytes, err := encodeKeyValue(partitionKey, e.keyDef.PartitionKey.Kind)
	if err != nil {
		return nil, fmt.Errorf("encode partition key: %w", err)
	}
	buf.Write(pkBytes)
	buf.WriteByte(keySeparator)

	return buf.Bytes(), nil
}

// EncodeSortKeyValue encodes a sort key value for range comparisons.
func (e *KeyEncoder) EncodeSortKeyValue(sortKey any) ([]byte, error) {
	return encodeKeyValue(sortKey, e.keyDef.SortKey.Kind)
}

// TablePrefix returns the prefix for all keys in this table/GSI.
func (e *KeyEncoder) TablePrefix() []byte {
	var buf bytes.Buffer
	buf.WriteString(e.tableName)
	if e.gsiName != "" {
		buf.WriteString(gsiMarker)
		buf.WriteString(e.gsiName)
	}
	buf.WriteByte(keySeparator)
	return buf.Bytes()
}

// encodeKeyValue encodes a key value with proper ordering based on key kind.
func encodeKeyValue(value any, kind table.KeyKind) ([]byte, error) {
	var buf bytes.Buffer

	switch kind {
	case table.KeyKindS:
		buf.WriteByte(keyTypeString)
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for S key, got %T", value)
		}
		// Escape null bytes in strings to preserve separator integrity
		buf.Write(escapeBytes([]byte(s)))

	case table.KeyKindN:
		buf.WriteByte(keyTypeNumber)
		// Numbers in DynamoDB are stored as strings
		var numStr string
		switch v := value.(type) {
		case string:
			numStr = v
		case float64:
			numStr = strconv.FormatFloat(v, 'f', -1, 64)
		case int:
			numStr = strconv.Itoa(v)
		case int64:
			numStr = strconv.FormatInt(v, 10)
		default:
			return nil, fmt.Errorf("expected number for N key, got %T", value)
		}
		encoded, err := encodeNumber(numStr)
		if err != nil {
			return nil, err
		}
		buf.Write(encoded)

	case table.KeyKindB:
		buf.WriteByte(keyTypeBinary)
		var b []byte
		switch v := value.(type) {
		case []byte:
			b = v
		case string:
			b = []byte(v)
		default:
			return nil, fmt.Errorf("expected binary for B key, got %T", value)
		}
		buf.Write(escapeBytes(b))

	default:
		return nil, fmt.Errorf("unsupported key kind: %s", kind)
	}

	return buf.Bytes(), nil
}

// encodeNumber encodes a number string for lexicographic ordering.
// Uses a scheme that preserves numeric ordering when compared as bytes.
// Format: [sign byte][magnitude bytes]
// Positive numbers: 0x80 + encoded big-endian float64
// Negative numbers: 0x7F - encoded big-endian float64 (inverted for reverse order)
func encodeNumber(numStr string) ([]byte, error) {
	f, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return nil, fmt.Errorf("parse number %q: %w", numStr, err)
	}

	bits := math.Float64bits(f)
	buf := make([]byte, 9)

	if f >= 0 {
		// For positive numbers (including +0), flip the sign bit
		// This makes positive numbers sort after negative numbers
		buf[0] = 0x80
		bits ^= (1 << 63) // flip sign bit
	} else {
		// For negative numbers, invert all bits
		// This makes more negative numbers sort before less negative numbers
		buf[0] = 0x7F
		bits = ^bits
	}

	binary.BigEndian.PutUint64(buf[1:], bits)
	return buf, nil
}

// decodeNumber decodes a number from the encoded format back to string.
func decodeNumber(encoded []byte) (string, error) {
	if len(encoded) != 9 {
		return "", fmt.Errorf("invalid encoded number length: %d", len(encoded))
	}

	bits := binary.BigEndian.Uint64(encoded[1:])

	if encoded[0] == 0x80 {
		// Positive number - flip sign bit back
		bits ^= (1 << 63)
	} else {
		// Negative number - invert all bits back
		bits = ^bits
	}

	f := math.Float64frombits(bits)
	return strconv.FormatFloat(f, 'f', -1, 64), nil
}

// escapeBytes escapes null bytes (0x00) in the input to preserve separator integrity.
// Uses 0x01 0x01 for literal 0x00, and 0x01 0x02 for literal 0x01.
func escapeBytes(b []byte) []byte {
	var buf bytes.Buffer
	for _, c := range b {
		switch c {
		case 0x00:
			buf.WriteByte(0x01)
			buf.WriteByte(0x01)
		case 0x01:
			buf.WriteByte(0x01)
			buf.WriteByte(0x02)
		default:
			buf.WriteByte(c)
		}
	}
	return buf.Bytes()
}

// unescapeBytes reverses the escaping done by escapeBytes.
func unescapeBytes(b []byte) []byte {
	var buf bytes.Buffer
	for i := 0; i < len(b); i++ {
		if b[i] == 0x01 && i+1 < len(b) {
			switch b[i+1] {
			case 0x01:
				buf.WriteByte(0x00)
				i++
			case 0x02:
				buf.WriteByte(0x01)
				i++
			default:
				buf.WriteByte(b[i])
			}
		} else {
			buf.WriteByte(b[i])
		}
	}
	return buf.Bytes()
}

// Item serialization for BadgerDB values

// SerializeItem serializes a DynamoDB item to bytes for storage.
func SerializeItem(item map[string]types.AttributeValue) ([]byte, error) {
	// Convert to a serializable format
	serializable := make(map[string]serializableAV)
	for k, v := range item {
		serializable[k] = toSerializable(v)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(serializable); err != nil {
		return nil, fmt.Errorf("encode item: %w", err)
	}
	return buf.Bytes(), nil
}

// DeserializeItem deserializes bytes back to a DynamoDB item.
func DeserializeItem(data []byte) (map[string]types.AttributeValue, error) {
	var serializable map[string]serializableAV
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&serializable); err != nil {
		return nil, fmt.Errorf("decode item: %w", err)
	}

	result := make(map[string]types.AttributeValue)
	for k, v := range serializable {
		result[k] = fromSerializable(v)
	}
	return result, nil
}

// serializableAV is a gob-encodable representation of AttributeValue
type serializableAV struct {
	Type  string
	Value any
}

func init() {
	// Register types for gob encoding
	gob.Register(map[string]serializableAV{})
	gob.Register([]serializableAV{})
	gob.Register([]string{})
	gob.Register([][]byte{})
}

func toSerializable(av types.AttributeValue) serializableAV {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return serializableAV{Type: "S", Value: v.Value}
	case *types.AttributeValueMemberN:
		return serializableAV{Type: "N", Value: v.Value}
	case *types.AttributeValueMemberB:
		return serializableAV{Type: "B", Value: v.Value}
	case *types.AttributeValueMemberBOOL:
		return serializableAV{Type: "BOOL", Value: v.Value}
	case *types.AttributeValueMemberNULL:
		return serializableAV{Type: "NULL", Value: v.Value}
	case *types.AttributeValueMemberSS:
		return serializableAV{Type: "SS", Value: v.Value}
	case *types.AttributeValueMemberNS:
		return serializableAV{Type: "NS", Value: v.Value}
	case *types.AttributeValueMemberBS:
		return serializableAV{Type: "BS", Value: v.Value}
	case *types.AttributeValueMemberM:
		m := make(map[string]serializableAV)
		for k, val := range v.Value {
			m[k] = toSerializable(val)
		}
		return serializableAV{Type: "M", Value: m}
	case *types.AttributeValueMemberL:
		l := make([]serializableAV, len(v.Value))
		for i, val := range v.Value {
			l[i] = toSerializable(val)
		}
		return serializableAV{Type: "L", Value: l}
	default:
		panic(fmt.Sprintf("unsupported attribute value type: %T", av))
	}
}

func fromSerializable(sav serializableAV) types.AttributeValue {
	switch sav.Type {
	case "S":
		return &types.AttributeValueMemberS{Value: sav.Value.(string)}
	case "N":
		return &types.AttributeValueMemberN{Value: sav.Value.(string)}
	case "B":
		return &types.AttributeValueMemberB{Value: sav.Value.([]byte)}
	case "BOOL":
		return &types.AttributeValueMemberBOOL{Value: sav.Value.(bool)}
	case "NULL":
		return &types.AttributeValueMemberNULL{Value: sav.Value.(bool)}
	case "SS":
		return &types.AttributeValueMemberSS{Value: sav.Value.([]string)}
	case "NS":
		return &types.AttributeValueMemberNS{Value: sav.Value.([]string)}
	case "BS":
		return &types.AttributeValueMemberBS{Value: sav.Value.([][]byte)}
	case "M":
		m := make(map[string]types.AttributeValue)
		for k, v := range sav.Value.(map[string]serializableAV) {
			m[k] = fromSerializable(v)
		}
		return &types.AttributeValueMemberM{Value: m}
	case "L":
		l := make([]types.AttributeValue, len(sav.Value.([]serializableAV)))
		for i, v := range sav.Value.([]serializableAV) {
			l[i] = fromSerializable(v)
		}
		return &types.AttributeValueMemberL{Value: l}
	default:
		panic(fmt.Sprintf("unsupported serializable type: %s", sav.Type))
	}
}

// SerializeItemJSON provides JSON serialization as an alternative.
// Useful for debugging and human-readable storage.
func SerializeItemJSON(item map[string]types.AttributeValue) ([]byte, error) {
	serializable := make(map[string]serializableAV)
	for k, v := range item {
		serializable[k] = toSerializable(v)
	}
	return json.Marshal(serializable)
}

// DeserializeItemJSON deserializes JSON back to a DynamoDB item.
func DeserializeItemJSON(data []byte) (map[string]types.AttributeValue, error) {
	var serializable map[string]serializableAV
	if err := json.Unmarshal(data, &serializable); err != nil {
		return nil, err
	}

	result := make(map[string]types.AttributeValue)
	for k, v := range serializable {
		result[k] = fromSerializableJSON(v)
	}
	return result, nil
}

func fromSerializableJSON(sav serializableAV) types.AttributeValue {
	switch sav.Type {
	case "S":
		return &types.AttributeValueMemberS{Value: sav.Value.(string)}
	case "N":
		return &types.AttributeValueMemberN{Value: sav.Value.(string)}
	case "B":
		// JSON decodes []byte as base64 string
		if s, ok := sav.Value.(string); ok {
			return &types.AttributeValueMemberB{Value: []byte(s)}
		}
		return &types.AttributeValueMemberB{Value: sav.Value.([]byte)}
	case "BOOL":
		return &types.AttributeValueMemberBOOL{Value: sav.Value.(bool)}
	case "NULL":
		return &types.AttributeValueMemberNULL{Value: sav.Value.(bool)}
	case "SS":
		arr := sav.Value.([]any)
		ss := make([]string, len(arr))
		for i, v := range arr {
			ss[i] = v.(string)
		}
		return &types.AttributeValueMemberSS{Value: ss}
	case "NS":
		arr := sav.Value.([]any)
		ns := make([]string, len(arr))
		for i, v := range arr {
			ns[i] = v.(string)
		}
		return &types.AttributeValueMemberNS{Value: ns}
	case "BS":
		arr := sav.Value.([]any)
		bs := make([][]byte, len(arr))
		for i, v := range arr {
			bs[i] = []byte(v.(string))
		}
		return &types.AttributeValueMemberBS{Value: bs}
	case "M":
		m := make(map[string]types.AttributeValue)
		for k, v := range sav.Value.(map[string]any) {
			vMap := v.(map[string]any)
			m[k] = fromSerializableJSON(serializableAV{
				Type:  vMap["Type"].(string),
				Value: vMap["Value"],
			})
		}
		return &types.AttributeValueMemberM{Value: m}
	case "L":
		arr := sav.Value.([]any)
		l := make([]types.AttributeValue, len(arr))
		for i, v := range arr {
			vMap := v.(map[string]any)
			l[i] = fromSerializableJSON(serializableAV{
				Type:  vMap["Type"].(string),
				Value: vMap["Value"],
			})
		}
		return &types.AttributeValueMemberL{Value: l}
	default:
		panic(fmt.Sprintf("unsupported serializable type: %s", sav.Type))
	}
}
