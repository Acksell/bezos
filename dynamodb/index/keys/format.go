package keys

import (
	"fmt"
	"strings"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Extractor can extract a key value from a DynamoDB item.
type Extractor interface {
	Extract(item map[string]types.AttributeValue) (any, error)
}

// Key combines a key definition with its value extractor.
type Key struct {
	Def       table.KeyDef
	Extractor Extractor
}

// FromItem extracts the key value from the item and wraps it in the appropriate AttributeValue type.
func (k Key) FromItem(item map[string]types.AttributeValue) (types.AttributeValue, error) {
	val, err := k.Extractor.Extract(item)
	if err != nil {
		return nil, err
	}
	return toAttributeValue(val, k.Def.Kind)
}

func toAttributeValue(val any, kind table.KeyKind) (types.AttributeValue, error) {
	switch kind {
	case table.KeyKindS, "":
		return &types.AttributeValueMemberS{Value: fmt.Sprint(val)}, nil
	case table.KeyKindN:
		// DynamoDB validates number format; we just convert to string
		return &types.AttributeValueMemberN{Value: fmt.Sprint(val)}, nil
	case table.KeyKindB:
		switch v := val.(type) {
		case []byte:
			return &types.AttributeValueMemberB{Value: v}, nil
		case string:
			return &types.AttributeValueMemberB{Value: []byte(v)}, nil
		default:
			return nil, fmt.Errorf("key kind B requires []byte or string, got %T", val)
		}
	default:
		return nil, fmt.Errorf("unsupported key kind: %q", kind)
	}
}

// FieldRef extracts a value from a field path in the item.
type FieldRef struct {
	Path []string
}

func (f FieldRef) Extract(item map[string]types.AttributeValue) (any, error) {
	current := item

	// Navigate to the nested field
	for i, key := range f.Path[:len(f.Path)-1] {
		av, ok := current[key]
		if !ok {
			return nil, fmt.Errorf("field %q not found at path %v", key, f.Path[:i+1])
		}
		mapVal, ok := av.(*types.AttributeValueMemberM)
		if !ok {
			return nil, fmt.Errorf("field %q is not a map (got %T), cannot traverse path %v", key, av, f.Path)
		}
		current = mapVal.Value
	}

	// Extract the final field
	finalKey := f.Path[len(f.Path)-1]
	av, ok := current[finalKey]
	if !ok {
		return nil, fmt.Errorf("field %q not found", strings.Join(f.Path, "."))
	}

	return extractValue(av)
}

// extractValue extracts the underlying value from an AttributeValue.
func extractValue(av types.AttributeValue) (any, error) {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value, nil
	case *types.AttributeValueMemberN:
		return v.Value, nil // DynamoDB stores numbers as strings
	case *types.AttributeValueMemberB:
		return v.Value, nil // []byte
	default:
		return nil, fmt.Errorf("invalid attribute kind, cannot extract key value from %T", av)
	}
}

// ConstVal is a constant value, pre-marshaled to an AttributeValue.
type ConstVal struct {
	Value any
	av    types.AttributeValue
}

func (c ConstVal) Extract(_ map[string]types.AttributeValue) (any, error) {
	return extractValue(c.av)
}

// FormatExpr is a composite of multiple extractors that produces a formatted string.
type FormatExpr struct {
	Parts []Extractor
}

func (f FormatExpr) Extract(item map[string]types.AttributeValue) (any, error) {
	if len(f.Parts) == 0 {
		return nil, fmt.Errorf("key format has no parts")
	}

	var result strings.Builder
	for i, p := range f.Parts {
		val, err := p.Extract(item)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", i, err)
		}
		result.WriteString(fmt.Sprint(val))
	}
	return result.String(), nil
}

// Field creates an Extractor that extracts a field value from the item.
// The path arguments specify nested field access: Field("user", "id") extracts item["user"]["id"].
// For top-level fields, use a single argument: Field("userID").
//
// Field can be used standalone or as a component in Fmt:
//
//	keys.Field("createdAt")                   // extracts createdAt field directly
//	keys.Fmt("USER#%s", keys.Field("userID")) // USER#123
func Field(path ...string) FieldRef {
	if len(path) == 0 {
		panic("Field requires at least one path element")
	}
	return FieldRef{Path: path}
}

// Const creates an Extractor that always returns the given constant value.
// The value is marshaled using attributevalue.Marshal at construction time,
// so invalid types will panic immediately.
//
// Example:
//
//	keys.Const("PROFILE")  // always returns "PROFILE"
//	keys.Const(123)        // numeric constant
func Const(value any) ConstVal {
	av, err := attributevalue.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("Const: cannot marshal %T: %v", value, err))
	}
	return ConstVal{Value: value, av: av}
}

// Fmt creates an Extractor using a printf-style format string.
// Use %s as placeholders for field values. The result is always a string.
//
// Examples:
//
//	keys.Fmt("USER#%s", keys.Field("userID"))                     // USER#123
//	keys.Fmt("ORDER#%s#%s", keys.Field("tenant"), keys.Field("id")) // ORDER#acme#456
func Fmt(fmtStr string, parts ...Extractor) FormatExpr {
	segments := strings.Split(fmtStr, "%s")

	if len(segments)-1 != len(parts) {
		panic(fmt.Sprintf("Fmt: format %q has %d placeholders but got %d parts",
			fmtStr, len(segments)-1, len(parts)))
	}

	result := make([]Extractor, 0, len(segments)+len(parts))

	for i, seg := range segments {
		if seg != "" {
			result = append(result, Const(seg))
		}
		if i < len(parts) {
			result = append(result, parts[i])
		}
	}

	return FormatExpr{Parts: result}
}
