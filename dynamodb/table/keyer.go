package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Keyer interface {
	Key(doc map[string]types.AttributeValue) (types.AttributeValue, error)
}

// FmtKeyer tries to find the `keys` in the document being inserted, and passes them to the format string.
// The keys can only be of type string, number, or bytes.
// Keys support nesting by using dot notation, e.g. "meta.version".
//
// The format string should only use %s, not %d. This is because numbers are encoded as strings in dynamo.
// If any key is not found in the document, an empty string is passed instead.
func FmtKeyer(fmt string, keys ...string) *keyFormat {
	return &keyFormat{fmt, keys}
}

type keyFormat struct {
	fmt  string
	keys []string
}

func (k keyFormat) Key(doc map[string]types.AttributeValue) (types.AttributeValue, error) {
	strVals := make([]string, len(k.keys))
	for i, key := range k.keys {
		v, found := doc[key]
		if !found {
			continue
		}
		var val string
		switch attr := v.(type) {
		case *types.AttributeValueMemberS:
			val = attr.Value
		case *types.AttributeValueMemberN:
			val = attr.Value
		case *types.AttributeValueMemberB:
			val = string(attr.Value)
		default:
			return nil, fmt.Errorf("type for key %q is not string, number, or bytes, got %T", key, v)
		}
		strVals[i] = val
	}
	vals := make([]interface{}, len(strVals))
	for i, v := range strVals {
		vals[i] = v
	}
	return &types.AttributeValueMemberS{Value: fmt.Sprintf(k.fmt, vals...)}, nil
}

func CopyKeyer(key string) *copyKey {
	return &copyKey{key}
}

type copyKey struct {
	key string
}

func (k copyKey) Key(doc map[string]types.AttributeValue) (types.AttributeValue, error) {
	v, found := doc[k.key]
	if !found {
		return nil, fmt.Errorf("key %q not found", k.key)
	}
	return v, nil
}

func ConstKeyer(val types.AttributeValue) *constKey {
	return &constKey{val}
}

type constKey struct {
	val types.AttributeValue
}

func (k constKey) Key(map[string]types.AttributeValue) (types.AttributeValue, error) {
	return k.val, nil
}
