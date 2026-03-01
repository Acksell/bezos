package val

import (
	"encoding/base64"

	"golang.org/x/exp/constraints"
)

// ValDef defines how to derive a key value.
// Exactly one of Format, FromField, or Const should be set.
type ValDef struct {
	// Format specifies a format pattern for the key value.
	// Created via keys.Fmt(), keys.NumFmt(), or keys.ByteFmt().
	Format *FmtSpec

	// FromField specifies that the key value should be copied directly
	// from the named field on the entity. Supports dot notation for
	// nested fields (e.g., "user.id").
	// Created via keys.FromField().
	FromField string

	// Const specifies a constant value for the key.
	// Created via keys.String(), keys.Number(), or keys.Bytes().
	Const *ConstValue
}

// Ptr returns a pointer to a copy of this ValDef.
// Useful for optional sort keys.
func (v ValDef) Ptr() *ValDef {
	return &v
}

// HasValueSource returns true if the ValDef has a value source defined.
func (v ValDef) HasValueSource() bool {
	return v.Format != nil || v.FromField != "" || v.Const != nil
}

// IsZero returns true if this is a zero-value (uninitialized) ValDef.
func (v ValDef) IsZero() bool {
	return !v.HasValueSource()
}

// ConstValue represents a constant key value.
type ConstValue struct {
	Kind  SpecKind // DynamoDB attribute type (S, N, B)
	Value any      // string, numeric, or []byte
}

// String creates a ValDef with a constant string value.
func String(v string) ValDef {
	return ValDef{Const: &ConstValue{Kind: SpecKindS, Value: v}}
}

// Numeric is a constraint for all numeric types.
type Numeric interface {
	constraints.Integer | constraints.Float
}

// Number creates a ValDef with a constant numeric value.
// Accepts any numeric type (int, float64, etc.).
func Number[T Numeric](v T) ValDef {
	return ValDef{Const: &ConstValue{Kind: SpecKindN, Value: v}}
}

// Bytes creates a ValDef with a constant binary value.
// The input should be a base64-encoded string.
func Bytes(b64 string) ValDef {
	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		panic("val.Bytes: invalid base64 string: " + err.Error())
	}
	return ValDef{Const: &ConstValue{Kind: SpecKindB, Value: decoded}}
}

// GetKind returns the DynamoDB attribute type for this constant.
func (c *ConstValue) GetKind() SpecKind {
	return c.Kind
}

// GetValue returns the constant value.
func (c *ConstValue) GetValue() any {
	return c.Value
}
