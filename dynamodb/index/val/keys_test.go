package val_test

import (
	"testing"

	"github.com/acksell/bezos/dynamodb/index/val"
)

func TestFmt(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{"simple format", "USER#{userID}"},
		{"multiple placeholders", "ORDER#{tenant}#{id}"},
		{"nested field", "USER#{user.id}"},
		{"constant pattern", "PROFILE"},
		{"single field", "{createdAt}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := val.Fmt(tt.pattern)
			if v.Format == nil {
				t.Fatal("Format should not be nil")
			}
			if v.Format.String() != tt.pattern {
				t.Errorf("Format.String() = %q, want %q", v.Format.String(), tt.pattern)
			}
		})
	}
}

func TestFmt_Panics(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{"empty pattern", ""},
		{"empty field ref", "USER#{}"},
		{"empty path component", "USER#{a..b}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Fmt(%q) did not panic", tt.pattern)
				}
			}()
			val.Fmt(tt.pattern)
		})
	}
}

func TestFmtSpec_IsConstant(t *testing.T) {
	tests := []struct {
		pattern string
		want    bool
	}{
		{"PROFILE", true},
		{"USER#{id}", false},
		{"{id}", false},
		{"PREFIX#{a}#{b}SUFFIX", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			v := val.Fmt(tt.pattern)
			if got := v.Format.IsConstant(); got != tt.want {
				t.Errorf("IsConstant() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValDef_IsZero(t *testing.T) {
	var zero val.ValDef
	if !zero.IsZero() {
		t.Error("zero value should be zero")
	}

	v := val.Fmt("USER#{id}")
	if v.IsZero() {
		t.Error("initialized value should not be zero")
	}
}

func TestFmtSpec_FieldRefs(t *testing.T) {
	tests := []struct {
		pattern string
		want    []string
	}{
		{"PROFILE", nil},
		{"USER#{id}", []string{"id"}},
		{"{id}", []string{"id"}},
		{"ORDER#{tenant}#{id}", []string{"tenant", "id"}},
		{"PREFIX#{a.b}#{c}SUFFIX", []string{"a.b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			v := val.Fmt(tt.pattern)
			got := v.Format.FieldRefs()
			if len(got) != len(tt.want) {
				t.Errorf("FieldRefs() = %v, want %v", got, tt.want)
				return
			}
			for i := range got {
				if got[i].Value != tt.want[i] {
					t.Errorf("FieldRefs()[%d].Value = %q, want %q", i, got[i].Value, tt.want[i])
				}
			}
		})
	}
}

func TestFmtSpec_FieldPaths(t *testing.T) {
	tests := []struct {
		pattern string
		want    []string
	}{
		{"PROFILE", nil},
		{"USER#{id}", []string{"id"}},
		{"{user.address.city}", []string{"user.address.city"}},
		{"ORDER#{tenant}#{order.id}", []string{"tenant", "order.id"}},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			v := val.Fmt(tt.pattern)
			got := v.Format.FieldPaths()
			if len(got) != len(tt.want) {
				t.Errorf("FieldPaths() len = %d, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("FieldPaths()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestValDef_Ptr(t *testing.T) {
	v := val.Fmt("PROFILE")
	ptr := v.Ptr()
	if ptr == nil {
		t.Error("Ptr() should not return nil")
	}
	if ptr.Format.String() != "PROFILE" {
		t.Errorf("Ptr().Format.String() = %q, want %q", ptr.Format.String(), "PROFILE")
	}
}

func TestConstantConstructors(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		v := val.String("PROFILE")
		if v.Const == nil {
			t.Fatal("Const should not be nil")
		}
		if v.Const.Value() != "PROFILE" {
			t.Errorf("Value() = %v, want %q", v.Const.Value(), "PROFILE")
		}
	})

	t.Run("Number", func(t *testing.T) {
		v := val.Number(42)
		if v.Const == nil {
			t.Fatal("Const should not be nil")
		}
		if v.Const.Value() != 42 {
			t.Errorf("Value() = %v, want %d", v.Const.Value(), 42)
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		v := val.Bytes("dGVzdA==")
		if v.Const == nil {
			t.Fatal("Const should not be nil")
		}
	})

	t.Run("FromField", func(t *testing.T) {
		v := val.FromField("email")
		if v.FromField != "email" {
			t.Errorf("FromField = %q, want %q", v.FromField, "email")
		}
	})
}
