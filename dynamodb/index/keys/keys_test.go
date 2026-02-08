package keys_test

import (
	"testing"

	"github.com/acksell/bezos/dynamodb/keys"
	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestFmt(t *testing.T) {
	tests := []struct {
		name    string
		format  keys.Extractor
		item    map[string]types.AttributeValue
		want    string
		wantErr bool
	}{
		{
			name:   "simple format",
			format: keys.Fmt("USER#%s", keys.Field("userID")),
			item: map[string]types.AttributeValue{
				"userID": &types.AttributeValueMemberS{Value: "123"},
			},
			want: "USER#123",
		},
		{
			name:   "multiple placeholders",
			format: keys.Fmt("ORDER#%s#%s", keys.Field("tenant"), keys.Field("id")),
			item: map[string]types.AttributeValue{
				"tenant": &types.AttributeValueMemberS{Value: "acme"},
				"id":     &types.AttributeValueMemberS{Value: "456"},
			},
			want: "ORDER#acme#456",
		},
		{
			name:   "nested field",
			format: keys.Fmt("USER#%s", keys.Field("user", "id")),
			item: map[string]types.AttributeValue{
				"user": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: "nested-123"},
					},
				},
			},
			want: "USER#nested-123",
		},
		{
			name:   "numeric field value",
			format: keys.Fmt("ITEM#%s", keys.Field("count")),
			item: map[string]types.AttributeValue{
				"count": &types.AttributeValueMemberN{Value: "42"},
			},
			want: "ITEM#42",
		},
		{
			name:   "missing field",
			format: keys.Fmt("USER#%s", keys.Field("missing")),
			item: map[string]types.AttributeValue{
				"other": &types.AttributeValueMemberS{Value: "value"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.format.Extract(tt.item)
			if (err != nil) != tt.wantErr {
				t.Errorf("Extract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			gotStr, ok := got.(string)
			if !ok {
				t.Errorf("Extract() returned %T, want string", got)
				return
			}
			if gotStr != tt.want {
				t.Errorf("Extract() = %q, want %q", gotStr, tt.want)
			}
		})
	}
}

func TestConst(t *testing.T) {
	extractor := keys.Const("PROFILE")
	item := map[string]types.AttributeValue{}

	got, err := extractor.Extract(item)
	if err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	if got != "PROFILE" {
		t.Errorf("Extract() = %q, want %q", got, "PROFILE")
	}
}

func TestField(t *testing.T) {
	tests := []struct {
		name    string
		format  keys.Extractor
		item    map[string]types.AttributeValue
		want    string
		wantErr bool
	}{
		{
			name:   "top-level field",
			format: keys.Field("createdAt"),
			item: map[string]types.AttributeValue{
				"createdAt": &types.AttributeValueMemberS{Value: "2024-01-01"},
			},
			want: "2024-01-01",
		},
		{
			name:   "nested field",
			format: keys.Field("user", "id"),
			item: map[string]types.AttributeValue{
				"user": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: "nested-123"},
					},
				},
			},
			want: "nested-123",
		},
		{
			name:   "missing field",
			format: keys.Field("missing"),
			item: map[string]types.AttributeValue{
				"other": &types.AttributeValueMemberS{Value: "value"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.format.Extract(tt.item)
			if (err != nil) != tt.wantErr {
				t.Errorf("Extract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			gotStr, ok := got.(string)
			if !ok {
				t.Errorf("Extract() returned %T, want string", got)
				return
			}
			if gotStr != tt.want {
				t.Errorf("Extract() = %q, want %q", gotStr, tt.want)
			}
		})
	}
}

func TestKey_AllKeyKinds(t *testing.T) {
	item := map[string]types.AttributeValue{
		"userID":    &types.AttributeValueMemberS{Value: "123"},
		"count":     &types.AttributeValueMemberN{Value: "42"},
		"binaryKey": &types.AttributeValueMemberB{Value: []byte("binary-data")},
	}

	tests := []struct {
		name     string
		key      keys.Key
		wantType string
		wantVal  string
	}{
		{
			name: "string key (S)",
			key: keys.Key{
				Def:       table.KeyDef{Name: "pk", Kind: table.KeyKindS},
				Extractor: keys.Fmt("USER#%s", keys.Field("userID")),
			},
			wantType: "S",
			wantVal:  "USER#123",
		},
		{
			name: "number key (N) from numeric field",
			key: keys.Key{
				Def:       table.KeyDef{Name: "pk", Kind: table.KeyKindN},
				Extractor: keys.Field("count"),
			},
			wantType: "N",
			wantVal:  "42",
		},
		{
			name: "binary key (B) from binary field",
			key: keys.Key{
				Def:       table.KeyDef{Name: "pk", Kind: table.KeyKindB},
				Extractor: keys.Field("binaryKey"),
			},
			wantType: "B",
			wantVal:  "binary-data",
		},
		{
			name: "default key kind (empty = S)",
			key: keys.Key{
				Def:       table.KeyDef{Name: "pk", Kind: ""},
				Extractor: keys.Const("default"),
			},
			wantType: "S",
			wantVal:  "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.key.FromItem(item)
			if err != nil {
				t.Fatalf("FromItem() error = %v", err)
			}

			switch tt.wantType {
			case "S":
				av, ok := got.(*types.AttributeValueMemberS)
				if !ok {
					t.Errorf("got type %T, want *types.AttributeValueMemberS", got)
					return
				}
				if av.Value != tt.wantVal {
					t.Errorf("got value %q, want %q", av.Value, tt.wantVal)
				}
			case "N":
				av, ok := got.(*types.AttributeValueMemberN)
				if !ok {
					t.Errorf("got type %T, want *types.AttributeValueMemberN", got)
					return
				}
				if av.Value != tt.wantVal {
					t.Errorf("got value %q, want %q", av.Value, tt.wantVal)
				}
			case "B":
				av, ok := got.(*types.AttributeValueMemberB)
				if !ok {
					t.Errorf("got type %T, want *types.AttributeValueMemberB", got)
					return
				}
				if string(av.Value) != tt.wantVal {
					t.Errorf("got value %q, want %q", string(av.Value), tt.wantVal)
				}
			}
		})
	}
}
