package updateexpressions

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestParseSetActions(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "simple set", expr: "SET foo = :val"},
		{name: "set with expression attribute name", expr: "SET #name = :val"},
		{name: "set multiple", expr: "SET foo = :val1, bar = :val2"},
		{name: "set with arithmetic add", expr: "SET #counter = #counter + :inc"},
		{name: "set with arithmetic subtract", expr: "SET #counter = #counter - :dec"},
		{name: "set with if_not_exists", expr: "SET foo = if_not_exists(foo, :default)"},
		{name: "set with list_append", expr: "SET myList = list_append(myList, :newItems)"},
		{name: "set with nested path", expr: "SET #user.profile.#name = :name"},
		{name: "set with list index", expr: "SET myList[0] = :val"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(result.SetActions) == 0 {
				t.Errorf("Expected SET actions, got none")
			}
		})
	}
}

func TestParseRemoveActions(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "simple remove", expr: "REMOVE foo"},
		{name: "remove multiple", expr: "REMOVE foo, bar, baz"},
		{name: "remove nested", expr: "REMOVE #user.profile"},
		{name: "remove list element", expr: "REMOVE myList[0]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(result.RemoveActions) == 0 {
				t.Errorf("Expected REMOVE actions, got none")
			}
		})
	}
}

func TestParseAddActions(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "add to number", expr: "ADD #counter :inc"},
		{name: "add to set", expr: "ADD mySet :newElements"},
		{name: "add multiple", expr: "ADD #counter :inc, mySet :newElements"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(result.AddActions) == 0 {
				t.Errorf("Expected ADD actions, got none")
			}
		})
	}
}

func TestParseDeleteActions(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "delete from set", expr: "DELETE mySet :elementsToRemove"},
		{name: "delete multiple", expr: "DELETE set1 :elems1, set2 :elems2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(result.DeleteActions) == 0 {
				t.Errorf("Expected DELETE actions, got none")
			}
		})
	}
}

func TestParseCombinedClauses(t *testing.T) {
	tests := []struct {
		name       string
		expr       string
		wantSet    int
		wantRemove int
		wantAdd    int
		wantDelete int
		wantErr    bool
	}{
		{name: "set and remove", expr: "SET foo = :val REMOVE bar", wantSet: 1, wantRemove: 1},
		{name: "all clauses", expr: "SET foo = :val REMOVE bar ADD #counter :inc DELETE mySet :elems", wantSet: 1, wantRemove: 1, wantAdd: 1, wantDelete: 1},
		{name: "multiple of same type", expr: "SET a = :a, b = :b, c = :c", wantSet: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if len(result.SetActions) != tt.wantSet {
				t.Errorf("SetActions = %d, want %d", len(result.SetActions), tt.wantSet)
			}
			if len(result.RemoveActions) != tt.wantRemove {
				t.Errorf("RemoveActions = %d, want %d", len(result.RemoveActions), tt.wantRemove)
			}
			if len(result.AddActions) != tt.wantAdd {
				t.Errorf("AddActions = %d, want %d", len(result.AddActions), tt.wantAdd)
			}
			if len(result.DeleteActions) != tt.wantDelete {
				t.Errorf("DeleteActions = %d, want %d", len(result.DeleteActions), tt.wantDelete)
			}
		})
	}
}

func TestApplySetActions(t *testing.T) {
	t.Run("simple set", func(t *testing.T) {
		expr, err := Parse("SET foo = :val")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionValues: map[string]types.AttributeValue{
				":val": &types.AttributeValueMemberS{Value: "hello"},
			},
		}
		result, err := Apply(expr, input, map[string]types.AttributeValue{})
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if v, ok := result["foo"].(*types.AttributeValueMemberS); !ok || v.Value != "hello" {
			t.Errorf("Expected foo='hello', got %v", result["foo"])
		}
	})

	t.Run("arithmetic add", func(t *testing.T) {
		expr, err := Parse("SET #counter = #counter + :inc")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionNames: map[string]string{
				"#counter": "counter",
			},
			ExpressionValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "5"},
			},
		}
		doc := map[string]types.AttributeValue{
			"counter": &types.AttributeValueMemberN{Value: "10"},
		}
		result, err := Apply(expr, input, doc)
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if v, ok := result["counter"].(*types.AttributeValueMemberN); !ok || v.Value != "15" {
			t.Errorf("Expected counter=15, got %v", result["counter"])
		}
	})

	t.Run("if_not_exists with existing", func(t *testing.T) {
		expr, err := Parse("SET foo = if_not_exists(foo, :default)")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionValues: map[string]types.AttributeValue{
				":default": &types.AttributeValueMemberS{Value: "default"},
			},
		}
		doc := map[string]types.AttributeValue{
			"foo": &types.AttributeValueMemberS{Value: "existing"},
		}
		result, err := Apply(expr, input, doc)
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if v, ok := result["foo"].(*types.AttributeValueMemberS); !ok || v.Value != "existing" {
			t.Errorf("Expected foo='existing', got %v", result["foo"])
		}
	})

	t.Run("if_not_exists with missing", func(t *testing.T) {
		expr, err := Parse("SET foo = if_not_exists(foo, :default)")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionValues: map[string]types.AttributeValue{
				":default": &types.AttributeValueMemberS{Value: "default"},
			},
		}
		result, err := Apply(expr, input, map[string]types.AttributeValue{})
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if v, ok := result["foo"].(*types.AttributeValueMemberS); !ok || v.Value != "default" {
			t.Errorf("Expected foo='default', got %v", result["foo"])
		}
	})

	t.Run("list_append", func(t *testing.T) {
		expr, err := Parse("SET myList = list_append(myList, :newItems)")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionValues: map[string]types.AttributeValue{
				":newItems": &types.AttributeValueMemberL{Value: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "c"},
				}},
			},
		}
		doc := map[string]types.AttributeValue{
			"myList": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "a"},
				&types.AttributeValueMemberS{Value: "b"},
			}},
		}
		result, err := Apply(expr, input, doc)
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		list, ok := result["myList"].(*types.AttributeValueMemberL)
		if !ok {
			t.Fatalf("Expected list, got %T", result["myList"])
		}
		if len(list.Value) != 3 {
			t.Errorf("Expected 3 elements, got %d", len(list.Value))
		}
	})
}

func TestApplyRemoveActions(t *testing.T) {
	expr, err := Parse("REMOVE foo")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	doc := map[string]types.AttributeValue{
		"foo": &types.AttributeValueMemberS{Value: "hello"},
		"bar": &types.AttributeValueMemberS{Value: "world"},
	}

	result, err := Apply(expr, EvalInput{}, doc)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if _, ok := result["foo"]; ok {
		t.Errorf("Expected foo to be removed")
	}
	if _, ok := result["bar"]; !ok {
		t.Errorf("Expected bar to remain")
	}
}

func TestApplyAddActions(t *testing.T) {
	t.Run("add to number", func(t *testing.T) {
		expr, err := Parse("ADD #counter :inc")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionNames: map[string]string{
				"#counter": "counter",
			},
			ExpressionValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "5"},
			},
		}
		doc := map[string]types.AttributeValue{
			"counter": &types.AttributeValueMemberN{Value: "10"},
		}
		result, err := Apply(expr, input, doc)
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if v, ok := result["counter"].(*types.AttributeValueMemberN); !ok || v.Value != "15" {
			t.Errorf("Expected counter=15, got %v", result["counter"])
		}
	})

	t.Run("add to string set", func(t *testing.T) {
		expr, err := Parse("ADD mySet :elems")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionValues: map[string]types.AttributeValue{
				":elems": &types.AttributeValueMemberSS{Value: []string{"c", "d"}},
			},
		}
		doc := map[string]types.AttributeValue{
			"mySet": &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
		}
		result, err := Apply(expr, input, doc)
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		set, ok := result["mySet"].(*types.AttributeValueMemberSS)
		if !ok {
			t.Fatalf("Expected string set, got %T", result["mySet"])
		}
		if len(set.Value) != 4 {
			t.Errorf("Expected 4 elements, got %d", len(set.Value))
		}
	})

	t.Run("add creates new attribute", func(t *testing.T) {
		expr, err := Parse("ADD #counter :inc")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}
		input := EvalInput{
			ExpressionNames: map[string]string{
				"#counter": "counter",
			},
			ExpressionValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "5"},
			},
		}
		result, err := Apply(expr, input, map[string]types.AttributeValue{})
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
		if v, ok := result["counter"].(*types.AttributeValueMemberN); !ok || v.Value != "5" {
			t.Errorf("Expected counter=5, got %v", result["counter"])
		}
	})
}

func TestApplyDeleteActions(t *testing.T) {
	expr, err := Parse("DELETE mySet :elems")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	input := EvalInput{
		ExpressionValues: map[string]types.AttributeValue{
			":elems": &types.AttributeValueMemberSS{Value: []string{"b", "d"}},
		},
	}

	doc := map[string]types.AttributeValue{
		"mySet": &types.AttributeValueMemberSS{Value: []string{"a", "b", "c", "d", "e"}},
	}

	result, err := Apply(expr, input, doc)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	set, ok := result["mySet"].(*types.AttributeValueMemberSS)
	if !ok {
		t.Fatalf("Expected string set, got %T", result["mySet"])
	}

	if len(set.Value) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(set.Value))
	}
}

func TestPathOverlapValidation(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		names   map[string]string
		wantErr bool
	}{
		{
			name:    "same path SET and REMOVE",
			expr:    "SET foo = :val REMOVE foo",
			wantErr: true,
		},
		{
			name:    "nested path overlap SET and REMOVE",
			expr:    "SET foo = :val REMOVE foo.bar",
			wantErr: true,
		},
		{
			name:    "nested path overlap reverse",
			expr:    "SET foo.bar = :val REMOVE foo",
			wantErr: true,
		},
		{
			name:    "same path SET and ADD",
			expr:    "SET #counter = :val ADD #counter :inc",
			names:   map[string]string{"#counter": "counter"},
			wantErr: true,
		},
		{
			name:    "same path REMOVE and DELETE",
			expr:    "REMOVE mySet DELETE mySet :elems",
			wantErr: true,
		},
		{
			name:    "different paths - no overlap",
			expr:    "SET foo = :val REMOVE bar",
			wantErr: false,
		},
		{
			name:    "similar names but no overlap",
			expr:    "SET foobar = :val REMOVE foo",
			wantErr: false,
		},
		{
			name:    "list index overlap",
			expr:    "SET myList[0] = :val REMOVE myList",
			wantErr: true,
		},
		{
			name:    "different list indices - allowed",
			expr:    "SET myList[0] = :val REMOVE myList[1]",
			wantErr: false,
		},
		{
			name:    "same clause same path - allowed",
			expr:    "SET foo = :val1, foo = :val2",
			wantErr: false,
		},
		{
			name:    "expression attr name resolves to overlap",
			expr:    "SET #a = :val REMOVE bar",
			names:   map[string]string{"#a": "bar"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.expr)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			input := EvalInput{
				ExpressionNames: tt.names,
				ExpressionValues: map[string]types.AttributeValue{
					":val":   &types.AttributeValueMemberS{Value: "test"},
					":val1":  &types.AttributeValueMemberS{Value: "test1"},
					":val2":  &types.AttributeValueMemberS{Value: "test2"},
					":inc":   &types.AttributeValueMemberN{Value: "1"},
					":elems": &types.AttributeValueMemberSS{Value: []string{"a"}},
				},
			}
			_, err = Apply(expr, input, map[string]types.AttributeValue{
				"foo":     &types.AttributeValueMemberS{Value: "old"},
				"bar":     &types.AttributeValueMemberS{Value: "old"},
				"foobar":  &types.AttributeValueMemberS{Value: "old"},
				"counter": &types.AttributeValueMemberN{Value: "0"},
				"mySet":   &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
				"myList": &types.AttributeValueMemberL{Value: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "a"},
					&types.AttributeValueMemberS{Value: "b"},
				}},
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil {
				t.Logf("Got expected error: %v", err)
			}
		})
	}
}
