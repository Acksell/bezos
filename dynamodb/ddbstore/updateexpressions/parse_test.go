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
		if v, ok := result.Item["foo"].(*types.AttributeValueMemberS); !ok || v.Value != "hello" {
			t.Errorf("Expected foo='hello', got %v", result.Item["foo"])
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
		if v, ok := result.Item["counter"].(*types.AttributeValueMemberN); !ok || v.Value != "15" {
			t.Errorf("Expected counter=15, got %v", result.Item["counter"])
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
		if v, ok := result.Item["foo"].(*types.AttributeValueMemberS); !ok || v.Value != "existing" {
			t.Errorf("Expected foo='existing', got %v", result.Item["foo"])
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
		if v, ok := result.Item["foo"].(*types.AttributeValueMemberS); !ok || v.Value != "default" {
			t.Errorf("Expected foo='default', got %v", result.Item["foo"])
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
		list, ok := result.Item["myList"].(*types.AttributeValueMemberL)
		if !ok {
			t.Fatalf("Expected list, got %T", result.Item["myList"])
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

	if _, ok := result.Item["foo"]; ok {
		t.Errorf("Expected foo to be removed")
	}
	if _, ok := result.Item["bar"]; !ok {
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
		if v, ok := result.Item["counter"].(*types.AttributeValueMemberN); !ok || v.Value != "15" {
			t.Errorf("Expected counter=15, got %v", result.Item["counter"])
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
		set, ok := result.Item["mySet"].(*types.AttributeValueMemberSS)
		if !ok {
			t.Fatalf("Expected string set, got %T", result.Item["mySet"])
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
		if v, ok := result.Item["counter"].(*types.AttributeValueMemberN); !ok || v.Value != "5" {
			t.Errorf("Expected counter=5, got %v", result.Item["counter"])
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

	set, ok := result.Item["mySet"].(*types.AttributeValueMemberSS)
	if !ok {
		t.Fatalf("Expected string set, got %T", result.Item["mySet"])
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

func TestReturnValues(t *testing.T) {
	tests := []struct {
		name         string
		expr         string
		names        map[string]string
		oldItem      map[string]types.AttributeValue
		returnValues types.ReturnValue
		wantAttrs    map[string]bool // attribute names we expect in ReturnAttributes
	}{
		{
			name:  "ALL_OLD returns entire old item",
			expr:  "SET #c = #c + :inc",
			names: map[string]string{"#c": "counter"},
			oldItem: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "id1"},
				"counter": &types.AttributeValueMemberN{Value: "10"},
				"myname":  &types.AttributeValueMemberS{Value: "test"},
			},
			returnValues: types.ReturnValueAllOld,
			wantAttrs:    map[string]bool{"pk": true, "counter": true, "myname": true},
		},
		{
			name:  "ALL_NEW returns entire new item",
			expr:  "SET #c = #c + :inc",
			names: map[string]string{"#c": "counter"},
			oldItem: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "id1"},
				"counter": &types.AttributeValueMemberN{Value: "10"},
				"myname":  &types.AttributeValueMemberS{Value: "test"},
			},
			returnValues: types.ReturnValueAllNew,
			wantAttrs:    map[string]bool{"pk": true, "counter": true, "myname": true},
		},
		{
			name:  "UPDATED_OLD returns only updated attrs from old",
			expr:  "SET #c = #c + :inc",
			names: map[string]string{"#c": "counter"},
			oldItem: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "id1"},
				"counter": &types.AttributeValueMemberN{Value: "10"},
				"myname":  &types.AttributeValueMemberS{Value: "test"},
			},
			returnValues: types.ReturnValueUpdatedOld,
			wantAttrs:    map[string]bool{"counter": true},
		},
		{
			name:  "UPDATED_NEW returns only updated attrs from new",
			expr:  "SET #c = #c + :inc",
			names: map[string]string{"#c": "counter"},
			oldItem: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "id1"},
				"counter": &types.AttributeValueMemberN{Value: "10"},
				"myname":  &types.AttributeValueMemberS{Value: "test"},
			},
			returnValues: types.ReturnValueUpdatedNew,
			wantAttrs:    map[string]bool{"counter": true},
		},
		{
			name:  "UPDATED_OLD with nested path returns top-level attr",
			expr:  "SET #u.profile.age = :age",
			names: map[string]string{"#u": "userdata"},
			oldItem: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "id1"},
				"userdata": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"profile": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
						"age": &types.AttributeValueMemberN{Value: "25"},
					}},
				}},
				"myname": &types.AttributeValueMemberS{Value: "test"},
			},
			returnValues: types.ReturnValueUpdatedOld,
			wantAttrs:    map[string]bool{"userdata": true},
		},
		{
			name:  "UPDATED_NEW with multiple attrs returns all touched",
			expr:  "SET #c = #c + :inc REMOVE myname",
			names: map[string]string{"#c": "counter"},
			oldItem: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "id1"},
				"counter": &types.AttributeValueMemberN{Value: "10"},
				"myname":  &types.AttributeValueMemberS{Value: "test"},
				"other":   &types.AttributeValueMemberS{Value: "untouched"},
			},
			returnValues: types.ReturnValueUpdatedNew,
			wantAttrs:    map[string]bool{"counter": true}, // myname was removed, so won't be in new
		},
		{
			name:  "NONE returns nil",
			expr:  "SET #c = #c + :inc",
			names: map[string]string{"#c": "counter"},
			oldItem: map[string]types.AttributeValue{
				"counter": &types.AttributeValueMemberN{Value: "10"},
			},
			returnValues: types.ReturnValueNone,
			wantAttrs:    nil,
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
					":inc": &types.AttributeValueMemberN{Value: "5"},
					":age": &types.AttributeValueMemberN{Value: "30"},
				},
				ReturnValues: tt.returnValues,
			}

			result, err := Apply(expr, input, tt.oldItem)
			if err != nil {
				t.Fatalf("Apply() error = %v", err)
			}

			if tt.wantAttrs == nil {
				if result.ReturnAttributes != nil {
					t.Errorf("Expected nil ReturnAttributes, got %v", result.ReturnAttributes)
				}
				return
			}

			if len(result.ReturnAttributes) != len(tt.wantAttrs) {
				t.Errorf("Expected %d return attributes, got %d: %v",
					len(tt.wantAttrs), len(result.ReturnAttributes), result.ReturnAttributes)
			}

			for name := range tt.wantAttrs {
				if _, ok := result.ReturnAttributes[name]; !ok {
					t.Errorf("Expected attribute %q in return, but not found", name)
				}
			}

			// Verify no unexpected attributes
			for name := range result.ReturnAttributes {
				if !tt.wantAttrs[name] {
					t.Errorf("Unexpected attribute %q in return", name)
				}
			}
		})
	}
}

func TestReturnValuesVerifyContent(t *testing.T) {
	t.Run("UPDATED_OLD contains old value, UPDATED_NEW contains new value", func(t *testing.T) {
		expr, err := Parse("SET #c = #c + :inc")
		if err != nil {
			t.Fatalf("Parse() error = %v", err)
		}

		oldItem := map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "id1"},
			"counter": &types.AttributeValueMemberN{Value: "10"},
		}

		// Test UPDATED_OLD
		resultOld, err := Apply(expr, EvalInput{
			ExpressionNames: map[string]string{"#c": "counter"},
			ExpressionValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "5"},
			},
			ReturnValues: types.ReturnValueUpdatedOld,
		}, oldItem)
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}

		oldCounter, ok := resultOld.ReturnAttributes["counter"].(*types.AttributeValueMemberN)
		if !ok {
			t.Fatalf("Expected counter in UPDATED_OLD return")
		}
		if oldCounter.Value != "10" {
			t.Errorf("UPDATED_OLD counter = %q, want %q", oldCounter.Value, "10")
		}

		// Test UPDATED_NEW with same expression
		resultNew, err := Apply(expr, EvalInput{
			ExpressionNames: map[string]string{"#c": "counter"},
			ExpressionValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "5"},
			},
			ReturnValues: types.ReturnValueUpdatedNew,
		}, oldItem)
		if err != nil {
			t.Fatalf("Apply() error = %v", err)
		}

		newCounter, ok := resultNew.ReturnAttributes["counter"].(*types.AttributeValueMemberN)
		if !ok {
			t.Fatalf("Expected counter in UPDATED_NEW return")
		}
		if newCounter.Value != "15" {
			t.Errorf("UPDATED_NEW counter = %q, want %q", newCounter.Value, "15")
		}
	})
}
