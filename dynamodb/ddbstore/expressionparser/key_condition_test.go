package expressionparser

import (
	"bezos/dynamodb/ddbstore/expressionparser/ast"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/stretchr/testify/require"
)

func TestKeyCondition(t *testing.T) {
	tests := []struct {
		name     string
		cond     expression.KeyConditionBuilder
		keyNames ast.TableKeyNames
		wantPK   any
		isvalid  bool
	}{
		{
			name: "correct pk",
			cond: expression.KeyEqual(expression.Key("pk"), expression.Value("abc")),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "correct pk - with only pk name in table",
			cond: expression.KeyEqual(expression.Key("pk"), expression.Value("abc")),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "incorrect pk",
			cond: expression.KeyEqual(expression.Key("badpk"), expression.Value("abc")),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
			},
			isvalid: false,
		},
		{
			name: "incorrect pk with sk specified",
			cond: expression.KeyEqual(expression.Key("badpk"), expression.Value("abc")).And(expression.Key("sk").Equal(expression.Value("abc"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			isvalid: false,
		},
		{
			name: "missing pk - only sk specified",
			cond: expression.KeyEqual(expression.Key("sk"), expression.Value("abc")),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			isvalid: false,
		},
		{
			name: "correct pk and valid sk",
			cond: expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("sk").Equal(expression.Value("def"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "correct pk but invalid sk",
			cond: expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("badsk").Equal(expression.Value("def"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: false,
		},
		{
			name: "correct pk but passed sk to table without sk",
			cond: expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("badsk").Equal(expression.Value("def"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
			},
			wantPK:  "abc",
			isvalid: false,
		},
		{
			name: "begins_with sk condition",
			cond: expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("sk").BeginsWith("def")),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "begins_with bad sk condition",
			cond: expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("badsk").BeginsWith("def")),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: false,
		},
		{
			name: "begins_with on pk should be invalid",
			cond: expression.Key("pk").BeginsWith("abc"),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
			},
			wantPK:  "abc",
			isvalid: false,
		},
		{
			name: "begins_with on pk should be invalid even with sk",
			cond: expression.Key("sk").Equal(expression.Value("def")).And(expression.KeyBeginsWith(expression.Key("pk"), "abc")),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: false,
		},
		{
			name: "between sk condition",
			cond: expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").Between(expression.Value("123"), expression.Value("456"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "between with invalid sk condition",
			cond: expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("badsk").Between(expression.Value("123"), expression.Value("456"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: false,
		},
		{
			name: "greater than",
			cond: expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").GreaterThan(expression.Value("123"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "greater than equal",
			cond: expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").GreaterThanEqual(expression.Value("123"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "less than equal",
			cond: expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").LessThanEqual(expression.Value("123"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		{
			name: "less than",
			cond: expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").LessThan(expression.Value("123"))),
			keyNames: ast.TableKeyNames{
				PartitionKeyName: "pk",
				SortKeyName:      "sk",
			},
			wantPK:  "abc",
			isvalid: true,
		},
		// {
		// can only test via manaully building the expression, as the builder does not support it
		// 	name: "not equal should not be supported",
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := expression.NewBuilder().WithKeyCondition(tt.cond)
			expr, err := b.Build()
			if err != nil {
				t.Fatalf("failed to build expression: %v", err)
			}
			in := Condition{
				Condition:        *expr.KeyCondition(),
				ExpressionNames:  expr.Names(),
				ExpressionValues: expr.Values(),
			}
			cond, err := ParseCondition(in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			kc, err := AsKeyCondition(cond, ast.Input{
				KeyNames:         tt.keyNames,
				ExpressionNames:  in.ExpressionNames,
				ExpressionValues: convertToASTVals(in.ExpressionValues),
			})
			if tt.isvalid && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.isvalid {
				require.Equal(t, tt.wantPK, kc.PartitionKeyValue)
			}
			if !tt.isvalid && err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}
