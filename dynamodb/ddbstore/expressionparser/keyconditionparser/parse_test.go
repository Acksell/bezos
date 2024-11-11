package keyconditionparser

import (
	"bezos/dynamodb/ddbstore/expressionparser/keyconditionast"
	"bezos/dynamodb/table"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/stretchr/testify/require"
)

var singleTableKeys = table.PrimaryKeyDefinition{
	PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
	SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
}

var tableKeysOnlyPK = table.PrimaryKeyDefinition{
	PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
}

func TestKeyCondition(t *testing.T) {
	tests := []struct {
		name       string
		cond       expression.KeyConditionBuilder
		keyNames   table.PrimaryKeyDefinition
		wantPK     any
		isvalid    bool
		wantSKCond *keyconditionast.SortKeyCondition
	}{
		{
			name:     "correct pk",
			cond:     expression.KeyEqual(expression.Key("pk"), expression.Value("abc")),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
		},
		{
			name:     "correct pk - with only pk name in table",
			cond:     expression.KeyEqual(expression.Key("pk"), expression.Value("abc")),
			keyNames: tableKeysOnlyPK,
			wantPK:   "abc",
			isvalid:  true,
		},
		{
			name:     "incorrect pk",
			cond:     expression.KeyEqual(expression.Key("badpk"), expression.Value("abc")),
			keyNames: tableKeysOnlyPK,
			isvalid:  false,
		},
		{
			name:     "incorrect pk with sk specified",
			cond:     expression.KeyEqual(expression.Key("badpk"), expression.Value("abc")).And(expression.Key("sk").Equal(expression.Value("abc"))),
			keyNames: singleTableKeys,
			isvalid:  false,
		},
		{
			name:     "missing pk - only sk specified",
			cond:     expression.KeyEqual(expression.Key("sk"), expression.Value("abc")),
			keyNames: singleTableKeys,
			isvalid:  false,
		},
		{
			name:     "correct pk and valid sk",
			cond:     expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("sk").Equal(expression.Value("def"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
			wantSKCond: &keyconditionast.SortKeyCondition{
				Compare: &keyconditionast.KeyComparison{
					KeyName: keyconditionast.NewExpressionAttributeName("#1", "sk"),
					Comp:    keyconditionast.Equal,
					Value:   keyconditionast.NewExpressionAttributeValue(":1", keyconditionast.KeyValue{Value: "def", Type: keyconditionast.STRING}),
				},
			},
		},
		{
			name:     "correct pk but invalid sk",
			cond:     expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("badsk").Equal(expression.Value("def"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  false,
		},
		{
			name:     "correct pk but passed sk to table without sk",
			cond:     expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("badsk").Equal(expression.Value("def"))),
			keyNames: tableKeysOnlyPK,
			wantPK:   "abc",
			isvalid:  false,
		},
		{
			name:     "begins_with sk condition",
			cond:     expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("sk").BeginsWith("def")),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
			wantSKCond: &keyconditionast.SortKeyCondition{
				BeginsWith: &keyconditionast.KeyBeginsWith{
					KeyName: keyconditionast.NewExpressionAttributeName("#1", "sk"),
					Prefix:  keyconditionast.NewExpressionAttributeValue(":1", keyconditionast.KeyValue{Value: "def", Type: keyconditionast.STRING}),
				},
			},
		},
		{
			name:     "begins_with bad sk condition",
			cond:     expression.KeyEqual(expression.Key("pk"), expression.Value("abc")).And(expression.Key("badsk").BeginsWith("def")),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  false,
		},
		{
			name:     "begins_with on pk should be invalid",
			cond:     expression.Key("pk").BeginsWith("abc"),
			keyNames: tableKeysOnlyPK,
			wantPK:   "abc",
			isvalid:  false,
		},
		{
			name:     "begins_with on pk should be invalid even with sk",
			cond:     expression.Key("sk").Equal(expression.Value("def")).And(expression.KeyBeginsWith(expression.Key("pk"), "abc")),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  false,
		},
		{
			name:     "between sk condition",
			cond:     expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").Between(expression.Value("123"), expression.Value("456"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
			wantSKCond: &keyconditionast.SortKeyCondition{
				Between: &keyconditionast.KeyBetween{
					KeyName: keyconditionast.NewExpressionAttributeName("#1", "sk"),
					Lower:   keyconditionast.NewExpressionAttributeValue(":1", keyconditionast.KeyValue{Value: "123", Type: keyconditionast.STRING}),
					Upper:   keyconditionast.NewExpressionAttributeValue(":2", keyconditionast.KeyValue{Value: "456", Type: keyconditionast.STRING}),
				},
			},
		},
		{
			name:     "between with invalid sk condition",
			cond:     expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("badsk").Between(expression.Value("123"), expression.Value("456"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  false,
		},
		{
			name:     "greater than",
			cond:     expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").GreaterThan(expression.Value("123"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
			wantSKCond: &keyconditionast.SortKeyCondition{
				Compare: &keyconditionast.KeyComparison{
					KeyName: keyconditionast.NewExpressionAttributeName("#1", "sk"),
					Comp:    keyconditionast.GreaterThan,
					Value:   keyconditionast.NewExpressionAttributeValue(":1", keyconditionast.KeyValue{Value: "123", Type: keyconditionast.STRING}),
				},
			},
		},
		{
			name:     "greater than equal",
			cond:     expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").GreaterThanEqual(expression.Value("123"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
			wantSKCond: &keyconditionast.SortKeyCondition{
				Compare: &keyconditionast.KeyComparison{
					KeyName: keyconditionast.NewExpressionAttributeName("#1", "sk"),
					Comp:    keyconditionast.GreaterOrEqual,
					Value:   keyconditionast.NewExpressionAttributeValue(":1", keyconditionast.KeyValue{Value: "123", Type: keyconditionast.STRING}),
				},
			},
		},
		{
			name:     "less than equal",
			cond:     expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").LessThanEqual(expression.Value("123"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
			wantSKCond: &keyconditionast.SortKeyCondition{
				Compare: &keyconditionast.KeyComparison{
					KeyName: keyconditionast.NewExpressionAttributeName("#1", "sk"),
					Comp:    keyconditionast.LessOrEqual,
					Value:   keyconditionast.NewExpressionAttributeValue(":1", keyconditionast.KeyValue{Value: "123", Type: keyconditionast.STRING}),
				},
			},
		},
		{
			name:     "less than",
			cond:     expression.Key("pk").Equal(expression.Value("abc")).And(expression.Key("sk").LessThan(expression.Value("123"))),
			keyNames: singleTableKeys,
			wantPK:   "abc",
			isvalid:  true,
			wantSKCond: &keyconditionast.SortKeyCondition{
				Compare: &keyconditionast.KeyComparison{
					KeyName: keyconditionast.NewExpressionAttributeName("#1", "sk"),
					Comp:    keyconditionast.LessThan,
					Value:   keyconditionast.NewExpressionAttributeValue(":1", keyconditionast.KeyValue{Value: "123", Type: keyconditionast.STRING}),
				},
			},
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
			in := KeyConditionParams{
				TableKeys:                 tt.keyNames,
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
			}
			fmt.Println("-----------------v", tt.name, "*expr.KeyCondition::=", *expr.KeyCondition())
			cond, err := ParseKeyCondition(*expr.KeyCondition(), in)
			if err != nil && tt.isvalid {
				t.Fatalf("unexpected error: %v", err)
			}
			if err == nil && !tt.isvalid {
				t.Fatalf("expected error, got nil")
			}
			if tt.isvalid {
				require.Equal(t, tt.wantPK, cond.PartitionKeyCond.EqualsValue.GetValue().Value)
				require.NotNil(t, cond, "expected non-nil condition")
				if tt.wantSKCond != nil {
					require.NotNil(t, cond.SortKeyCond, "expected non-nil SortKeyCondition")
					require.Equal(t, tt.wantSKCond, cond.SortKeyCond)
				}
			}
		})
	}
}
