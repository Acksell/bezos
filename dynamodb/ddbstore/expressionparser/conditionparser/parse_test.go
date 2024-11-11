package conditionparser

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestEvalCondition(t *testing.T) {
	testCases := []struct {
		name      string
		cond      expression.ConditionBuilder
		doc       map[string]types.AttributeValue
		expected  bool
		shouldErr bool
	}{
		{
			name: "simple condition",
			cond: expression.AttributeExists(expression.Name("id")).And(expression.Name("id").Equal(expression.Value("123"))),
			doc: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "123"},
			},
			expected: true,
		},
		{
			name: "simple negative condition",
			cond: expression.AttributeExists(expression.Name("nope")).And(expression.Name("id").Equal(expression.Value("123"))),
			doc: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "123"},
			},
			expected:  false,
			shouldErr: true,
		},
		{
			name: "nested path",
			cond: expression.Name("nested.path").AttributeExists().And(expression.Name("nested.path").Equal(expression.Value("123"))),
			doc: map[string]types.AttributeValue{
				"nested": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"path": &types.AttributeValueMemberS{Value: "123"},
					},
				},
			},
			expected: true,
		},
		{
			name: "nested path with list",
			cond: expression.Name("nested.path[0]").AttributeExists().And(expression.Name("nested.path[0]").Equal(expression.Value("123"))),
			doc: map[string]types.AttributeValue{
				"nested": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"path": &types.AttributeValueMemberL{
							Value: []types.AttributeValue{
								&types.AttributeValueMemberS{Value: "123"},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "too long nested path",
			cond: expression.Name("nested.path[1].another.path").AttributeExists().And(expression.Name("nested.path[0].another.path").Equal(expression.Value("123"))),
			doc: map[string]types.AttributeValue{
				"nested": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"path": &types.AttributeValueMemberL{
							Value: []types.AttributeValue{
								&types.AttributeValueMemberS{
									Value: "123",
								},
								&types.AttributeValueMemberS{
									Value: "456",
								},
							},
						},
					},
				},
			},
			expected:  false,
			shouldErr: true,
		},
		{
			name: "single expression attribute name comparison should work",
			// aliasedName will be converted to an expression attribute name by the expression builder.
			cond: expression.Name("aliasedName").Equal(expression.Value("123")),
			doc: map[string]types.AttributeValue{
				"aliasedName": &types.AttributeValueMemberS{Value: "123"},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fmt.Println("--------TEST CASE:", tc.name)
			b := expression.NewBuilder().WithCondition(tc.cond)
			expr, err := b.Build()
			if err != nil {
				t.Fatalf("failed to build expression: %v", err)
			}
			cond := ConditionInput{
				Condition:        *expr.Condition(),
				ExpressionNames:  expr.Names(),
				ExpressionValues: expr.Values(),
			}
			fmt.Println("CONDITION:", cond.Condition, "NAMES:", cond.ExpressionNames, "VALUES:", cond.ExpressionValues)

			valid, err := EvalCondition(cond, tc.doc)
			if err != nil && !tc.shouldErr {
				t.Fatalf("failed to validate condition: %v", err)
			} else if err == nil && tc.shouldErr {
				t.Fatalf("expected error, got nil")
			}

			require.Equal(t, tc.expected, valid, "condition outcome")
		})
	}
}
