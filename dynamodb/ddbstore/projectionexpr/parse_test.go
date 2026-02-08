package projectionexpr

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantPaths []string
		wantErr   bool
	}{
		{
			name:      "single attribute",
			input:     "title",
			wantPaths: []string{"title"},
		},
		{
			name:      "multiple attributes",
			input:     "title, price, authors",
			wantPaths: []string{"title", "price", "authors"},
		},
		{
			name:      "nested attribute",
			input:     "info.rating",
			wantPaths: []string{"info.rating"},
		},
		{
			name:      "deeply nested attribute",
			input:     "ProductReviews.FiveStar.ReviewText",
			wantPaths: []string{"ProductReviews.FiveStar.ReviewText"},
		},
		{
			name:      "list index",
			input:     "authors[0]",
			wantPaths: []string{"authors[0]"},
		},
		{
			name:      "nested with list index",
			input:     "ProductReviews.FiveStar[0].ReviewText",
			wantPaths: []string{"ProductReviews.FiveStar[0].ReviewText"},
		},
		{
			name:      "multiple list indices",
			input:     "matrix[0][1]",
			wantPaths: []string{"matrix[0][1]"},
		},
		{
			name:      "expression attribute name",
			input:     "#yr",
			wantPaths: []string{"#yr"},
		},
		{
			name:      "mixed expression names and regular names",
			input:     "#yr, title, info.rating",
			wantPaths: []string{"#yr", "title", "info.rating"},
		},
		{
			name:      "no whitespace",
			input:     "a,b,c",
			wantPaths: []string{"a", "b", "c"},
		},
		{
			name:      "extra whitespace",
			input:     "  a  ,  b  ,  c  ",
			wantPaths: []string{"a", "b", "c"},
		},
		{
			name:      "underscore in name",
			input:     "my_attribute",
			wantPaths: []string{"my_attribute"},
		},
		{
			name:      "attribute starting with underscore",
			input:     "_private",
			wantPaths: []string{"_private"},
		},
		{
			name:      "complex mixed expression",
			input:     "id, #name, info.details[0].content, metadata.tags[1]",
			wantPaths: []string{"id", "#name", "info.details[0].content", "metadata.tags[1]"},
		},
		{
			name:    "empty expression",
			input:   "",
			wantErr: true,
		},
		{
			name:    "parentheses not supported",
			input:   "(title)",
			wantErr: true,
		},
		{
			name:    "invalid - reserved word without alias",
			input:   "AND",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, expr)

			gotPaths := make([]string, 0, len(expr.Paths))
			for _, p := range expr.Paths {
				gotPaths = append(gotPaths, p.String())
			}

			assert.Equal(t, tt.wantPaths, gotPaths)
		})
	}
}

func TestApply(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		names   map[string]string
		doc     map[string]types.AttributeValue
		want    map[string]types.AttributeValue
		wantErr bool
	}{
		{
			name: "single top-level attribute",
			expr: "title",
			doc: map[string]types.AttributeValue{
				"title":  &types.AttributeValueMemberS{Value: "Book Title"},
				"author": &types.AttributeValueMemberS{Value: "John Doe"},
				"price":  &types.AttributeValueMemberN{Value: "29.99"},
			},
			want: map[string]types.AttributeValue{
				"title": &types.AttributeValueMemberS{Value: "Book Title"},
			},
		},
		{
			name: "multiple top-level attributes",
			expr: "title, price",
			doc: map[string]types.AttributeValue{
				"title":  &types.AttributeValueMemberS{Value: "Book Title"},
				"author": &types.AttributeValueMemberS{Value: "John Doe"},
				"price":  &types.AttributeValueMemberN{Value: "29.99"},
			},
			want: map[string]types.AttributeValue{
				"title": &types.AttributeValueMemberS{Value: "Book Title"},
				"price": &types.AttributeValueMemberN{Value: "29.99"},
			},
		},
		{
			name: "nested attribute",
			expr: "info.rating",
			doc: map[string]types.AttributeValue{
				"title": &types.AttributeValueMemberS{Value: "Book Title"},
				"info": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"rating":   &types.AttributeValueMemberN{Value: "4.5"},
						"reviews":  &types.AttributeValueMemberN{Value: "100"},
						"category": &types.AttributeValueMemberS{Value: "Fiction"},
					},
				},
			},
			want: map[string]types.AttributeValue{
				"info": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"rating": &types.AttributeValueMemberN{Value: "4.5"},
					},
				},
			},
		},
		{
			name: "deeply nested attribute",
			expr: "a.b.c",
			doc: map[string]types.AttributeValue{
				"a": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"b": &types.AttributeValueMemberM{
							Value: map[string]types.AttributeValue{
								"c": &types.AttributeValueMemberS{Value: "deep value"},
								"d": &types.AttributeValueMemberS{Value: "other"},
							},
						},
					},
				},
			},
			want: map[string]types.AttributeValue{
				"a": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"b": &types.AttributeValueMemberM{
							Value: map[string]types.AttributeValue{
								"c": &types.AttributeValueMemberS{Value: "deep value"},
							},
						},
					},
				},
			},
		},
		{
			name: "list element - returns list with single element",
			expr: "authors[0]",
			doc: map[string]types.AttributeValue{
				"authors": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberS{Value: "First Author"},
						&types.AttributeValueMemberS{Value: "Second Author"},
					},
				},
			},
			want: map[string]types.AttributeValue{
				"authors": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberS{Value: "First Author"},
					},
				},
			},
		},
		{
			name:  "expression attribute name",
			expr:  "#yr",
			names: map[string]string{"#yr": "year"},
			doc: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "2024"},
				"month": &types.AttributeValueMemberN{Value: "6"},
			},
			want: map[string]types.AttributeValue{
				"year": &types.AttributeValueMemberN{Value: "2024"},
			},
		},
		{
			name: "missing attribute is silently ignored",
			expr: "title, nonexistent, price",
			doc: map[string]types.AttributeValue{
				"title": &types.AttributeValueMemberS{Value: "Book"},
				"price": &types.AttributeValueMemberN{Value: "10"},
			},
			want: map[string]types.AttributeValue{
				"title": &types.AttributeValueMemberS{Value: "Book"},
				"price": &types.AttributeValueMemberN{Value: "10"},
			},
		},
		{
			name: "nil document returns nil",
			expr: "title",
			doc:  nil,
			want: nil,
		},
		{
			name: "empty document returns empty",
			expr: "title",
			doc:  map[string]types.AttributeValue{},
			want: map[string]types.AttributeValue{},
		},
		{
			name: "multiple nested paths from same parent",
			expr: "info.rating, info.category",
			doc: map[string]types.AttributeValue{
				"info": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"rating":   &types.AttributeValueMemberN{Value: "4.5"},
						"category": &types.AttributeValueMemberS{Value: "Fiction"},
						"pages":    &types.AttributeValueMemberN{Value: "300"},
					},
				},
			},
			want: map[string]types.AttributeValue{
				"info": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"rating":   &types.AttributeValueMemberN{Value: "4.5"},
						"category": &types.AttributeValueMemberS{Value: "Fiction"},
					},
				},
			},
		},
		{
			// Based on AWS DynamoDB documentation example
			name: "DDB doc example - mixed projection",
			expr: "Description, RelatedItems[0], ProductReviews.FiveStar",
			doc: map[string]types.AttributeValue{
				"Id":          &types.AttributeValueMemberN{Value: "123"},
				"Description": &types.AttributeValueMemberS{Value: "123 description"},
				"RelatedItems": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberN{Value: "341"},
						&types.AttributeValueMemberN{Value: "472"},
						&types.AttributeValueMemberN{Value: "649"},
					},
				},
				"ProductReviews": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"FiveStar": &types.AttributeValueMemberL{
							Value: []types.AttributeValue{
								&types.AttributeValueMemberS{Value: "Excellent! Can't recommend it highly enough! Buy it!"},
								&types.AttributeValueMemberS{Value: "Do yourself a favor and buy this."},
							},
						},
						"OneStar": &types.AttributeValueMemberL{
							Value: []types.AttributeValue{
								&types.AttributeValueMemberS{Value: "Terrible product."},
							},
						},
					},
				},
			},
			want: map[string]types.AttributeValue{
				"Description": &types.AttributeValueMemberS{Value: "123 description"},
				"RelatedItems": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberN{Value: "341"},
					},
				},
				"ProductReviews": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"FiveStar": &types.AttributeValueMemberL{
							Value: []types.AttributeValue{
								&types.AttributeValueMemberS{Value: "Excellent! Can't recommend it highly enough! Buy it!"},
								&types.AttributeValueMemberS{Value: "Do yourself a favor and buy this."},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.expr)
			require.NoError(t, err)

			result, err := Apply(expr, ApplyInput{ExpressionNames: tt.names}, tt.doc)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestAttributePathString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple",
			input: "title",
			want:  "title",
		},
		{
			name:  "nested",
			input: "a.b.c",
			want:  "a.b.c",
		},
		{
			name:  "indexed",
			input: "mylist[0]",
			want:  "mylist[0]",
		},
		{
			name:  "complex",
			input: "a.b[0].c[1].d",
			want:  "a.b[0].c[1].d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			require.NoError(t, err)
			require.Len(t, expr.Paths, 1)

			assert.Equal(t, tt.want, expr.Paths[0].String())
		})
	}
}
