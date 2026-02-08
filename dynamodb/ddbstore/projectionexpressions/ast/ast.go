// Package ast contains the AST types for DynamoDB ProjectionExpression parsing.
//
// A ProjectionExpression is a comma-separated list of attribute paths that
// specifies which attributes to retrieve from a DynamoDB item.
//
// Examples:
//   - "title, price, authors"
//   - "ProductReviews.FiveStar[0].ReviewText"
//   - "#yr, title, info.rating"
package ast

import (
	"bezos/dynamodb/ddbstore/astutil"
	"fmt"
)

// ProjectionExpression represents the full parsed projection expression.
type ProjectionExpression struct {
	Paths []*AttributePath
}

// NewProjectionExpression creates a new ProjectionExpression from a list of paths.
func NewProjectionExpression(head, tail any) *ProjectionExpression {
	paths := astutil.HeadTailSlice[*AttributePath](head, tail)
	return &ProjectionExpression{Paths: paths}
}

// AttributeNames returns all the top-level attribute names in the projection.
func (p *ProjectionExpression) AttributeNames() []string {
	names := make([]string, 0, len(p.Paths))
	for _, path := range p.Paths {
		if len(path.Parts) > 0 && path.Parts[0].Identifier != nil {
			names = append(names, path.Parts[0].Identifier.ResolvedName())
		}
	}
	return names
}

// AttributePath represents a document path (e.g., "user.profile[0].age")
type AttributePath struct {
	Parts []*AttributePathPart
}

// NewAttributePath creates a new AttributePath from a head identifier and tail parts.
func NewAttributePath(head any, tail any) *AttributePath {
	parts := []*AttributePathPart{
		newAttributePathPart(head),
	}

	switch t := tail.(type) {
	case string:
		parts = append(parts, newAttributePathPart(tail))
	case int:
		parts = append(parts, newAttributePathPart(tail))
	case []any:
		for _, v := range t {
			parts = append(parts, newAttributePathPart(v))
		}
	}
	return &AttributePath{Parts: parts}
}

func newAttributePathPart(p any) *AttributePathPart {
	switch v := p.(type) {
	case string:
		if astutil.IsReservedName(v) {
			panic(fmt.Sprintf("attribute name %q is reserved, use ExpressionAttributeNames instead", v))
		}
		return &AttributePathPart{Identifier: &Identifier{Name: &v}}
	case int:
		return &AttributePathPart{Index: &v}
	case *ExpressionAttributeName:
		return &AttributePathPart{Identifier: &Identifier{NameExpression: v}}
	default:
		panic(fmt.Sprintf("unsupported path part type %T", p))
	}
}

// String returns a string representation of the path for debugging.
func (a *AttributePath) String() string {
	result := ""
	for i, part := range a.Parts {
		if part.Index != nil {
			result += fmt.Sprintf("[%d]", *part.Index)
		} else if part.Identifier != nil {
			if i > 0 {
				result += "."
			}
			result += part.Identifier.ResolvedName()
		}
	}
	return result
}

// AttributePathPart represents a single component of an attribute path.
// Either Identifier or Index is set, never both.
type AttributePathPart struct {
	Identifier *Identifier
	Index      *int
}

// Identifier represents an attribute name, either direct or via expression attribute name.
type Identifier struct {
	Name           *string                  // Direct attribute name
	NameExpression *ExpressionAttributeName // Expression attribute name (e.g., #yr)
}

// ResolvedName returns the resolved attribute name.
// Note: For ExpressionAttributeName, this returns the alias, not the resolved name.
// Use GetName to get the actual name with expression attribute name resolution.
func (i *Identifier) ResolvedName() string {
	if i.Name != nil {
		return *i.Name
	}
	if i.NameExpression != nil {
		return i.NameExpression.Alias
	}
	return ""
}

// GetName returns the resolved attribute name, looking up expression attribute names.
func (i *Identifier) GetName(names map[string]string) string {
	if i.Name != nil {
		return *i.Name
	}
	if i.NameExpression == nil {
		panic("both Name and NameExpression are nil")
	}
	resolved, found := names[i.NameExpression.Alias]
	if !found {
		panic(fmt.Sprintf("expression attribute name %s not found", i.NameExpression.Alias))
	}
	return resolved
}

// ExpressionAttributeName represents an alias for an attribute name (e.g., #yr).
type ExpressionAttributeName struct {
	Alias string
}

// NewExpressionAttributeName creates a new ExpressionAttributeName.
func NewExpressionAttributeName(alias string) *ExpressionAttributeName {
	return &ExpressionAttributeName{Alias: alias}
}
