package keyconditions

import (
	"bezos/dynamodb/ddbstore/expressions/keyconditions/ast"
	"bezos/dynamodb/table"
	"fmt"
)

// structs in this file require a second pass to convert to the AST.

// Need to disambiguate between PK and SK if they are both using
// the "=" operation.
type ambiguousKeyCondition struct {
	Left  *rawEqualCondition
	Right *rawEqualCondition
}

func (c *ambiguousKeyCondition) Disambiguate(params *keyConditionParserParams) (*ast.KeyCondition, error) {
	pk := params.TableKeys.PartitionKey.Name
	sk := params.TableKeys.SortKey.Name
	leftname := c.Left.Identifier.GetName()
	var rawPKCond *rawEqualCondition
	var rawSKCond *rawEqualCondition
	switch leftname { // find which key is the partition key, use other as sort key
	case pk:
		rawPKCond = c.Left
		rawSKCond = c.Right
	case sk:
		rawPKCond = c.Right
		rawSKCond = c.Left
	default:
		panic(fmt.Sprintf("name %q is not a key in this table", leftname))
	}
	pkCond, err := rawPKCond.toPKCond(params.TableKeys)
	if err != nil {
		return nil, fmt.Errorf("to pk cond: %w", err)
	}
	skCond, err := rawSKCond.toSKCond(params.TableKeys)
	if err != nil {
		return nil, fmt.Errorf("to sk cond: %w", err)
	}
	return ast.New(pkCond, skCond), nil
}

type rawEqualCondition struct {
	Identifier  ast.Identifier
	EqualsValue ast.Value
}

func (r *rawEqualCondition) toPKCond(table table.PrimaryKeyDefinition) (ast.PartitionKeyCondition, error) {
	if err := verifyPK(r.Identifier.GetName(), table); err != nil {
		return ast.PartitionKeyCondition{}, fmt.Errorf("verify pk: %w", err)
	}
	return ast.NewPartitionKeyCondition(r.Identifier, r.EqualsValue), nil
}

func (r *rawEqualCondition) toSKCond(table table.PrimaryKeyDefinition) (*ast.SortKeyCondition, error) {
	if err := verifySK(r.Identifier.GetName(), table); err != nil {
		return nil, fmt.Errorf("verify sk: %w", err)
	}
	return ast.NewComparisonCondition(r.Identifier, ast.Equal, r.EqualsValue), nil
}
