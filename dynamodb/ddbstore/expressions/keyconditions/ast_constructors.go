package keyconditions

import (
	"bezos/dynamodb/ddbstore/expressions/astutil"
	"bezos/dynamodb/ddbstore/expressions/keyconditions/ast"
	"bezos/dynamodb/table"
	"fmt"
)

//todo add some more of these, instead of defining them inline in the peg file, so that we can test them.

func fromPKSK(part, sort any, table table.PrimaryKeyDefinition) (*ast.KeyCondition, error) {
	pk := astutil.CastTo[ast.PartitionKeyCondition](part)
	if err := verifyPK(pk.KeyName.GetName(), table); err != nil {
		return nil, err
	}
	if sort == nil {
		return ast.New(pk, nil), nil
	}
	sk := astutil.CastTo[*ast.SortKeyCondition](sort)
	return ast.New(pk, sk), verifySK(sk.KeyName(), table)
}

func verifyIdentifierAgainstTable(name string, table table.PrimaryKeyDefinition) error {
	if astutil.IsReservedName(name) {
		return fmt.Errorf("name %q is a reserved word", name)
	}
	if table.PartitionKey.Name != name && table.SortKey.Name != name {
		return fmt.Errorf("name %q is not a key in this table", name)
	}
	return nil
}

func verifyPK(name string, table table.PrimaryKeyDefinition) error {
	if astutil.IsReservedName(name) {
		return fmt.Errorf("name %q is a reserved word", name)
	}
	if got, want := table.PartitionKey.Name, name; got != want {
		return fmt.Errorf("name %q is not a partition key in this table, expected %q", got, want)
	}
	return nil
}

func verifySK(name string, table table.PrimaryKeyDefinition) error {
	if astutil.IsReservedName(name) {
		return fmt.Errorf("name %q is a reserved word", name)
	}
	if got, want := table.SortKey.Name, name; got != want {
		return fmt.Errorf("name %q is not a sort key in this table, expected %q", got, want)
	}
	return nil
}
