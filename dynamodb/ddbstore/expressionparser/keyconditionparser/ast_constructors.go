package keyconditionparser

import (
	"bezos/dynamodb/ddbstore/expressionparser/astutil"
	"bezos/dynamodb/ddbstore/expressionparser/keyconditionast"
	"bezos/dynamodb/table"
	"fmt"
)

//todo add some more of these, instead of defining them inline in the peg file, so that we can test them.

func fromPKSK(part, sort any, table table.PrimaryKeyDefinition) (*keyconditionast.KeyCondition, error) {
	pk := astutil.CastTo[keyconditionast.PartitionKeyCondition](part)
	if err := verifyPK(pk.KeyName.GetName(), table); err != nil {
		return nil, err
	}
	if sort == nil {
		return keyconditionast.New(pk, nil), nil
	}
	sk := astutil.CastTo[*keyconditionast.SortKeyCondition](sort)
	return keyconditionast.New(pk, sk), verifySK(sk.KeyName(), table)
}

func verifyIdentifierAgainstTable(name string, table table.PrimaryKeyDefinition) error {
	if table.PartitionKey.Name != name && table.SortKey.Name != name {
		return fmt.Errorf("name %q is not a key in this table", name)
	}
	return nil
}

func verifyPK(name string, table table.PrimaryKeyDefinition) error {
	if got, want := table.PartitionKey.Name, name; got != want {
		return fmt.Errorf("name %q is not a partition key in this table, expected %q", got, want)
	}
	return nil
}

func verifySK(name string, table table.PrimaryKeyDefinition) error {
	if got, want := table.SortKey.Name, name; got != want {
		return fmt.Errorf("name %q is not a sort key in this table, expected %q", got, want)
	}
	return nil
}
