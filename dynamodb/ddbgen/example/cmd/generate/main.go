// This file generates the type-safe key constructors.
// Run: go generate ./dynamodb/ddbgen/example
package main

import (
	"fmt"
	"os"

	"github.com/acksell/bezos/dynamodb/ddbgen"
	_ "github.com/acksell/bezos/dynamodb/ddbgen/example" // Side-effect import triggers registration
)

func main() {
	err := ddbgen.Generate(ddbgen.DefaultConfig())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
