// ddbgen is a code generator for type-safe DynamoDB key constructors.
//
// Usage: Create a main.go file that imports your indexes package and calls Generate:
//
//	package main
//
//	import (
//	    _ "myapp/db"  // Side-effect import triggers registration
//	    "github.com/acksell/bezos/dynamodb/ddbgen"
//	)
//
//	func main() {
//	    ddbgen.MustGenerate(ddbgen.Config{
//	        Package: "db",
//	        Output:  "keys_gen.go",
//	    })
//	}
//
// In your indexes package:
//
//	var UserIndex = index.PrimaryIndex{Table: UserTable, ...}
//
//	func init() {
//	    ddbgen.RegisterIndex("User", User{}, &UserIndex)
//	}
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/acksell/bezos/dynamodb/ddbgen/codegen"
	"github.com/acksell/bezos/dynamodb/index"
	"github.com/acksell/bezos/dynamodb/index/keys"
	"github.com/acksell/bezos/dynamodb/table"
)

func main() {
	var (
		outputFile = flag.String("output", "generated_keys.go", "output file path")
		pkgName    = flag.String("pkg", "", "package name (defaults to current directory name)")
	)
	flag.Parse()

	pkg := *pkgName
	if pkg == "" {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error getting working directory: %v\n", err)
			os.Exit(1)
		}
		pkg = filepath.Base(wd)
	}

	// Example indexes using the real index package types
	userTable := table.TableDefinition{
		Name: "users",
		KeyDefinitions: table.PrimaryKeyDefinition{
			PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
			SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
		},
	}

	userIndex := index.PrimaryIndex{
		Table:        userTable,
		PartitionKey: keys.Fmt("USER#%s", keys.Field("userID")),
		SortKey:      keys.Const("PROFILE"),
		Secondary: []index.SecondaryIndex{
			{
				Name: "ByEmail",
				PartitionKey: keys.Key{
					Def:       table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
					Extractor: keys.Fmt("EMAIL#%s", keys.Field("email")),
				},
				SortKey: &keys.Key{
					Def:       table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
					Extractor: keys.Fmt("USER#%s", keys.Field("userID")),
				},
			},
		},
	}

	orderTable := table.TableDefinition{
		Name: "orders",
		KeyDefinitions: table.PrimaryKeyDefinition{
			PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
			SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
		},
	}

	orderIndex := index.PrimaryIndex{
		Table:        orderTable,
		PartitionKey: keys.Fmt("TENANT#%s", keys.Field("tenantID")),
		SortKey:      keys.Fmt("ORDER#%s", keys.Field("orderID")),
	}

	cfg := codegen.Config{
		Package: pkg,
		Indexes: []codegen.IndexBinding{
			{Name: "User", Index: userIndex, VarName: "UserIndex"},
			{Name: "Order", Index: orderIndex, VarName: "OrderIndex"},
		},
	}

	gen := codegen.New(cfg)
	output, err := gen.Generate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error generating code: %v\n", err)
		if len(output) > 0 {
			fmt.Fprintf(os.Stderr, "unformatted output:\n%s\n", output)
		}
		os.Exit(1)
	}

	if err := os.WriteFile(*outputFile, output, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated %s\n", *outputFile)
}
