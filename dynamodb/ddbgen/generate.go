package ddbgen

import (
	"fmt"
	"os"
	"strings"

	"github.com/acksell/bezos/dynamodb/ddbgen/codegen"
)

// Config holds configuration for code generation.
type Config struct {
	// Package is the Go package name for generated code.
	Package string
	// Output is the file path to write generated code to.
	Output string
}

// DefaultConfig returns a Config with defaults from the go:generate environment.
// Package defaults to $GOPACKAGE, Output defaults to "keys_gen.go".
func DefaultConfig() Config {
	pkg := os.Getenv("GOPACKAGE") // Set by go:generate
	return Config{
		Package: pkg,
		Output:  "index_gen.go",
	}
}

// Generate produces type-safe code for all registered indexes.
// It writes the generated code to the file specified in Config.Output.
//
// Example:
//
//	//go:generate go run ./cmd/generate
//
//	ddbgen.Generate(ddbgen.DefaultConfig())
func Generate(cfg Config) error {
	bindings := Registered()
	if len(bindings) == 0 {
		return fmt.Errorf("no indexes registered; make sure to import the package containing BindIndex() calls")
	}

	if cfg.Package == "" {
		return fmt.Errorf("Package is required")
	}
	if cfg.Output == "" {
		return fmt.Errorf("Output is required")
	}

	// Convert []IndexBinding to []codegen.IndexBinding
	codegenBindings := make([]codegen.IndexBinding, len(bindings))
	for i, b := range bindings {
		// Convention: user's private variable is lowercase name + "Index"
		// e.g., User entity -> userIndex
		varName := strings.ToLower(b.Name[:1]) + b.Name[1:] + "Index"
		codegenBindings[i] = codegen.IndexBinding{
			Name:       b.Name,
			EntityType: b.EntityType,
			Index:      *b.Index,
			VarName:    varName,
		}
	}

	genCfg := codegen.Config{
		Package: cfg.Package,
		Indexes: codegenBindings,
	}

	gen := codegen.New(genCfg)
	generatedCode, err := gen.Generate()
	if err != nil {
		return fmt.Errorf("generating code: %w", err)
	}

	if err := os.WriteFile(cfg.Output, generatedCode, 0644); err != nil {
		return fmt.Errorf("writing output file: %w", err)
	}

	fmt.Printf("Generated %s\n", cfg.Output)
	return nil
}

// MustGenerate is like Generate but panics on error.
func MustGenerate(cfg Config) {
	if err := Generate(cfg); err != nil {
		panic(err)
	}
}
