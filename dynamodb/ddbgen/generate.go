package ddbgen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
)

const (
	// bezosModule is the module path of the bezos library.
	bezosModule = "github.com/acksell/bezos"

	valPkg = bezosModule + "/dynamodb/index/val"
)

// GenerateOptions configures code generation behavior.
type GenerateOptions struct {
	// Dir is the directory containing the user's package
	Dir string
	// Output is the output file name for generated Go code
	Output string
	// NoSchema disables schema/ subdirectory generation
	NoSchema bool
}

// RunGenerate discovers PrimaryIndex definitions and generates code via
// a compile-and-execute loader sidecar program.
func RunGenerate(opts GenerateOptions) error {
	if opts.Dir == "" {
		opts.Dir = "."
	}
	if opts.Output == "" {
		opts.Output = "index_gen.go"
	}

	// Phase 1: Minimal type-checking to discover index variables and entity metadata
	result, err := Discover(opts.Dir)
	if err != nil {
		return fmt.Errorf("discovering indexes: %w", err)
	}

	if len(result.Indexes) == 0 {
		fmt.Fprintf(os.Stderr, "ddb gen: no index.PrimaryIndex definitions found in %s\n", opts.Dir)
		return nil
	}

	// Phase 2: Run loader sidecar to extract runtime data (JSON on stdout)
	indexes, err := runLoader(result, opts)
	if err != nil {
		return err
	}

	// Phase 3: Generate code from the extracted data
	outputPath := opts.Output
	if opts.Dir != "." {
		outputPath = filepath.Join(opts.Dir, opts.Output)
	}
	absOutput, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("resolving output path: %w", err)
	}

	code, err := generateCode(result.PackageName, indexes)
	if err != nil {
		return fmt.Errorf("generating code: %w", err)
	}

	if err := os.WriteFile(absOutput, code, 0644); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}
	fmt.Printf("ddb gen: generated %s (%d indexes)\n", absOutput, len(indexes))

	// Phase 4: Generate schema files
	if !opts.NoSchema {
		absDir, err := filepath.Abs(opts.Dir)
		if err != nil {
			return fmt.Errorf("resolving directory: %w", err)
		}
		schemaDir := filepath.Join(absDir, "schema")
		if err := os.MkdirAll(schemaDir, 0755); err != nil {
			return fmt.Errorf("creating schema directory: %w", err)
		}
		if err := generateSchemaFiles(schemaDir, indexes); err != nil {
			return fmt.Errorf("generating schema: %w", err)
		}
	}

	return nil
}

// runLoader generates and runs the loader sidecar, returning the extracted index data.
func runLoader(result *DiscoverResult, opts GenerateOptions) ([]indexInfo, error) {
	absDir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("resolving directory: %w", err)
	}

	// Find module root — sidecar runs inside user's module
	modRoot, _, err := findModuleRoot(absDir)
	if err != nil {
		return nil, fmt.Errorf("finding module root: %w", err)
	}

	// Generate accessor file in the user's package so the loader can access unexported vars
	accessorPath := filepath.Join(absDir, "ddbgen_accessors.go")
	if err := generateAccessors(result, accessorPath); err != nil {
		return nil, fmt.Errorf("generating accessors: %w", err)
	}
	defer os.Remove(accessorPath)

	// Generate the loader main.go source
	loaderSrc, err := generateLoaderSource(result)
	if err != nil {
		return nil, fmt.Errorf("generating loader source: %w", err)
	}

	// Write loader into a temp subdirectory of the module root so it inherits
	// the user's go.mod — no separate go.mod, no go mod tidy, no downloads.
	tmpDir, err := os.MkdirTemp(modRoot, ".ddbgen-loader-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	mainPath := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(mainPath, loaderSrc, 0644); err != nil {
		return nil, fmt.Errorf("writing loader main.go: %w", err)
	}

	// Build and run the loader from within the module root
	var stdout bytes.Buffer
	cmd := exec.Command("go", "run", mainPath)
	cmd.Dir = modRoot
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("loader execution failed: %w", err)
	}

	// Parse JSON output
	var indexes []indexInfo
	if err := json.Unmarshal(stdout.Bytes(), &indexes); err != nil {
		return nil, fmt.Errorf("parsing loader output: %w\nraw output: %s", err, stdout.String())
	}

	return indexes, nil
}

// findModuleRoot walks up from dir to find go.mod and returns (root dir, module path).
func findModuleRoot(dir string) (string, string, error) {
	current := dir
	for {
		goModPath := filepath.Join(current, "go.mod")
		data, err := os.ReadFile(goModPath)
		if err == nil {
			modPath := extractModulePath(string(data))
			if modPath == "" {
				return "", "", fmt.Errorf("could not parse module path from %s", goModPath)
			}
			return current, modPath, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", "", fmt.Errorf("no go.mod found in %s or any parent directory", dir)
		}
		current = parent
	}
}

func extractModulePath(goModContent string) string {
	for _, line := range strings.Split(goModContent, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module "))
		}
	}
	return ""
}

// generateAccessors writes a small Go file into the user's package that exports
// accessor functions for unexported PrimaryIndex variables.
func generateAccessors(result *DiscoverResult, outputPath string) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "// Code generated by ddbgen. DO NOT EDIT.\n\n")
	fmt.Fprintf(&buf, "package %s\n\n", result.PackageName)
	fmt.Fprintf(&buf, "import \"github.com/acksell/bezos/dynamodb/index\"\n\n")

	for _, idx := range result.Indexes {
		funcName := accessorFuncName(idx.VarName)
		fmt.Fprintf(&buf, "func %s() index.PrimaryIndex[%s] { return %s }\n",
			funcName, idx.EntityType, idx.VarName)
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("formatting accessor file: %w\n%s", err, buf.String())
	}

	return os.WriteFile(outputPath, formatted, 0644)
}

// todo deprecate..?
// accessorFuncName returns the exported accessor function name for a variable.
func accessorFuncName(varName string) string {
	return "DdbgenGet_" + varName
}

// generateLoaderSource produces the Go source for the minimal loader sidecar.
// The loader imports the user's package, extracts runtime PrimaryIndex data,
// serializes it to JSON, and writes it to stdout.
func generateLoaderSource(result *DiscoverResult) ([]byte, error) {
	tmpl, err := template.New("loader").Delims("[[", "]]").Funcs(template.FuncMap{
		"accessorFunc": accessorFuncName,
	}).Parse(loaderTemplate)
	if err != nil {
		return nil, fmt.Errorf("parsing loader template: %w", err)
	}

	data := struct {
		UserPackagePath string
		ValPkg          string
		Indexes         []IndexVar
		EntityFields    map[string][]FieldInfo
	}{
		UserPackagePath: result.PackagePath,
		ValPkg:          valPkg,
		Indexes:         result.Indexes,
		EntityFields:    result.EntityFields,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("executing loader template: %w", err)
	}

	return buf.Bytes(), nil
}

// loaderTemplate is the Go source template for the minimal loader sidecar.
// It imports the user's package to access PrimaryIndex variables at runtime,
// extracts runtime data (table names, key patterns, GSIs), serializes to JSON,
// and writes to stdout. All code generation happens back in the ddbgen package.
var loaderTemplate = `// Code generated by ddbgen. DO NOT EDIT.
// Temporary loader program for extracting runtime index data.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	userpkg "[[.UserPackagePath]]"
	"[[.ValPkg]]"
)

type fieldInfo struct {
	Name string ` + "`json:\"name\"`" + `
	Tag  string ` + "`json:\"tag\"`" + `
	Type string ` + "`json:\"type\"`" + `
}

type gsiInfo struct {
	Name      string       ` + "`json:\"name\"`" + `
	Index     int          ` + "`json:\"index\"`" + `
	PKDef     string       ` + "`json:\"pkDef\"`" + `
	PKPattern val.ValDef   ` + "`json:\"pkPattern\"`" + `
	SKDef     string       ` + "`json:\"skDef\"`" + `
	SKPattern *val.ValDef  ` + "`json:\"skPattern,omitempty\"`" + `
}

type indexInfo struct {
	VarName      string       ` + "`json:\"varName\"`" + `
	EntityType   string       ` + "`json:\"entityType\"`" + `
	TableName    string       ` + "`json:\"tableName\"`" + `
	PKDefName    string       ` + "`json:\"pkDefName\"`" + `
	SKDefName    string       ` + "`json:\"skDefName\"`" + `
	PartitionKey val.ValDef   ` + "`json:\"partitionKey\"`" + `
	SortKey      *val.ValDef  ` + "`json:\"sortKey,omitempty\"`" + `
	GSIs         []gsiInfo    ` + "`json:\"gsis,omitempty\"`" + `
	IsVersioned  bool         ` + "`json:\"isVersioned\"`" + `
	Fields       []fieldInfo  ` + "`json:\"fields\"`" + `
}

func main() {
	var indexes []indexInfo
[[range .Indexes]]
	{
		idx := userpkg.[[accessorFunc .VarName]]()
		info := indexInfo{
			VarName:      "[[.VarName]]",
			EntityType:   "[[.EntityType]]",
			IsVersioned:  [[.IsVersioned]],
			Fields: []fieldInfo{
			[[- range (index $.EntityFields .EntityType)]]
				{Name: "[[.Name]]", Tag: "[[.Tag]]", Type: "[[.Type]]"},
			[[- end]]
			},
			TableName:    idx.Table.Name,
			PKDefName:    idx.Table.KeyDefinitions.PartitionKey.Name,
			SKDefName:    idx.Table.KeyDefinitions.SortKey.Name,
			PartitionKey: idx.PartitionKey,
			SortKey:      idx.SortKey,
		}
		for i, sec := range idx.Secondary {
			gsi := gsiInfo{
				Name:      sec.GSI.Name,
				Index:     i,
				PKDef:     sec.GSI.KeyDefinitions.PartitionKey.Name,
				PKPattern: sec.Partition,
				SKDef:     sec.GSI.KeyDefinitions.SortKey.Name,
				SKPattern: sec.Sort,
			}
			info.GSIs = append(info.GSIs, gsi)
		}
		indexes = append(indexes, info)
	}
[[end]]
	data, err := json.Marshal(indexes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ddb gen: marshaling index data: %v\\n", err)
		os.Exit(1)
	}
	os.Stdout.Write(data)
}
`
