package main

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const schemaFilename = "schema_dynamodb.yaml"

// DiscoverSchemas finds all schema_dynamodb.yaml files in the current repository/directory.
// It tries multiple strategies in order of efficiency:
// 1. git ls-files (fastest, works in git repos)
// 2. find command (medium, works on Unix systems)
// 3. Pure Go walk (slowest, always works)
func DiscoverSchemas() ([]string, error) {
	// Strategy 1: Try git ls-files
	if files, err := discoverWithGitLsFiles(); err == nil && len(files) > 0 {
		return files, nil
	}

	// Strategy 2: Try find command
	if files, err := discoverWithFind(); err == nil && len(files) > 0 {
		return files, nil
	}

	// Strategy 3: Pure Go walk
	return discoverWithWalk()
}

// discoverWithGitLsFiles uses git ls-files to find schema files.
// This is very fast as it uses git's index.
func discoverWithGitLsFiles() ([]string, error) {
	// Check if we're in a git repo
	if _, err := exec.LookPath("git"); err != nil {
		return nil, err
	}

	// git ls-files --cached --others --exclude-standard finds tracked and untracked files
	cmd := exec.Command("git", "ls-files", "--cached", "--others", "--exclude-standard")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	return filterSchemaFiles(output)
}

// discoverWithFind uses the find command to locate schema files.
func discoverWithFind() ([]string, error) {
	if _, err := exec.LookPath("find"); err != nil {
		return nil, err
	}

	// Find all schema_dynamodb.yaml files
	cmd := exec.Command("find", ".", "-name", schemaFilename, "-type", "f")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	return parseAndAbsolutize(output)
}

// discoverWithWalk uses pure Go to walk the directory tree.
func discoverWithWalk() ([]string, error) {
	var files []string

	// Directories to skip for performance
	skipDirs := map[string]bool{
		".git":         true,
		"node_modules": true,
		"vendor":       true,
		".ddb":         true,
		"__pycache__":  true,
		".venv":        true,
	}

	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// Skip common large directories
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}

		// Check if it's the schema file
		if d.Name() == schemaFilename {
			abs, err := filepath.Abs(path)
			if err != nil {
				abs = path
			}
			files = append(files, abs)
		}

		return nil
	})

	return files, err
}

// filterSchemaFiles filters git ls-files output for schema_dynamodb.yaml files.
func filterSchemaFiles(output []byte) ([]string, error) {
	var files []string
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasSuffix(line, schemaFilename) {
			abs, err := filepath.Abs(line)
			if err != nil {
				abs = line
			}
			files = append(files, abs)
		}
	}
	return files, scanner.Err()
}

// parseAndAbsolutize parses command output and converts paths to absolute.
func parseAndAbsolutize(output []byte) ([]string, error) {
	var files []string
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			abs, err := filepath.Abs(line)
			if err != nil {
				abs = line
			}
			files = append(files, abs)
		}
	}
	return files, scanner.Err()
}
