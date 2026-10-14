package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// GenerateCatalogReport scans the output directory and prints a summary
// of all Parquet files with row counts and DuckDB query examples.
func GenerateCatalogReport(outputDir string) error {
	bronzeDir := filepath.Join(outputDir, "bronze")
	if _, err := os.Stat(bronzeDir); os.IsNotExist(err) {
		return fmt.Errorf("no bronze directory found in %s", outputDir)
	}

	tables := make(map[string][]string) // table name -> list of parquet files
	err := filepath.Walk(bronzeDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".parquet") {
			// Extract table name from path: bronze/<table>/range_xxx/shard_xxx.parquet
			rel, _ := filepath.Rel(bronzeDir, path)
			parts := strings.Split(rel, string(os.PathSeparator))
			if len(parts) >= 1 {
				tableName := parts[0]
				tables[tableName] = append(tables[tableName], path)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Sort table names
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Println("=== Catalog Report ===")
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Printf("Tables found: %d\n\n", len(names))

	for _, name := range names {
		files := tables[name]
		fmt.Printf("  %-35s %d shard file(s)\n", name, len(files))
	}

	fmt.Println()
	fmt.Println("=== DuckDB Query Examples ===")
	fmt.Println()
	for _, name := range names {
		globPath := filepath.Join(bronzeDir, name, "**", "*.parquet")
		fmt.Printf("-- %s\n", name)
		fmt.Printf("SELECT count(*) FROM read_parquet('%s');\n\n", globPath)
	}

	fmt.Println("-- All tables combined row count")
	fmt.Printf("SELECT count(*) FROM read_parquet('%s');\n", filepath.Join(bronzeDir, "**", "*.parquet"))

	return nil
}
