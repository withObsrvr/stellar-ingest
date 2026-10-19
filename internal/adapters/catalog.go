package adapters

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/withObsrvr/stellar-ingest/internal/logging"
)

// GenerateCatalogReport scans the output directory and narrates a summary
// of all Parquet files with shard counts and DuckDB query examples.
//
// Narration, not a result: it is informational, it prints on every run, and
// §3 reserves stdout for the one thing a caller would parse. It used to go
// to stdout, where it sat in front of anything downstream.
func GenerateCatalogReport(outputDir string, log *logging.Logger) error {
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

	log.Section("Catalog Report")
	log.Field("Output directory", "%s", outputDir)
	log.Field("Tables found", "%d", len(names))
	log.Line()

	for _, name := range names {
		files := tables[name]
		log.Info("  %-35s %d shard file(s)", name, len(files))
	}

	// The query examples are a copy-paste aid rather than part of the
	// report, so they are detail: useful when asked for, noise on every
	// chunk of a thousand-chunk backfill.
	log.Line()
	log.Section("DuckDB Query Examples")
	log.Line()
	for _, name := range names {
		globPath := filepath.Join(bronzeDir, name, "**", "*.parquet")
		log.Detail("-- %s", name)
		log.Detail("SELECT count(*) FROM read_parquet('%s');", globPath)
	}

	log.Detail("-- All tables combined row count")
	log.Detail("SELECT count(*) FROM read_parquet('%s');", filepath.Join(bronzeDir, "**", "*.parquet"))

	return nil
}
