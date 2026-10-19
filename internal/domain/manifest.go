package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

const manifestSchemaVersion = 1

type AttemptManifest struct {
	SchemaVersion     int                      `json:"schema_version"`
	AttemptID         string                   `json:"attempt_id"`
	Status            string                   `json:"status"`
	PipelineVersion   string                   `json:"pipeline_version"`
	NetworkPassphrase string                   `json:"network_passphrase"`
	SourceType        string                   `json:"source_type"`
	SourceLocation    string                   `json:"source_location"`
	RangeStart        uint32                   `json:"range_start"`
	RangeEnd          uint32                   `json:"range_end"`
	CreatedAt         time.Time                `json:"created_at"`
	Tables            map[string]ManifestTable `json:"tables"`
}

type ManifestTable struct {
	Status   string         `json:"status"`
	Rows     int64          `json:"rows"`
	Files    []ManifestFile `json:"files"`
	Bytes    int64          `json:"bytes"`
	Checksum string         `json:"checksum_sha256"`
}

type ManifestFile struct {
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	Checksum string `json:"checksum_sha256"`
}

func WriteAttemptManifest(path, attemptID string, config OrchestratorConfig, lineage []LineageRecord) error {
	root := filepath.Dir(path)
	rows := make(map[string]int64)
	for _, record := range lineage {
		rows[record.Dataset] += record.RecordsProcessed
	}

	manifest := AttemptManifest{
		SchemaVersion: manifestSchemaVersion, AttemptID: attemptID, Status: "validated",
		PipelineVersion: Version, NetworkPassphrase: config.NetworkPassphrase,
		SourceType: config.StorageType, SourceLocation: config.Bucket,
		RangeStart: config.StartLedger, RangeEnd: config.EndLedger,
		CreatedAt: time.Now().UTC(), Tables: make(map[string]ManifestTable),
	}

	for _, table := range selectedOutputTables(config.OnlyTables) {
		files, parquetRows, totalBytes, aggregateChecksum, err := manifestFiles(root, table)
		if err != nil {
			return err
		}
		rowCount := parquetRows
		if lineageRows := rows[table]; lineageRows != 0 && lineageRows > rowCount {
			return fmt.Errorf("table %s lineage reports %d rows but parquet contains %d", table, lineageRows, rowCount)
		}
		status := "success_nonzero"
		if rowCount == 0 {
			status = "success_zero"
			if len(files) != 0 {
				return fmt.Errorf("table %s reports zero rows but has %d parquet files", table, len(files))
			}
		} else if len(files) == 0 {
			return fmt.Errorf("table %s reports %d rows but has no parquet files", table, rowCount)
		}
		manifest.Tables[table] = ManifestTable{Status: status, Rows: rowCount, Files: files, Bytes: totalBytes, Checksum: aggregateChecksum}
	}
	if ledgers, ok := manifest.Tables["ledgers"]; ok {
		expected := int64(config.EndLedger-config.StartLedger) + 1
		if ledgers.Rows != expected {
			return fmt.Errorf("ledger coverage validation failed: got %d ledger rows, want %d", ledgers.Rows, expected)
		}
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("publish manifest: %w", err)
	}
	return nil
}

func selectedOutputTables(selection map[string]bool) []string {
	set := make(map[string]bool)
	for extractor, output := range ExtractorOutputDirByName {
		if TableSetIncludes(selection, extractor, output, MapToDuckLakeTable(output)) {
			set[output] = true
		}
	}
	tables := make([]string, 0, len(set))
	for table := range set {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func manifestFiles(root, table string) ([]ManifestFile, int64, int64, string, error) {
	base := filepath.Join(root, "bronze", table)
	var files []ManifestFile
	var total int64
	var totalRows int64
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".parquet" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		checksum, err := FileSHA256(path)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		parquetFile, err := parquet.OpenFile(f, info.Size())
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("open parquet %s: %w", path, err)
		}
		if closeErr != nil {
			return closeErr
		}
		totalRows += parquetFile.NumRows()
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, ManifestFile{Path: filepath.ToSlash(rel), Bytes: info.Size(), Checksum: checksum})
		total += info.Size()
		return nil
	})
	if err != nil {
		return nil, 0, 0, "", fmt.Errorf("scan table %s: %w", table, err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	h := sha256.New()
	for _, file := range files {
		_, _ = io.WriteString(h, file.Path+"\x00"+file.Checksum+"\n")
	}
	return files, totalRows, total, hex.EncodeToString(h.Sum(nil)), nil
}

func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ReadAttemptManifest(path string) (*AttemptManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest AttemptManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if manifest.SchemaVersion != manifestSchemaVersion || manifest.Status != "validated" || strings.TrimSpace(manifest.AttemptID) == "" {
		return nil, fmt.Errorf("manifest is not a supported validated attempt")
	}
	return &manifest, nil
}
