package jobs

import (
	"database/sql"
	"encoding/json"
	"github.com/withObsrvr/stellar-ingest/internal/domain"
	"github.com/withObsrvr/stellar-ingest/internal/fixtures"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Simulate two crash points using actual Parquet files: before the incomplete
// shard's footer is valid, and after finalization but before its checkpoint.
// A completed sibling shard must survive while the incomplete one is replaced.
func TestOrchestratorResumesIncompleteParquetShard(t *testing.T) {
	scrubEnv(t)
	meta, fixturePath := fixtures.LoadMeta(t, "pubnet-genesis-p1")
	work := t.TempDir()
	baselineRoot := filepath.Join(work, "baseline")
	baseline := runFixture(t, meta, fixturePath, baselineRoot)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want, err := digestFixtureOutput(db, baseline, meta.Name)
	if err != nil {
		t.Fatal(err)
	}

	for _, crashPoint := range []string{"partial-file", "finalized-before-checkpoint"} {
		t.Run(crashPoint, func(t *testing.T) {
			root := filepath.Join(work, crashPoint)
			if err := os.MkdirAll(root, 0o755); err != nil {
				t.Fatal(err)
			}
			cfg := domain.OrchestratorConfig{
				StartLedger: meta.StartLedger, EndLedger: meta.EndLedger,
				NumWorkers: 2, BatchSize: 2, OutputDir: root,
				NetworkPassphrase: meta.NetworkPassphrase,
				StorageType:       "XDR", Bucket: fixturePath,
			}
			cm, err := NewCheckpointManager(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			// Two workers on five ledgers split into [100,102] and [103,104].
			cm.CompleteShard(0, Shard{ID: 0, StartLedger: 100, EndLedger: 102})
			if err := cm.Save(); err != nil {
				t.Fatal(err)
			}
			attempt := filepath.Join(root, "attempts", cm.AttemptID())
			var completedFile, incompleteFile string
			err = filepath.WalkDir(baseline, func(path string, entry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".parquet") || strings.Contains(path, "_meta_lineage") {
					return nil
				}
				rel, err := filepath.Rel(filepath.Dir(baseline), path)
				if err != nil {
					return err
				}
				target := filepath.Join(attempt, rel)
				if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
					return err
				}
				bytes, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if err := os.WriteFile(target, bytes, 0o644); err != nil {
					return err
				}
				if strings.HasPrefix(entry.Name(), "shard_0000_") {
					completedFile = target
				}
				if strings.HasPrefix(entry.Name(), "shard_0001_") {
					incompleteFile = target
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if completedFile == "" || incompleteFile == "" {
				t.Fatalf("missing shard files: complete=%q incomplete=%q", completedFile, incompleteFile)
			}
			completeHash, err := domain.FileSHA256(completedFile)
			if err != nil {
				t.Fatal(err)
			}
			if crashPoint == "partial-file" {
				if err := os.WriteFile(incompleteFile, []byte("unfinished parquet footer"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(filepath.Join(attempt, "manifest.json")); !os.IsNotExist(err) {
				t.Fatalf("manifest exists before recovery: %v", err)
			}
			if err := NewOrchestrator(cfg).Run(); err != nil {
				t.Fatal(err)
			}
			if hash, err := domain.FileSHA256(completedFile); err != nil || hash != completeHash {
				t.Fatalf("completed shard changed: hash=%q error=%v", hash, err)
			}
			if _, err := os.Stat(incompleteFile); !os.IsNotExist(err) {
				t.Fatalf("stale incomplete shard file survived recovery: %v", err)
			}
			got, err := digestFixtureOutput(db, filepath.Join(attempt, "bronze"), meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			requireSameDigest(t, "recovered "+crashPoint, want, got)
			blob, err := os.ReadFile(filepath.Join(attempt, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var manifest domain.AttemptManifest
			if err := json.Unmarshal(blob, &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.AttemptID != cm.AttemptID() || manifest.Tables["ledgers"].Rows != 5 {
				t.Fatalf("invalid recovered manifest: attempt=%s ledger rows=%d", manifest.AttemptID, manifest.Tables["ledgers"].Rows)
			}
		})
	}
}
