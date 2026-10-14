package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

type ContractBalanceEnrichmentResult struct {
	CandidateRows int64
	DecodedRows   int64
	UpdatedRows   int64
	SkippedRows   int64
}

type contractBalanceStageRow struct {
	contractID    string
	ledger        int64
	ledgerKeyHash string
	holder        string
	balance       string
}

type ContractBalanceEnricherConfig struct {
	DuckLake       DuckLakeConfig
	StartLedger    int64
	EndLedger      int64
	ChunkSize      int64
	RunID          string
	Network        string
	VersionLabel   string
	ManifestSchema string
	DryRun         bool
	Resume         bool
}

type ContractBalanceEnricher struct {
	config   ContractBalanceEnricherConfig
	pusher   *DuckLakePusher
	target   string
	manifest string
}

func NewContractBalanceEnricher(config ContractBalanceEnricherConfig) (*ContractBalanceEnricher, error) {
	if config.StartLedger <= 0 || config.EndLedger < config.StartLedger {
		return nil, fmt.Errorf("invalid ledger range %d-%d", config.StartLedger, config.EndLedger)
	}
	if config.ChunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}
	if !config.DryRun && strings.TrimSpace(config.RunID) == "" {
		return nil, fmt.Errorf("run ID is required for a mutating enrichment")
	}
	if config.ManifestSchema == "" {
		config.ManifestSchema = "bronze_operations"
	}

	pusher, err := NewDuckLakePusher(config.DuckLake)
	if err != nil {
		return nil, err
	}
	target := strings.Join([]string{pusher.config.CatalogName, pusher.config.SchemaName, "contract_data_snapshot_v1"}, ".")
	manifest := strings.Join([]string{pusher.config.CatalogName, config.ManifestSchema, "contract_balance_enrichment_manifest"}, ".")
	if _, err := quoteQualifiedIdentifier(target); err != nil {
		pusher.Close()
		return nil, err
	}
	if _, err := quoteQualifiedIdentifier(manifest); err != nil {
		pusher.Close()
		return nil, err
	}
	return &ContractBalanceEnricher{config: config, pusher: pusher, target: target, manifest: manifest}, nil
}

func (e *ContractBalanceEnricher) Close() error {
	return e.pusher.Close()
}

func (e *ContractBalanceEnricher) Run(ctx context.Context) (ContractBalanceEnrichmentResult, error) {
	var total ContractBalanceEnrichmentResult
	if err := e.pusher.Setup(ctx); err != nil {
		return total, err
	}
	if !e.config.DryRun {
		if err := e.ensureManifest(ctx); err != nil {
			return total, err
		}
	}

	for chunkStart := e.config.StartLedger; chunkStart <= e.config.EndLedger; {
		chunkEnd := chunkStart + e.config.ChunkSize - 1
		if chunkEnd < chunkStart || chunkEnd > e.config.EndLedger {
			chunkEnd = e.config.EndLedger
		}

		if !e.config.DryRun && e.config.Resume {
			completed, err := e.chunkCompleted(ctx, chunkStart, chunkEnd)
			if err != nil {
				return total, err
			}
			if completed {
				log.Printf("[ContractBalanceEnrichment] chunk=%d-%d status=skipped reason=manifest_completed", chunkStart, chunkEnd)
				if chunkEnd == e.config.EndLedger {
					break
				}
				chunkStart = chunkEnd + 1
				continue
			}
		}

		startedAt := time.Now().UTC()
		if !e.config.DryRun {
			if err := e.recordManifest(ctx, chunkStart, chunkEnd, "running", ContractBalanceEnrichmentResult{}, startedAt, nil, ""); err != nil {
				return total, err
			}
		}

		result, err := enrichContractBalanceChunk(ctx, e.pusher.db, e.target, chunkStart, chunkEnd, e.config.DryRun)
		total.CandidateRows += result.CandidateRows
		total.DecodedRows += result.DecodedRows
		total.UpdatedRows += result.UpdatedRows
		total.SkippedRows += result.SkippedRows
		if err != nil {
			if !e.config.DryRun {
				completedAt := time.Now().UTC()
				if manifestErr := e.recordManifest(ctx, chunkStart, chunkEnd, "failed", result, startedAt, &completedAt, err.Error()); manifestErr != nil {
					return total, fmt.Errorf("enrich chunk %d-%d: %v (record failure manifest: %w)", chunkStart, chunkEnd, err, manifestErr)
				}
			}
			return total, fmt.Errorf("enrich chunk %d-%d: %w", chunkStart, chunkEnd, err)
		}

		if !e.config.DryRun {
			completedAt := time.Now().UTC()
			if err := e.recordManifest(ctx, chunkStart, chunkEnd, "completed", result, startedAt, &completedAt, ""); err != nil {
				return total, err
			}
		}
		log.Printf(
			"[ContractBalanceEnrichment] chunk=%d-%d dry_run=%t candidates=%d decoded=%d updated=%d skipped=%d",
			chunkStart, chunkEnd, e.config.DryRun, result.CandidateRows, result.DecodedRows, result.UpdatedRows, result.SkippedRows,
		)

		if chunkEnd == e.config.EndLedger {
			break
		}
		chunkStart = chunkEnd + 1
	}
	return total, nil
}

func (e *ContractBalanceEnricher) ensureManifest(ctx context.Context) error {
	catalog, err := quoteQualifiedIdentifier(e.pusher.config.CatalogName)
	if err != nil {
		return err
	}
	schema, err := quoteQualifiedIdentifier(e.config.ManifestSchema)
	if err != nil {
		return err
	}
	manifest, err := quoteQualifiedIdentifier(e.manifest)
	if err != nil {
		return err
	}
	if _, err := e.pusher.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", catalog, schema)); err != nil {
		return fmt.Errorf("create enrichment manifest schema: %w", err)
	}
	if _, err := e.pusher.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			run_id VARCHAR,
			network VARCHAR,
			chunk_start BIGINT,
			chunk_end BIGINT,
			status VARCHAR,
			dry_run BOOLEAN,
			candidate_rows BIGINT,
			decoded_rows BIGINT,
			updated_rows BIGINT,
			skipped_rows BIGINT,
			started_at TIMESTAMP,
			completed_at TIMESTAMP,
			error_message VARCHAR,
			version_label VARCHAR
		)
	`, manifest)); err != nil {
		return fmt.Errorf("create enrichment manifest table: %w", err)
	}
	return nil
}

func (e *ContractBalanceEnricher) chunkCompleted(ctx context.Context, startLedger, endLedger int64) (bool, error) {
	manifest, err := quoteQualifiedIdentifier(e.manifest)
	if err != nil {
		return false, err
	}
	var count int64
	if err := e.pusher.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COUNT(*) FROM %s
		WHERE run_id = ? AND network = ? AND chunk_start = ? AND chunk_end = ? AND status = 'completed'
	`, manifest), e.config.RunID, e.config.Network, startLedger, endLedger).Scan(&count); err != nil {
		return false, fmt.Errorf("read enrichment manifest for %d-%d: %w", startLedger, endLedger, err)
	}
	return count > 0, nil
}

func (e *ContractBalanceEnricher) recordManifest(
	ctx context.Context,
	startLedger int64,
	endLedger int64,
	status string,
	result ContractBalanceEnrichmentResult,
	startedAt time.Time,
	completedAt *time.Time,
	errorMessage string,
) error {
	manifest, err := quoteQualifiedIdentifier(e.manifest)
	if err != nil {
		return err
	}
	tx, err := e.pusher.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin enrichment manifest update: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE run_id = ? AND network = ? AND chunk_start = ? AND chunk_end = ?
	`, manifest), e.config.RunID, e.config.Network, startLedger, endLedger); err != nil {
		return fmt.Errorf("replace enrichment manifest row: %w", err)
	}
	var completed any
	if completedAt != nil {
		completed = *completedAt
	}
	var failure any
	if errorMessage != "" {
		failure = errorMessage
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			run_id, network, chunk_start, chunk_end, status, dry_run,
			candidate_rows, decoded_rows, updated_rows, skipped_rows,
			started_at, completed_at, error_message, version_label
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, manifest),
		e.config.RunID, e.config.Network, startLedger, endLedger, status, e.config.DryRun,
		result.CandidateRows, result.DecodedRows, result.UpdatedRows, result.SkippedRows,
		startedAt, completed, failure, e.config.VersionLabel,
	); err != nil {
		return fmt.Errorf("insert enrichment manifest row: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit enrichment manifest update: %w", err)
	}
	committed = true
	return nil
}

var sqlIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func quoteQualifiedIdentifier(name string) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) == 0 {
		return "", fmt.Errorf("empty SQL identifier")
	}
	quoted := make([]string, len(parts))
	for i, part := range parts {
		if !sqlIdentifierPattern.MatchString(part) {
			return "", fmt.Errorf("unsafe SQL identifier %q", name)
		}
		quoted[i] = `"` + part + `"`
	}
	return strings.Join(quoted, "."), nil
}

func enrichContractBalanceChunk(
	ctx context.Context,
	db *sql.DB,
	targetTable string,
	startLedger int64,
	endLedger int64,
	dryRun bool,
) (ContractBalanceEnrichmentResult, error) {
	var result ContractBalanceEnrichmentResult
	if startLedger <= 0 || endLedger < startLedger {
		return result, fmt.Errorf("invalid ledger range %d-%d", startLedger, endLedger)
	}
	target, err := quoteQualifiedIdentifier(targetTable)
	if err != nil {
		return result, err
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT contract_id, ledger_sequence, ledger_key_hash, contract_data_xdr
		FROM %s
		WHERE ledger_sequence BETWEEN ? AND ?
		  AND contract_key_type = 'ScValTypeScvVec'
		  AND contract_data_xdr IS NOT NULL
		  AND contract_data_xdr <> ''
		  AND (balance_holder IS NULL OR balance IS NULL)
		ORDER BY ledger_sequence, ledger_key_hash
	`, target), startLedger, endLedger)
	if err != nil {
		return result, fmt.Errorf("scan contract balance candidates: %w", err)
	}

	stagedByKey := make(map[string]contractBalanceStageRow)
	for rows.Next() {
		var contractID, ledgerKeyHash, contractDataXDR string
		var ledger int64
		if err := rows.Scan(&contractID, &ledger, &ledgerKeyHash, &contractDataXDR); err != nil {
			rows.Close()
			return result, fmt.Errorf("scan contract balance candidate: %w", err)
		}
		result.CandidateRows++

		holder, balance, ok, err := decodeContractBalanceDataXDR(contractDataXDR)
		if err != nil {
			rows.Close()
			return result, fmt.Errorf("decode contract balance at ledger %d key %s: %w", ledger, ledgerKeyHash, err)
		}
		if !ok {
			result.SkippedRows++
			continue
		}
		result.DecodedRows++
		key := fmt.Sprintf("%s\x00%d\x00%s", contractID, ledger, ledgerKeyHash)
		stage := contractBalanceStageRow{
			contractID:    contractID,
			ledger:        ledger,
			ledgerKeyHash: ledgerKeyHash,
			holder:        holder,
			balance:       balance,
		}
		if previous, exists := stagedByKey[key]; exists && previous != stage {
			rows.Close()
			return result, fmt.Errorf("conflicting balance values for ledger %d key %s", ledger, ledgerKeyHash)
		}
		stagedByKey[key] = stage
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, fmt.Errorf("iterate contract balance candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return result, fmt.Errorf("close contract balance candidates: %w", err)
	}

	if dryRun || len(stagedByKey) == 0 {
		return result, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin contract balance enrichment: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, `
		CREATE OR REPLACE TEMP TABLE contract_balance_enrichment_stage (
			contract_id VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_key_hash VARCHAR NOT NULL,
			balance_holder VARCHAR NOT NULL,
			balance VARCHAR NOT NULL
		)
	`); err != nil {
		return result, fmt.Errorf("create contract balance stage: %w", err)
	}
	insert, err := tx.PrepareContext(ctx, `
		INSERT INTO contract_balance_enrichment_stage
			(contract_id, ledger_sequence, ledger_key_hash, balance_holder, balance)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return result, fmt.Errorf("prepare contract balance stage insert: %w", err)
	}
	for _, stage := range stagedByKey {
		if _, err := insert.ExecContext(ctx, stage.contractID, stage.ledger, stage.ledgerKeyHash, stage.holder, stage.balance); err != nil {
			insert.Close()
			return result, fmt.Errorf("insert contract balance stage: %w", err)
		}
	}
	if err := insert.Close(); err != nil {
		return result, fmt.Errorf("close contract balance stage insert: %w", err)
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s AS target
		SET balance_holder = stage.balance_holder,
		    balance = stage.balance
		FROM contract_balance_enrichment_stage AS stage
		WHERE target.contract_id = stage.contract_id
		  AND target.ledger_sequence = stage.ledger_sequence
		  AND target.ledger_key_hash = stage.ledger_key_hash
		  AND (target.balance_holder IS NULL OR target.balance IS NULL)
	`, target)); err != nil {
		return result, fmt.Errorf("update enriched contract balances: %w", err)
	}

	var unverified int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM contract_balance_enrichment_stage AS stage
		WHERE NOT EXISTS (
			SELECT 1
			FROM %s AS target
			WHERE target.contract_id = stage.contract_id
			  AND target.ledger_sequence = stage.ledger_sequence
			  AND target.ledger_key_hash = stage.ledger_key_hash
			  AND target.balance_holder = stage.balance_holder
			  AND target.balance = stage.balance
		)
	`, target)).Scan(&unverified); err != nil {
		return result, fmt.Errorf("verify enriched contract balances: %w", err)
	}
	if unverified != 0 {
		return result, fmt.Errorf("contract balance verification failed for %d staged rows", unverified)
	}

	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit contract balance enrichment: %w", err)
	}
	committed = true
	result.UpdatedRows = int64(len(stagedByKey))
	return result, nil
}
