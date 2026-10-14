package main

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func createContractBalanceManifestFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		CREATE TABLE contract_balance_enrichment_manifest (
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
	`); err != nil {
		t.Fatalf("create enrichment manifest fixture: %v", err)
	}
}

func manifestTestEnricher(db *sql.DB, network string) *ContractBalanceEnricher {
	return &ContractBalanceEnricher{
		config: ContractBalanceEnricherConfig{
			RunID:        "shared-repair-run",
			Network:      network,
			VersionLabel: "test",
		},
		pusher:   &DuckLakePusher{db: db},
		manifest: "contract_balance_enrichment_manifest",
	}
}

const historicalNativeBalanceXDR = "AAAAAAAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAABAAAAABAAAAAgAAAA8AAAAHQmFsYW5jZQAAAAASAAAAAaAYIAbRNL61yGL1IwE4Zd6B7YCDUI7STCNQPchp4CbqAAAAAQAAABEAAAABAAAAAwAAAA8AAAAGYW1vdW50AAAAAAAKAAAAAAAAAAAAAAAXRXv3gAAAAA8AAAAKYXV0aG9yaXplZAAAAAAAAAAAAAEAAAAPAAAACGNsYXdiYWNrAAAAAAAAAAA="

func TestDecodeContractBalanceDataXDRHistoricalNativeBalance(t *testing.T) {
	holder, balance, ok, err := decodeContractBalanceDataXDR(historicalNativeBalanceXDR)
	if err != nil {
		t.Fatalf("decode historical contract data XDR: %v", err)
	}
	if !ok {
		t.Fatal("historical native balance entry was not recognized")
	}
	if holder != "CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG" {
		t.Fatalf("holder = %q", holder)
	}
	if balance != "99950000000" {
		t.Fatalf("balance = %q, want raw stroops 99950000000", balance)
	}
}

func TestEnrichContractBalanceChunkIsVerifiedAndIdempotent(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `
		CREATE TABLE contract_data_snapshot_v1 (
			contract_id VARCHAR,
			ledger_sequence BIGINT,
			ledger_key_hash VARCHAR,
			contract_key_type VARCHAR,
			contract_data_xdr VARCHAR,
			balance_holder VARCHAR,
			balance VARCHAR
		);
		INSERT INTO contract_data_snapshot_v1 VALUES
			('CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC', 2996166, 'd2ccddb78a822eb99301d8ddf17861a387392655834cf957b708aba18eee4241', 'ScValTypeScvVec', ?, NULL, NULL);
	`, historicalNativeBalanceXDR)
	if err != nil {
		t.Fatalf("seed contract data: %v", err)
	}

	result, err := enrichContractBalanceChunk(ctx, db, "contract_data_snapshot_v1", 2_900_000, 3_000_000, false)
	if err != nil {
		t.Fatalf("enrich chunk: %v", err)
	}
	if result.CandidateRows != 1 || result.DecodedRows != 1 || result.UpdatedRows != 1 || result.SkippedRows != 0 {
		t.Fatalf("unexpected first result: %+v", result)
	}

	var holder, balance string
	if err := db.QueryRowContext(ctx, `SELECT balance_holder, balance FROM contract_data_snapshot_v1`).Scan(&holder, &balance); err != nil {
		t.Fatalf("read repaired row: %v", err)
	}
	if holder != "CCQBQIAG2E2L5NOIML2SGAJYMXPID3MAQNII5USMENID3SDJ4ATOU2HG" || balance != "99950000000" {
		t.Fatalf("repaired row = holder %q, balance %q", holder, balance)
	}

	second, err := enrichContractBalanceChunk(ctx, db, "contract_data_snapshot_v1", 2_900_000, 3_000_000, false)
	if err != nil {
		t.Fatalf("rerun chunk: %v", err)
	}
	if second.CandidateRows != 0 || second.UpdatedRows != 0 {
		t.Fatalf("rerun was not idempotent: %+v", second)
	}
}

func TestEnrichmentManifestCompletionIsScopedByNetwork(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	createContractBalanceManifestFixture(t, db)

	ctx := context.Background()
	if _, err := db.Exec(`
		INSERT INTO contract_balance_enrichment_manifest (
			run_id, network, chunk_start, chunk_end, status
		) VALUES ('shared-repair-run', 'testnet', 3, 100002, 'completed')
	`); err != nil {
		t.Fatalf("seed testnet completion: %v", err)
	}

	completed, err := manifestTestEnricher(db, "mainnet").chunkCompleted(ctx, 3, 100002)
	if err != nil {
		t.Fatalf("check mainnet completion: %v", err)
	}
	if completed {
		t.Fatal("mainnet chunk was incorrectly completed by a testnet manifest row with the same run and range")
	}
}

func TestEnrichmentManifestReplacementPreservesOtherNetworks(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	createContractBalanceManifestFixture(t, db)

	ctx := context.Background()
	if _, err := db.Exec(`
		INSERT INTO contract_balance_enrichment_manifest (
			run_id, network, chunk_start, chunk_end, status
		) VALUES ('shared-repair-run', 'testnet', 3, 100002, 'completed')
	`); err != nil {
		t.Fatalf("seed testnet completion: %v", err)
	}

	startedAt := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	if err := manifestTestEnricher(db, "mainnet").recordManifest(
		ctx, 3, 100002, "running", ContractBalanceEnrichmentResult{}, startedAt, nil, "",
	); err != nil {
		t.Fatalf("record mainnet manifest: %v", err)
	}

	var testnetRows, mainnetRows int64
	if err := db.QueryRow(`
		SELECT
			count(*) FILTER (WHERE network = 'testnet' AND status = 'completed'),
			count(*) FILTER (WHERE network = 'mainnet' AND status = 'running')
		FROM contract_balance_enrichment_manifest
	`).Scan(&testnetRows, &mainnetRows); err != nil {
		t.Fatalf("verify network-scoped manifest replacement: %v", err)
	}
	if testnetRows != 1 || mainnetRows != 1 {
		t.Fatalf("manifest rows after mainnet replacement = testnet completed %d, mainnet running %d; want 1/1", testnetRows, mainnetRows)
	}
}
