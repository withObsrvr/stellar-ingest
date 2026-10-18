package main

// Durable range state, so a multi-day backfill can be interrupted.
//
// The loader already resumes *within* one dispatch: checkpoint.json records
// each shard's last completed ledger, and the DuckLake push is idempotent --
// it deletes the pushed range per table before inserting, so re-running a
// range cannot duplicate rows. What neither provides is an answer to "which
// ranges are already done", because checkpoint.json lives in the output
// directory and that directory is wiped before each dispatch.
//
// Without that answer a 64-million-ledger backfill is driven by an operator
// keeping track externally, and a crash mid-run leaves no record of how far
// it got. This is the gap Item 5 of the throughput recommendations names,
// borrowed from the Stellar archive node: mark an artifact `freezing`, write
// it, then flip to `frozen`. Planning becomes postcondition-driven -- diff
// what is wanted against what the catalog has, and build only the gap.
//
// Because the push is already idempotent, the states mean something slightly
// weaker here than in the archive node, and it is worth being exact:
//
//	frozen    the range was extracted AND pushed to DuckLake. Skip it.
//	freezing  someone claimed it and has not finished. If the claim is
//	          recent, another worker owns it. If the lease has expired, the
//	          run died -- re-derive it, which is safe precisely because the
//	          push deletes before inserting.
//
// So `freezing` is not a correctness barrier, it is a scheduling one: it
// stops two workers burning the same range and stops a resumed plan from
// redoing work it already finished.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// rangeSkippedMarker is how a child tells its parent it did no work.
//
// A child that finds its range already frozen, or held by another worker,
// exits 0 -- there is nothing wrong and a resumed plan must not fail. But the
// parent then reported it as "ok", indistinguishable from a chunk that
// actually built: a re-dispatch of five finished chunks printed "5/5 chunks
// succeeded in 87ms", which is true and useless.
//
// An exit code would be the obvious channel and is the wrong one: a non-zero
// exit marks the whole Nomad allocation failed, so a single-range dispatch of
// an already-complete range would look broken.
//
// Parent and child are the same binary, so this is one constant rather than
// a string match across a boundary.
const rangeSkippedMarker = "RANGE-ALREADY-COMPLETE"

// Range state values. Deliberately the archive node's words rather than
// "running"/"done", so the borrowed design stays legible against their code.
const (
	rangeFreezing = "freezing"
	rangeFrozen   = "frozen"
)

// defaultClaimLease is how long a `freezing` claim is honoured before the
// range is treated as abandoned.
//
// It has to exceed the longest plausible single-range run. The validation
// dispatches took about three minutes for 2,000 ledgers of current-era
// traffic, and a 10,000-ledger partition of the heaviest history is the
// realistic worst case, so this is roughly an order of magnitude of
// headroom. Too short and two workers duplicate effort; too long and a
// crashed range sits unclaimed. Only the first costs correctness-adjacent
// confusion, and only when the push is mid-flight.
const defaultClaimLease = 2 * time.Hour

// LedgerRange is a half-open-free, inclusive ledger span: the same geometry
// the loader's --start/--end take.
type LedgerRange struct {
	Start uint32
	End   uint32
}

func (r LedgerRange) String() string { return fmt.Sprintf("%d-%d", r.Start, r.End) }

// RangeState is one row of the range catalog.
type RangeState struct {
	Range       LedgerRange
	State       string
	AttemptID   string
	Version     string
	ClaimedAt   time.Time
	CompletedAt time.Time
	RowsPushed  int64
}

// RangePlan is the result of diffing wanted against the catalog.
type RangePlan struct {
	Todo    []LedgerRange // to build: never seen, or abandoned mid-build
	Frozen  []LedgerRange // already complete; skipped
	Claimed []LedgerRange // held by a live claim from another worker
}

// planRanges diffs the wanted set against what the catalog holds.
//
// Pure, and the whole decision lives here: everything else in this file is
// storage. A range is built when it is absent or abandoned, skipped when it
// is frozen, and left alone when another worker holds a live claim.
func planRanges(wanted []LedgerRange, have map[LedgerRange]RangeState, lease time.Duration, now time.Time) RangePlan {
	var plan RangePlan
	for _, r := range wanted {
		st, seen := have[r]
		switch {
		case !seen:
			plan.Todo = append(plan.Todo, r)
		case st.State == rangeFrozen:
			plan.Frozen = append(plan.Frozen, r)
		case st.State == rangeFreezing && now.Sub(st.ClaimedAt) >= lease:
			// The claim outlived its lease, so the run that took it is gone.
			// Re-deriving is safe: the push deletes the range per table
			// before inserting, so a half-finished push is overwritten
			// rather than doubled.
			plan.Todo = append(plan.Todo, r)
		default:
			plan.Claimed = append(plan.Claimed, r)
		}
	}
	return plan
}

// enumerateRanges splits a span into fixed-size ranges aligned to size, which
// is how the backfill is actually dispatched. The final range is truncated to
// end rather than overshooting.
func enumerateRanges(start, end, size uint32) []LedgerRange {
	if size == 0 || end < start {
		return nil
	}
	var out []LedgerRange
	for s := start; s <= end; s += size {
		e := s + size - 1
		if e > end {
			e = end
		}
		out = append(out, LedgerRange{Start: s, End: e})
		if e == end {
			break
		}
	}
	return out
}

// RangeStateStore persists range state in the DuckLake catalog's PostgreSQL
// database -- the same database, but its own table. It is deliberately not
// inside the DuckLake metadata schema: that schema is DuckLake's, and a
// foreign table in it is the kind of thing that survives until an upgrade
// decides otherwise.
type RangeStateStore struct {
	pool  *pgxpool.Pool
	table string
	eraID string
}

func NewRangeStateStore(ctx context.Context, dsn, table, eraID string) (*RangeStateStore, error) {
	if table == "" {
		table = "public.ingest_range_state"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to range state catalog: %w", err)
	}
	s := &RangeStateStore{pool: pool, table: table, eraID: eraID}
	if err := s.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *RangeStateStore) Close() { s.pool.Close() }

func (s *RangeStateStore) ensureSchema(ctx context.Context) error {
	// era_id is part of the key so a validation era and a production era can
	// share a catalog without colliding -- the same isolation the validation
	// dispatches got from a separate metadata schema, available per-run.
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			era_id           TEXT        NOT NULL,
			start_ledger     BIGINT      NOT NULL,
			end_ledger       BIGINT      NOT NULL,
			state            TEXT        NOT NULL CHECK (state IN ('freezing','frozen')),
			attempt_id       TEXT        NOT NULL,
			pipeline_version TEXT,
			claimed_at       TIMESTAMPTZ NOT NULL,
			completed_at     TIMESTAMPTZ,
			rows_pushed      BIGINT,
			PRIMARY KEY (era_id, start_ledger, end_ledger)
		)`, s.table))
	if err != nil {
		return fmt.Errorf("create range state table: %w", err)
	}
	return nil
}

// Load returns every range the catalog knows about for this era.
func (s *RangeStateStore) Load(ctx context.Context) (map[LedgerRange]RangeState, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT start_ledger, end_ledger, state, attempt_id,
		       COALESCE(pipeline_version,''), claimed_at,
		       COALESCE(completed_at, 'epoch'::timestamptz), COALESCE(rows_pushed, 0)
		  FROM %s WHERE era_id = $1`, s.table), s.eraID)
	if err != nil {
		return nil, fmt.Errorf("load range state: %w", err)
	}
	defer rows.Close()

	have := map[LedgerRange]RangeState{}
	for rows.Next() {
		var st RangeState
		var start, end int64
		if err := rows.Scan(&start, &end, &st.State, &st.AttemptID, &st.Version,
			&st.ClaimedAt, &st.CompletedAt, &st.RowsPushed); err != nil {
			return nil, fmt.Errorf("scan range state: %w", err)
		}
		st.Range = LedgerRange{Start: uint32(start), End: uint32(end)}
		have[st.Range] = st
	}
	return have, rows.Err()
}

// Claim marks a range `freezing`. It succeeds when the range is unknown, or
// when an existing claim has outlived its lease. It refuses a frozen range
// and a live claim held by someone else, so two workers cannot take the same
// range.
func (s *RangeStateStore) Claim(ctx context.Context, r LedgerRange, attemptID, version string, lease time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (era_id, start_ledger, end_ledger, state, attempt_id,
		                pipeline_version, claimed_at)
		VALUES ($1, $2, $3, 'freezing', $4, $5, NOW())
		ON CONFLICT (era_id, start_ledger, end_ledger) DO UPDATE
		   SET state = 'freezing', attempt_id = EXCLUDED.attempt_id,
		       pipeline_version = EXCLUDED.pipeline_version,
		       claimed_at = NOW(), completed_at = NULL, rows_pushed = NULL
		 WHERE %s.state = 'freezing'
		   AND %s.claimed_at < NOW() - $6::interval`,
		s.table, s.table, s.table),
		s.eraID, int64(r.Start), int64(r.End), attemptID, version, lease.String())
	if err != nil {
		return false, fmt.Errorf("claim range %s: %w", r, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Freeze marks a range complete. Called only after the DuckLake push returns
// without error -- that ordering is the whole point, because a range is not
// done when its Parquet exists, it is done when the push lands.
func (s *RangeStateStore) Freeze(ctx context.Context, r LedgerRange, attemptID string, rowsPushed int64) error {
	// attempt_id is in the predicate on purpose: freezing must only succeed
	// while we still hold the claim we took. If the lease expired and another
	// run re-claimed mid-push, this updates nothing and the caller is told,
	// rather than one run marking another's range frozen.
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`
		UPDATE %s SET state = 'frozen', completed_at = NOW(), rows_pushed = $4
		 WHERE era_id = $1 AND start_ledger = $2 AND end_ledger = $3
		   AND attempt_id = $5 AND state = 'freezing'`,
		s.table), s.eraID, int64(r.Start), int64(r.End), rowsPushed, attemptID)
	if err != nil {
		return fmt.Errorf("freeze range %s: %w", r, err)
	}
	if tag.RowsAffected() != 1 {
		// Our claim is gone -- the lease expired and another run took it, or
		// the row was deleted. The data is pushed either way, since the push
		// is idempotent; what is lost is this run's right to declare the
		// range done, and the other run will redo and freeze it.
		return fmt.Errorf("freeze range %s: claim no longer held by attempt %s", r, attemptID)
	}
	return nil
}

// Plan loads the catalog and diffs it against the wanted set.
func (s *RangeStateStore) Plan(ctx context.Context, wanted []LedgerRange, lease time.Duration) (RangePlan, error) {
	have, err := s.Load(ctx)
	if err != nil {
		return RangePlan{}, err
	}
	return planRanges(wanted, have, lease, time.Now()), nil
}

// formatPlan renders a plan for an operator, and for a pipe.
//
// Every range to build is printed, one "start end" pair per line, so the
// output feeds a dispatch loop directly. Nothing is truncated: a plan that
// quietly capped its own output would read as "less work than there is",
// which is the opposite of what this is for.
//
// The summary goes last so it survives a `| tail`, and the counts are
// ledgers as well as ranges because ranges are a poor proxy for time when
// the final one is short.
func formatPlan(p RangePlan) string {
	var b strings.Builder
	sort.Slice(p.Todo, func(i, j int) bool { return p.Todo[i].Start < p.Todo[j].Start })
	for _, r := range p.Todo {
		fmt.Fprintf(&b, "%d %d\n", r.Start, r.End)
	}
	if len(p.Claimed) > 0 {
		sort.Slice(p.Claimed, func(i, j int) bool { return p.Claimed[i].Start < p.Claimed[j].Start })
		fmt.Fprintf(&b, "# %d range(s) held by a live claim, not listed above:\n", len(p.Claimed))
		for _, r := range p.Claimed {
			fmt.Fprintf(&b, "#   %s\n", r)
		}
	}
	fmt.Fprintf(&b, "# %s\n", SummarizePlan(p))
	return b.String()
}

// SummarizePlan renders a plan for an operator deciding whether to start a
// multi-day run.
func SummarizePlan(p RangePlan) string {
	ledgers := func(rs []LedgerRange) uint64 {
		var n uint64
		for _, r := range rs {
			n += uint64(r.End-r.Start) + 1
		}
		return n
	}
	sort.Slice(p.Todo, func(i, j int) bool { return p.Todo[i].Start < p.Todo[j].Start })
	s := fmt.Sprintf("plan: %d ranges to build (%d ledgers), %d already frozen (%d ledgers)",
		len(p.Todo), ledgers(p.Todo), len(p.Frozen), ledgers(p.Frozen))
	if len(p.Claimed) > 0 {
		s += fmt.Sprintf(", %d held by live claims", len(p.Claimed))
	}
	return s
}
