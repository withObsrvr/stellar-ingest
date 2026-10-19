package ingest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CheckpointData tracks per-shard progress for resume capability.
type CheckpointData struct {
	AttemptID   string                  `json:"attempt_id"`
	Version     string                  `json:"pipeline_version"`
	StartLedger uint32                  `json:"start_ledger"`
	EndLedger   uint32                  `json:"end_ledger"`
	Workers     int                     `json:"workers"`
	StorageType string                  `json:"storage_type"`
	Bucket      string                  `json:"bucket"`
	Network     string                  `json:"network_passphrase"`
	EraID       string                  `json:"era_id,omitempty"`
	LedgersFile uint32                  `json:"ledgers_per_file,omitempty"`
	FilesPart   uint32                  `json:"files_per_partition,omitempty"`
	OnlyTables  string                  `json:"only_tables,omitempty"`
	Shards      map[int]ShardCheckpoint `json:"shards"`
	StartedAt   time.Time               `json:"started_at"`
	LastUpdated time.Time               `json:"last_updated"`
}

// ShardCheckpoint tracks progress for a single shard.
type ShardCheckpoint struct {
	ShardID          int       `json:"shard_id"`
	StartLedger      uint32    `json:"start_ledger"`
	EndLedger        uint32    `json:"end_ledger"`
	LastCompletedSeq uint32    `json:"last_completed_seq"`
	Completed        bool      `json:"completed"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// CheckpointManager manages reading/writing checkpoint files.
type CheckpointManager struct {
	mu   sync.Mutex
	path string
	data CheckpointData
}

// NewCheckpointManager creates or loads a checkpoint for the given output directory.
func NewCheckpointManager(outputDir string, config OrchestratorConfig) (*CheckpointManager, error) {
	path := filepath.Join(outputDir, "checkpoint.json")

	cm := &CheckpointManager{
		path: path,
		data: CheckpointData{
			AttemptID:   NewAttemptID(),
			Version:     Version,
			StartLedger: config.StartLedger,
			EndLedger:   config.EndLedger,
			Workers:     config.NumWorkers,
			StorageType: config.StorageType,
			Bucket:      config.Bucket,
			Network:     config.NetworkPassphrase,
			EraID:       config.EraID,
			LedgersFile: config.LedgersPerFile,
			FilesPart:   config.FilesPerPartition,
			OnlyTables:  TableSetKey(config.OnlyTables),
			Shards:      make(map[int]ShardCheckpoint),
			StartedAt:   time.Now(),
			LastUpdated: time.Now(),
		},
	}

	// Try to load existing checkpoint
	if existing, err := cm.load(); err == nil {
		// Validate it matches current config
		if existing.AttemptID != "" &&
			existing.Version == Version &&
			existing.StartLedger == config.StartLedger &&
			existing.EndLedger == config.EndLedger &&
			existing.Workers == config.NumWorkers &&
			existing.StorageType == config.StorageType &&
			existing.Bucket == config.Bucket &&
			existing.Network == config.NetworkPassphrase &&
			existing.EraID == config.EraID &&
			existing.LedgersFile == config.LedgersPerFile &&
			existing.FilesPart == config.FilesPerPartition &&
			existing.OnlyTables == TableSetKey(config.OnlyTables) {
			cm.data = *existing
			return cm, nil
		}
		// Config mismatch — start fresh.
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("load checkpoint %s: %w", path, err)
	}

	return cm, nil
}

// AttemptID identifies the isolated output tree owned by this checkpoint.
func (cm *CheckpointManager) AttemptID() string {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.data.AttemptID
}

func (cm *CheckpointManager) load() (*CheckpointData, error) {
	data, err := os.ReadFile(cm.path)
	if err != nil {
		return nil, err
	}
	var cp CheckpointData
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

// Save writes the checkpoint to disk.
func (cm *CheckpointManager) Save() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.data.LastUpdated = time.Now()
	data, err := json.MarshalIndent(cm.data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	tmpPath := cm.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return os.Rename(tmpPath, cm.path)
}

// UpdateShard records progress for a shard.
func (cm *CheckpointManager) UpdateShard(shardID int, lastCompletedSeq uint32) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sc, ok := cm.data.Shards[shardID]
	if !ok {
		sc = ShardCheckpoint{ShardID: shardID}
	}
	sc.LastCompletedSeq = lastCompletedSeq
	sc.UpdatedAt = time.Now()
	cm.data.Shards[shardID] = sc
}

// CompleteShard marks a shard as fully completed.
func (cm *CheckpointManager) CompleteShard(shardID int, shard Shard) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.data.Shards[shardID] = ShardCheckpoint{
		ShardID:          shardID,
		StartLedger:      shard.StartLedger,
		EndLedger:        shard.EndLedger,
		LastCompletedSeq: shard.EndLedger,
		Completed:        true,
		UpdatedAt:        time.Now(),
	}
}

// GetShardResumePoint returns the ledger to resume from for a shard.
// Returns (startFrom, shouldSkip). If shouldSkip is true, the shard is already done.
func (cm *CheckpointManager) GetShardResumePoint(shardID int, shard Shard) (uint32, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sc, ok := cm.data.Shards[shardID]
	if !ok {
		return shard.StartLedger, false
	}
	if sc.Completed {
		return 0, true // skip this shard
	}
	if sc.LastCompletedSeq >= shard.EndLedger {
		return 0, true // already done
	}
	if sc.LastCompletedSeq > shard.StartLedger {
		return sc.LastCompletedSeq + 1, false // resume from next ledger
	}
	return shard.StartLedger, false
}

func (cm *CheckpointManager) IsShardComplete(shardID int) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.data.Shards[shardID].Completed
}

// AllComplete returns true if all shards are completed.
func (cm *CheckpointManager) AllComplete() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, sc := range cm.data.Shards {
		if !sc.Completed {
			return false
		}
	}
	return len(cm.data.Shards) > 0
}
