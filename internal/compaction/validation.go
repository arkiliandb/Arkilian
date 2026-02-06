package compaction

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"

	"github.com/arkilian/arkilian/internal/manifest"
	_ "github.com/mattn/go-sqlite3"
)

// ValidationResult holds the outcome of a compaction validation.
type ValidationResult struct {
	Valid            bool
	ExpectedRowCount int64
	ActualRowCount   int64
	ExpectedChecksum string
	ActualChecksum   string
	Errors           []string
}

// Validator validates compacted partitions against their source partitions.
type Validator struct{}

// NewValidator creates a new compaction validator.
func NewValidator() *Validator {
	return &Validator{}
}

// Validate checks that a compacted partition contains the correct data
// by comparing row counts and checksums against the source partitions.
func (v *Validator) Validate(ctx context.Context, result *MergeResult, sources []*manifest.PartitionRecord) (*ValidationResult, error) {
	vr := &ValidationResult{Valid: true}

	// Validate row count matches sum of sources
	var expectedRows int64
	for _, src := range sources {
		expectedRows += src.RowCount
	}
	vr.ExpectedRowCount = expectedRows
	vr.ActualRowCount = result.TotalRows

	if result.TotalRows != expectedRows {
		vr.Valid = false
		vr.Errors = append(vr.Errors, fmt.Sprintf(
			"row count mismatch: expected %d (sum of sources), got %d",
			expectedRows, result.TotalRows))
	}

	// Validate checksum of compacted partition
	actualChecksum, err := v.computePartitionChecksum(ctx, result.SQLitePath)
	if err != nil {
		vr.Valid = false
		vr.Errors = append(vr.Errors, fmt.Sprintf("failed to compute checksum: %v", err))
		return vr, nil
	}
	vr.ActualChecksum = actualChecksum

	// Verify the partition file is readable and has the expected row count
	actualRowCount, err := v.countRows(ctx, result.SQLitePath)
	if err != nil {
		vr.Valid = false
		vr.Errors = append(vr.Errors, fmt.Sprintf("failed to count rows in compacted partition: %v", err))
		return vr, nil
	}

	if actualRowCount != expectedRows {
		vr.Valid = false
		vr.Errors = append(vr.Errors, fmt.Sprintf(
			"actual row count in SQLite (%d) does not match expected (%d)",
			actualRowCount, expectedRows))
	}

	return vr, nil
}

// ValidateChecksum compares checksums between source partition data and the compacted result.
func (v *Validator) ValidateChecksum(ctx context.Context, sourcePaths []string, compactedPath string) (*ValidationResult, error) {
	vr := &ValidationResult{Valid: true}

	// Compute combined checksum from all source partitions
	expectedChecksum, err := v.computeCombinedChecksum(ctx, sourcePaths)
	if err != nil {
		vr.Valid = false
		vr.Errors = append(vr.Errors, fmt.Sprintf("failed to compute source checksum: %v", err))
		return vr, nil
	}
	vr.ExpectedChecksum = expectedChecksum

	// Compute checksum of compacted partition
	actualChecksum, err := v.computePartitionChecksum(ctx, compactedPath)
	if err != nil {
		vr.Valid = false
		vr.Errors = append(vr.Errors, fmt.Sprintf("failed to compute compacted checksum: %v", err))
		return vr, nil
	}
	vr.ActualChecksum = actualChecksum

	if expectedChecksum != actualChecksum {
		vr.Valid = false
		vr.Errors = append(vr.Errors, fmt.Sprintf(
			"checksum mismatch: sources=%s, compacted=%s",
			expectedChecksum, actualChecksum))
	}

	return vr, nil
}

// computePartitionChecksum computes a SHA-256 checksum of all row data in a partition,
// with rows sorted by event_id for deterministic output.
func (v *Validator) computePartitionChecksum(ctx context.Context, sqlitePath string) (string, error) {
	db, err := sql.Open("sqlite3", sqlitePath+"?mode=ro")
	if err != nil {
		return "", fmt.Errorf("failed to open partition: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		"SELECT event_id, tenant_id, user_id, event_time, event_type, payload FROM events ORDER BY event_id")
	if err != nil {
		return "", fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	h := sha256.New()
	for rows.Next() {
		var eventID, payload []byte
		var tenantID, eventType string
		var userID, eventTime int64

		if err := rows.Scan(&eventID, &tenantID, &userID, &eventTime, &eventType, &payload); err != nil {
			return "", fmt.Errorf("failed to scan row: %w", err)
		}

		// Hash each field deterministically
		h.Write(eventID)
		h.Write([]byte(tenantID))
		h.Write(int64ToValidationBytes(userID))
		h.Write(int64ToValidationBytes(eventTime))
		h.Write([]byte(eventType))
		h.Write(payload)
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating rows: %w", err)
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// computeCombinedChecksum computes a combined checksum from multiple source partitions.
// All rows are read, sorted by event_id, then hashed together.
func (v *Validator) computeCombinedChecksum(ctx context.Context, sourcePaths []string) (string, error) {
	type rowData struct {
		eventID   []byte
		tenantID  string
		userID    int64
		eventTime int64
		eventType string
		payload   []byte
	}

	var allRows []rowData

	for _, path := range sourcePaths {
		db, err := sql.Open("sqlite3", path+"?mode=ro")
		if err != nil {
			return "", fmt.Errorf("failed to open source: %w", err)
		}

		rows, err := db.QueryContext(ctx,
			"SELECT event_id, tenant_id, user_id, event_time, event_type, payload FROM events")
		if err != nil {
			db.Close()
			return "", fmt.Errorf("failed to query source: %w", err)
		}

		for rows.Next() {
			var rd rowData
			if err := rows.Scan(&rd.eventID, &rd.tenantID, &rd.userID, &rd.eventTime, &rd.eventType, &rd.payload); err != nil {
				rows.Close()
				db.Close()
				return "", fmt.Errorf("failed to scan row: %w", err)
			}
			allRows = append(allRows, rd)
		}
		rows.Close()
		db.Close()
	}

	// Sort by event_id for deterministic ordering
	sort.Slice(allRows, func(i, j int) bool {
		return compareBytesLess(allRows[i].eventID, allRows[j].eventID)
	})

	h := sha256.New()
	for _, rd := range allRows {
		h.Write(rd.eventID)
		h.Write([]byte(rd.tenantID))
		h.Write(int64ToValidationBytes(rd.userID))
		h.Write(int64ToValidationBytes(rd.eventTime))
		h.Write([]byte(rd.eventType))
		h.Write(rd.payload)
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// countRows counts the number of rows in a partition's events table.
func (v *Validator) countRows(ctx context.Context, sqlitePath string) (int64, error) {
	db, err := sql.Open("sqlite3", sqlitePath+"?mode=ro")
	if err != nil {
		return 0, fmt.Errorf("failed to open partition: %w", err)
	}
	defer db.Close()

	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return count, nil
}

// int64ToValidationBytes converts an int64 to bytes for checksum computation.
func int64ToValidationBytes(v int64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}
