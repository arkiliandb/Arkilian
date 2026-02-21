package index

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/config"
	"github.com/arkilian/arkilian/internal/observability"
)

// mockIndexCatalog is a mock implementation of IndexCatalog for testing.
type mockIndexCatalog struct {
	indexes     map[string][]string
	listError   error
	deleteError error
}

func newMockIndexCatalog() *mockIndexCatalog {
	return &mockIndexCatalog{
		indexes: make(map[string][]string),
	}
}

func (m *mockIndexCatalog) RegisterIndexPartition(ctx context.Context, info *IndexPartitionInfo) error {
	return nil
}

func (m *mockIndexCatalog) FindIndexPartition(ctx context.Context, collection, column string, bucketID int) (*IndexPartitionInfo, error) {
	return nil, nil
}

func (m *mockIndexCatalog) ListIndexes(ctx context.Context, collection string) ([]string, error) {
	if m.listError != nil {
		return nil, m.listError
	}
	return m.indexes[collection], nil
}

func (m *mockIndexCatalog) DeleteIndex(ctx context.Context, collection, column string) ([]string, error) {
	if m.deleteError != nil {
		return nil, m.deleteError
	}
	delete(m.indexes, collection)
	return []string{fmt.Sprintf("indexes/%s/%s/0.sqlite", collection, column)}, nil
}

// mockPartitionProvider is a mock implementation of PartitionProvider for testing.
type mockPartitionProvider struct {
	partitions []*PartitionInfo
	getError   error
}

func newMockPartitionProvider() *mockPartitionProvider {
	return &mockPartitionProvider{
		partitions: make([]*PartitionInfo, 0),
	}
}

func (m *mockPartitionProvider) GetPartitions(ctx context.Context) ([]*PartitionInfo, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	result := make([]*PartitionInfo, len(m.partitions))
	copy(result, m.partitions)
	return result, nil
}

// TestPolicy_ColumnAboveThresholdTriggersCreate verifies that a column with frequency
// above the create threshold triggers a CREATE action.
func TestPolicy_ColumnAboveThresholdTriggersCreate(t *testing.T) {
	stats := observability.NewQueryStats(1 * time.Hour)
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")

	mockCatalog := newMockIndexCatalog()
	mockDataCatalog := newMockPartitionProvider()

	cfg := config.IndexConfig{
		Enabled:         true,
		CreateThreshold: 3,
		DropThreshold:   1,
		CheckInterval:   1 * time.Minute,
		MaxIndexes:      10,
		BucketCount:     64,
	}

	// Note: builder and storage are nil because evaluate() doesn't use them
	policy := NewPolicy(stats, nil, mockCatalog, mockDataCatalog, nil, cfg)

	actions, err := policy.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}

	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}

	if actions[0].Type != ActionCreate {
		t.Errorf("expected CREATE action, got %s", actions[0].Type)
	}

	if actions[0].Column != "user_id" {
		t.Errorf("expected column 'user_id', got '%s'", actions[0].Column)
	}
}

// TestPolicy_ColumnBelowThresholdTriggersDrop verifies that a column with frequency
// below the drop threshold triggers a DROP action.
func TestPolicy_ColumnBelowThresholdTriggersDrop(t *testing.T) {
	stats := observability.NewQueryStats(1 * time.Hour)
	// Record some predicates but not for the indexed column
	stats.RecordPredicate("other_column", "=")

	mockCatalog := newMockIndexCatalog()
	mockCatalog.indexes["events"] = []string{"user_id"} // user_id is already indexed

	mockDataCatalog := newMockPartitionProvider()

	cfg := config.IndexConfig{
		Enabled:         true,
		CreateThreshold: 100,
		DropThreshold:   5,
		CheckInterval:   1 * time.Minute,
		MaxIndexes:      10,
		BucketCount:     64,
	}

	policy := NewPolicy(stats, nil, mockCatalog, mockDataCatalog, nil, cfg)

	actions, err := policy.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}

	// Should have a DROP action for user_id since it's below threshold
	found := false
	for _, action := range actions {
		if action.Type == ActionDrop && action.Column == "user_id" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("expected DROP action for 'user_id', got actions: %+v", actions)
	}
}

// TestPolicy_MaxIndexesLimitRespected verifies that the MaxIndexes limit is respected
// and no more than maxIndexes indexes are created.
func TestPolicy_MaxIndexesLimitRespected(t *testing.T) {
	stats := observability.NewQueryStats(1 * time.Hour)

	// Record many predicates above threshold
	for i := 0; i < 15; i++ {
		col := string(rune('a' + i))
		stats.RecordPredicate(col, "=")
		stats.RecordPredicate(col, "=")
		stats.RecordPredicate(col, "=")
		stats.RecordPredicate(col, "=")
	}

	mockCatalog := newMockIndexCatalog()
	mockDataCatalog := newMockPartitionProvider()

	cfg := config.IndexConfig{
		Enabled:         true,
		CreateThreshold: 3,
		DropThreshold:   1,
		CheckInterval:   1 * time.Minute,
		MaxIndexes:      5, // Limit to 5 indexes
		BucketCount:     64,
	}

	policy := NewPolicy(stats, nil, mockCatalog, mockDataCatalog, nil, cfg)

	actions, err := policy.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}

	createCount := 0
	for _, action := range actions {
		if action.Type == ActionCreate {
			createCount++
		}
	}

	if createCount > cfg.MaxIndexes {
		t.Errorf("expected at most %d CREATE actions, got %d", cfg.MaxIndexes, createCount)
	}
}

// TestPolicy_ExistingIndexNotRecreated verifies that an existing index is not
// recreated even if the column is frequently queried.
func TestPolicy_ExistingIndexNotRecreated(t *testing.T) {
	stats := observability.NewQueryStats(1 * time.Hour)
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")
	stats.RecordPredicate("user_id", "=")

	mockCatalog := newMockIndexCatalog()
	mockCatalog.indexes["events"] = []string{"user_id"} // user_id already indexed

	mockDataCatalog := newMockPartitionProvider()

	cfg := config.IndexConfig{
		Enabled:         true,
		CreateThreshold: 3,
		DropThreshold:   1,
		CheckInterval:   1 * time.Minute,
		MaxIndexes:      10,
		BucketCount:     64,
	}

	policy := NewPolicy(stats, nil, mockCatalog, mockDataCatalog, nil, cfg)

	actions, err := policy.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}

	for _, action := range actions {
		if action.Type == ActionCreate && action.Column == "user_id" {
			t.Errorf("should not create index for 'user_id' - it already exists")
		}
	}
}

// TestPolicy_NoActionsWhenThresholdsNotMet verifies that no actions are generated
// when no columns meet the create or drop thresholds.
func TestPolicy_NoActionsWhenThresholdsNotMet(t *testing.T) {
	stats := observability.NewQueryStats(1 * time.Hour)
	// Record predicates but below create threshold
	stats.RecordPredicate("column_a", "=")
	stats.RecordPredicate("column_b", "=")

	mockCatalog := newMockIndexCatalog()
	// No existing indexes
	mockDataCatalog := newMockPartitionProvider()

	cfg := config.IndexConfig{
		Enabled:         true,
		CreateThreshold: 100, // High threshold
		DropThreshold:   5,
		CheckInterval:   1 * time.Minute,
		MaxIndexes:      10,
		BucketCount:     64,
	}

	policy := NewPolicy(stats, nil, mockCatalog, mockDataCatalog, nil, cfg)

	actions, err := policy.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}

	if len(actions) != 0 {
		t.Errorf("expected 0 actions, got %d: %+v", len(actions), actions)
	}
}