package planner

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/pkg/types"
)

// mockSchemaProvider implements SchemaProvider for testing.
type mockSchemaProvider struct {
	schemas map[int]*SchemaRecord
}

func (m *mockSchemaProvider) GetSchemaVersion(_ context.Context, version int) (*SchemaRecord, error) {
	rec, ok := m.schemas[version]
	if !ok {
		return nil, fmt.Errorf("schema version %d not found", version)
	}
	return rec, nil
}

func schemaV1() types.Schema {
	return types.Schema{
		Version: 1,
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT"},
			{Name: "user_id", Type: "INTEGER"},
			{Name: "event_time", Type: "INTEGER"},
			{Name: "event_type", Type: "TEXT"},
			{Name: "payload", Type: "BLOB"},
		},
	}
}

func schemaV2() types.Schema {
	return types.Schema{
		Version: 2,
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT"},
			{Name: "user_id", Type: "INTEGER"},
			{Name: "event_time", Type: "INTEGER"},
			{Name: "event_type", Type: "TEXT"},
			{Name: "payload", Type: "BLOB"},
			{Name: "region", Type: "TEXT", Nullable: true},
		},
	}
}

func newMockProvider() *mockSchemaProvider {
	return &mockSchemaProvider{
		schemas: map[int]*SchemaRecord{
			1: {Version: 1, Schema: schemaV1()},
			2: {Version: 2, Schema: schemaV2()},
		},
	}
}

func TestRewriter_SameVersion_NoRewrite(t *testing.T) {
	rewriter := NewRewriter(newMockProvider())
	ctx := context.Background()

	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Expr: &parser.ColumnRef{Column: "tenant_id"}},
		},
		From: &parser.TableRef{Name: "events"},
	}

	plan := &QueryPlan{
		Statement: stmt,
		Partitions: []*manifest.PartitionRecord{
			{PartitionID: "p1", SchemaVersion: 1},
			{PartitionID: "p2", SchemaVersion: 1},
		},
	}

	results, err := rewriter.RewriteForPartitions(ctx, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Both should use the original statement (no rewrite)
	for _, r := range results {
		if len(r.MissingColumns) != 0 {
			t.Errorf("expected no missing columns, got %v", r.MissingColumns)
		}
		if r.Statement != stmt {
			t.Error("expected original statement pointer for same-version partitions")
		}
	}
}

func TestRewriter_MixedVersions_RewritesMissingColumns(t *testing.T) {
	rewriter := NewRewriter(newMockProvider())
	ctx := context.Background()

	// Query references "region" which only exists in v2
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Expr: &parser.ColumnRef{Column: "tenant_id"}},
			{Expr: &parser.ColumnRef{Column: "region"}},
		},
		From: &parser.TableRef{Name: "events"},
	}

	plan := &QueryPlan{
		Statement: stmt,
		Partitions: []*manifest.PartitionRecord{
			{PartitionID: "p1", SchemaVersion: 1}, // old — missing "region"
			{PartitionID: "p2", SchemaVersion: 2}, // new — has "region"
		},
	}

	results, err := rewriter.RewriteForPartitions(ctx, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// p1 (v1) should have "region" as missing and rewritten to NULL
	r1 := results[0]
	if len(r1.MissingColumns) != 1 || r1.MissingColumns[0] != "region" {
		t.Errorf("expected missing column 'region' for v1 partition, got %v", r1.MissingColumns)
	}

	// The rewritten statement should have NULL instead of region column ref
	rewrittenSQL := r1.Statement.String()
	if !strings.Contains(rewrittenSQL, "NULL") {
		t.Errorf("expected NULL in rewritten SQL for v1 partition, got: %s", rewrittenSQL)
	}

	// p2 (v2) should have no missing columns
	r2 := results[1]
	if len(r2.MissingColumns) != 0 {
		t.Errorf("expected no missing columns for v2 partition, got %v", r2.MissingColumns)
	}
}

func TestRewriter_WhereClauseRewriting(t *testing.T) {
	rewriter := NewRewriter(newMockProvider())
	ctx := context.Background()

	// Query with WHERE on a column missing in v1
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Expr: &parser.ColumnRef{Column: "tenant_id"}},
		},
		From: &parser.TableRef{Name: "events"},
		Where: &parser.BinaryExpr{
			Left:     &parser.ColumnRef{Column: "region"},
			Operator: "=",
			Right:    &parser.Literal{Value: "us-east-1"},
		},
	}

	plan := &QueryPlan{
		Statement: stmt,
		Partitions: []*manifest.PartitionRecord{
			{PartitionID: "p1", SchemaVersion: 1}, // old — missing "region"
			{PartitionID: "p2", SchemaVersion: 2}, // new — has "region"
		},
	}

	results, err := rewriter.RewriteForPartitions(ctx, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// p1 (v1) WHERE clause should have NULL instead of region
	r := results[0]
	rewrittenSQL := r.Statement.String()
	if !strings.Contains(rewrittenSQL, "NULL") {
		t.Errorf("expected NULL in WHERE clause for v1 partition, got: %s", rewrittenSQL)
	}

	// p2 (v2) should keep original region reference
	r2 := results[1]
	r2SQL := r2.Statement.String()
	if strings.Contains(r2SQL, "NULL") {
		t.Errorf("v2 partition should not have NULL substitution, got: %s", r2SQL)
	}
}

func TestNeedsRewriting(t *testing.T) {
	tests := []struct {
		name       string
		partitions []*manifest.PartitionRecord
		want       bool
	}{
		{
			name:       "empty",
			partitions: nil,
			want:       false,
		},
		{
			name: "single partition",
			partitions: []*manifest.PartitionRecord{
				{SchemaVersion: 1},
			},
			want: false,
		},
		{
			name: "same versions",
			partitions: []*manifest.PartitionRecord{
				{SchemaVersion: 2},
				{SchemaVersion: 2},
			},
			want: false,
		},
		{
			name: "mixed versions",
			partitions: []*manifest.PartitionRecord{
				{SchemaVersion: 1},
				{SchemaVersion: 2},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NeedsRewriting(tt.partitions)
			if got != tt.want {
				t.Errorf("NeedsRewriting() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDetectSchemaVersions(t *testing.T) {
	partitions := []*manifest.PartitionRecord{
		{SchemaVersion: 1},
		{SchemaVersion: 2},
		{SchemaVersion: 1},
		{SchemaVersion: 3},
	}

	versions := DetectSchemaVersions(partitions)
	if len(versions) != 3 {
		t.Errorf("expected 3 distinct versions, got %d", len(versions))
	}
}

func TestExtractReferencedColumns(t *testing.T) {
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Expr: &parser.ColumnRef{Column: "tenant_id"}},
			{Expr: &parser.AggregateExpr{Function: "COUNT", Arg: &parser.ColumnRef{Column: "user_id"}}},
		},
		From: &parser.TableRef{Name: "events"},
		Where: &parser.BinaryExpr{
			Left:     &parser.ColumnRef{Column: "region"},
			Operator: "=",
			Right:    &parser.Literal{Value: "us-east-1"},
		},
		GroupBy: []parser.Expression{
			&parser.ColumnRef{Column: "tenant_id"},
		},
		OrderBy: []parser.OrderByClause{
			{Expr: &parser.ColumnRef{Column: "event_time"}, Desc: true},
		},
	}

	cols := extractReferencedColumns(stmt)
	expected := map[string]bool{
		"tenant_id":  true,
		"user_id":    true,
		"region":     true,
		"event_time": true,
	}

	if len(cols) != len(expected) {
		t.Errorf("expected %d columns, got %d: %v", len(expected), len(cols), cols)
	}
	for _, c := range cols {
		if !expected[c] {
			t.Errorf("unexpected column: %s", c)
		}
	}
}
