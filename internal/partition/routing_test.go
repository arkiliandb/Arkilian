package partition

import (
	"testing"
	"time"

	"github.com/arkilian/arkilian/pkg/types"
)

func TestRouteByTime(t *testing.T) {
	// 2026-02-06 12:30:00 UTC in nanoseconds
	ts := time.Date(2026, 2, 6, 12, 30, 0, 0, time.UTC).UnixNano()
	row := types.Row{EventTime: ts, TenantID: "acme", UserID: 1}

	router, err := NewRouter(types.PartitionKeyConfig{Strategy: types.StrategyTime})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key, err := router.RouteRow(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.Value != "20260206" {
		t.Errorf("expected 20260206, got %s", key.Value)
	}
	if key.Strategy != types.StrategyTime {
		t.Errorf("expected strategy time, got %s", key.Strategy)
	}
}

func TestRouteByTenant(t *testing.T) {
	row := types.Row{TenantID: "acme-corp", UserID: 42, EventTime: time.Now().UnixNano()}

	router, err := NewRouter(types.PartitionKeyConfig{Strategy: types.StrategyTenant})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key, err := router.RouteRow(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key.Value != "acme-corp" {
		t.Errorf("expected acme-corp, got %s", key.Value)
	}
}

func TestRouteByTenantEmptyReturnsError(t *testing.T) {
	row := types.Row{TenantID: "", UserID: 1, EventTime: time.Now().UnixNano()}

	router, err := NewRouter(types.PartitionKeyConfig{Strategy: types.StrategyTenant})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = router.RouteRow(row)
	if err == nil {
		t.Fatal("expected error for empty tenant_id")
	}
}

func TestRouteByHash(t *testing.T) {
	router, err := NewRouter(types.PartitionKeyConfig{Strategy: types.StrategyHash, HashModulo: 8})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	row := types.Row{UserID: 12345, TenantID: "t", EventTime: time.Now().UnixNano()}
	key, err := router.RouteRow(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The bucket should be deterministic and in range [0, 8)
	if key.Strategy != types.StrategyHash {
		t.Errorf("expected strategy hash, got %s", key.Strategy)
	}

	// Same user ID should always produce the same key
	key2, _ := router.RouteRow(row)
	if key.Value != key2.Value {
		t.Errorf("hash routing not deterministic: %s vs %s", key.Value, key2.Value)
	}
}

func TestRouteByHashInvalidModulo(t *testing.T) {
	_, err := NewRouter(types.PartitionKeyConfig{Strategy: types.StrategyHash, HashModulo: 0})
	if err == nil {
		t.Fatal("expected error for hash_modulo=0")
	}
}

func TestRouteRows(t *testing.T) {
	router, err := NewRouter(types.PartitionKeyConfig{Strategy: types.StrategyTenant})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rows := []types.Row{
		{TenantID: "alpha", UserID: 1, EventTime: time.Now().UnixNano()},
		{TenantID: "beta", UserID: 2, EventTime: time.Now().UnixNano()},
		{TenantID: "alpha", UserID: 3, EventTime: time.Now().UnixNano()},
	}

	groups, err := router.RouteRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(groups["alpha"]) != 2 {
		t.Errorf("expected 2 rows for alpha, got %d", len(groups["alpha"]))
	}
	if len(groups["beta"]) != 1 {
		t.Errorf("expected 1 row for beta, got %d", len(groups["beta"]))
	}
}

func TestUnsupportedStrategy(t *testing.T) {
	_, err := NewRouter(types.PartitionKeyConfig{Strategy: "unknown"})
	if err == nil {
		t.Fatal("expected error for unsupported strategy")
	}
}
