// Package integration provides end-to-end integration tests for Project Arkilian.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	apihttp "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/router"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
)

// TestNotificationFiltering tests that notifications are filtered by partition key.
func TestNotificationFiltering(t *testing.T) {
	ctx := context.Background()

	tempDir, err := os.MkdirTemp("", "arkilian-notification-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")
	walDir := filepath.Join(tempDir, "wal")

	for _, dir := range []string{partitionDir, storageDir, walDir} {
		os.MkdirAll(dir, 0755)
	}

	store, _ := storage.NewLocalStorage(storageDir)
	catalog, _ := manifest.NewCatalog(manifestPath)
	defer catalog.Close()

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	walInstance, _ := wal.NewWAL(walDir, 64*1024*1024)
	defer walInstance.Close()

	// Create notifier
	notifier := router.NewNotifier(1000)
	defer notifier.Close()

	// Create flusher with notifier
	flusher := wal.NewFlusher(walInstance, builder, store, catalog, metaGen, 100*time.Millisecond, 1000)
	flusher.SetNotifier(notifier)

	// Subscribe to notifications for partition key "20260206"
	subscriber := notifier.Subscribe("test-sub", []string{"20260206"})

	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, walInstance, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Ingest data
	reqBody := apihttp.IngestRequest{
		PartitionKey: "20260206",
		Rows: []map[string]interface{}{
			{
				"tenant_id":  "acme",
				"user_id":    float64(12345),
				"event_time": float64(time.Now().UnixNano()),
				"event_type": "page_view",
				"payload":    map[string]interface{}{"page": "/home"},
			},
		},
	}
	body, _ := json.Marshal(reqBody)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d - %s", rec.Code, rec.Body.String())
	}

	// Start flusher
	flusherCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

	// Wait for notification
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case notif := <-subscriber.Ch:
		if notif.Type != router.PartitionCreated {
			t.Errorf("expected PartitionCreated, got %v", notif.Type)
		}
		if notif.PartitionKey != "20260206" {
			t.Errorf("expected partition_key=20260206, got %s", notif.PartitionKey)
		}
		t.Logf("Notification received: LSN=%d, partition_id=%s", notif.LSN, notif.PartitionID)
	case <-timer.C:
		t.Error("expected notification within 2s")
	}
}

// TestNotificationNonBlocking tests that Publish is non-blocking.
func TestNotificationNonBlocking(t *testing.T) {
	notifier := router.NewNotifier(10) // Small buffer

	// Subscribe with a slow consumer (channel that blocks)
	slowSub := notifier.Subscribe("slow", []string{""})

	// Fill up the channel
	for i := 0; i < 10; i++ {
		notifier.Publish(router.Notification{
			Type:         router.PartitionCreated,
			PartitionKey: "test",
			LSN:          uint64(i),
		})
	}

	// This should not block even though channel is full
	notifier.Publish(router.Notification{
		Type:         router.PartitionCreated,
		PartitionKey: "test",
		LSN:          999,
	})

	// Read one to make room
	<-slowSub.Ch

	// Another publish should work
	notifier.Publish(router.Notification{
		Type:         router.PartitionCreated,
		PartitionKey: "test",
		LSN:          1000,
	})

	t.Logf("Non-blocking test passed")
}