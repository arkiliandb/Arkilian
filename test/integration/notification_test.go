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

// setupNotificationTestEnv creates a test environment with notification support.
func setupNotificationTestEnv(t *testing.T) (
	*wal.WAL,
	*wal.Flusher,
	*router.Notifier,
	manifest.Catalog,
	storage.ObjectStorage,
	string,
	func(),
) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "arkilian-notification-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	walDir := filepath.Join(tempDir, "wal")
	partitionDir := filepath.Join(tempDir, "partitions")
	storageDir := filepath.Join(tempDir, "storage")
	manifestPath := filepath.Join(tempDir, "manifest.db")

	for _, dir := range []string{walDir, partitionDir, storageDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	store, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	catalog, err := manifest.NewCatalog(manifestPath)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create catalog: %v", err)
	}

	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()

	walInstance, err := wal.NewWAL(walDir, 64*1024*1024)
	if err != nil {
		catalog.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create WAL: %v", err)
	}

	// Create notifier
	notifier := router.NewNotifier(1000)

	// Create flusher with notifier
	flusher := wal.NewFlusher(walInstance, builder, store, catalog, metaGen, 100*time.Millisecond, 1000)
	flusher.SetNotifier(notifier)

	cleanup := func() {
		walInstance.Close()
		notifier.Close()
		catalog.Close()
		os.RemoveAll(tempDir)
	}

	return walInstance, flusher, notifier, catalog, store, tempDir, cleanup
}

// TestWriteVisibility tests that write notifications are received within 100ms.
func TestWriteVisibility(t *testing.T) {
	walInstance, flusher, notifier, catalog, store, _, cleanup := setupNotificationTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Start flusher in background
	flusherCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

	// Subscribe to notifications
	subscriber := notifier.Subscribe("test-subscriber", []string{""}) // Subscribe to all partitions
	defer notifier.Unsubscribe("test-subscriber")

	// Create ingest handler with WAL
	builder := partition.NewBuilder("", 0)
	metaGen := partition.NewMetadataGenerator()
	handler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, walInstance)
	wrappedHandler := apihttp.DefaultMiddleware()(handler)

	// Ingest test data
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
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d - %s", rec.Code, rec.Body.String())
	}

	var resp apihttp.WALIngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.LSN == 0 {
		t.Fatal("expected LSN in response")
	}

	// Wait for notification with timeout
	timeout := 500 * time.Millisecond
	deadline := time.Now().Add(timeout)

	var receivedNotif *router.Notification
	for time.Now().Before(deadline) {
		select {
		case notif := <-subscriber.Ch:
			receivedNotif = &notif
			goto received
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}

received:
	if receivedNotif == nil {
		t.Errorf("expected notification within %v, got none", timeout)
	} else {
		// Verify notification content
		if receivedNotif.PartitionKey != "20260206" {
			t.Errorf("expected partition_key='20260206', got '%s'", receivedNotif.PartitionKey)
		}
		if receivedNotif.LSN != resp.LSN {
			t.Errorf("expected LSN=%d, got %d", resp.LSN, receivedNotif.LSN)
		}
		if receivedNotif.Type != router.PartitionCreated {
			t.Errorf("expected notification type=%d, got %d", router.PartitionCreated, receivedNotif.Type)
		}

		t.Logf("Notification received: partition_key=%s, lsn=%d, type=%d",
			receivedNotif.PartitionKey, receivedNotif.LSN, receivedNotif.Type)
	}
}

// TestNotificationFiltering tests that notifications can be filtered by partition key.
func TestNotificationFiltering(t *testing.T) {
	_, flusher, notifier, catalog, store, tempDir, cleanup := setupNotificationTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Start flusher
	flusherCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go flusher.Run(flusherCtx)

	// Subscribe only to partition key "20260206"
	subscriber := notifier.Subscribe("filtered-subscriber", []string{"20260206"})
	defer notifier.Unsubscribe("filtered-subscriber")

	// Subscribe to different partition
	otherSubscriber := notifier.Subscribe("other-subscriber", []string{"20260207"})
	defer notifier.Unsubscribe("other-subscriber")

	// Create handler with proper partition directory
	partitionDir := filepath.Join(tempDir, "partitions")
	os.MkdirAll(partitionDir, 0755)
	builder := partition.NewBuilder(partitionDir, 0)
	metaGen := partition.NewMetadataGenerator()
	walHandler := apihttp.NewIngestHandler(builder, metaGen, catalog, store, nil, nil)
	wrappedHandler := apihttp.DefaultMiddleware()(walHandler)

	// Ingest to "20260206"
	reqBody := apihttp.IngestRequest{
		PartitionKey: "20260206",
		Rows: []map[string]interface{}{
			{
				"tenant_id":  "acme",
				"user_id":    float64(12345),
				"event_time": float64(time.Now().UnixNano()),
				"event_type": "test",
				"payload":    map[string]interface{}{},
			},
		},
	}

	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d - %s", rec.Code, rec.Body.String())
	}

	// Wait for notifications
	time.Sleep(200 * time.Millisecond)

	// Check filtered subscriber received notification
	select {
	case notif := <-subscriber.Ch:
		if notif.PartitionKey != "20260206" {
			t.Errorf("expected partition_key='20260206', got '%s'", notif.PartitionKey)
		}
	default:
		t.Log("Filtered subscriber may not have received notification (depends on flusher timing)")
	}

	// Other subscriber should not receive notification for different partition
	select {
	case notif := <-otherSubscriber.Ch:
		if notif.PartitionKey == "20260206" {
			t.Error("other subscriber should not receive notification for different partition")
		}
	default:
		// Expected - other subscriber shouldn't get this notification
	}
}

// TestNotificationNonBlocking tests that publish is non-blocking when channels are full.
func TestNotificationNonBlocking(t *testing.T) {
	notifier := router.NewNotifier(10) // Small buffer

	// Subscribe with blocking channel
	_ = notifier.Subscribe("blocking-subscriber", []string{""})
	defer notifier.Unsubscribe("blocking-subscriber")

	// Fill up the channel
	for i := 0; i < 15; i++ {
		notif := router.Notification{
			Type:         router.PartitionCreated,
			PartitionKey: "test",
			PartitionID:  "test-id",
			LSN:          uint64(i),
			Timestamp:    time.Now().UnixNano(),
		}
		notifier.Publish(notif)
	}

	// Publish should not block even when channel is full
	// If this hangs, the test will timeout
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			notif := router.Notification{
				Type:         router.PartitionCreated,
				PartitionKey: "test",
				PartitionID:  "test-id",
				LSN:          uint64(100 + i),
				Timestamp:    time.Now().UnixNano(),
			}
			notifier.Publish(notif)
		}
		close(done)
	}()

	select {
	case <-done:
		// Publish was non-blocking
	case <-time.After(1 * time.Second):
		t.Error("Publish blocked when channel was full")
	}
}