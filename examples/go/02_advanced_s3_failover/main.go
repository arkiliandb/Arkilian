package main

import (
	"fmt"
	"os"
	"time"

	"github.com/arkiliandb/Arkilian/bindings/go/arkilian"
)

const (
	primaryDB  = "primary_production.sqlite"
	restoredDB = "hydrated_recovery.sqlite"
)

func cleanupDB(path string) {
	for _, ext := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + ext)
	}
}

func getEnvDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	fmt.Println("=== Arkilian Go SDK: Advanced S3 Streaming & Cold-Start Hydration ===")
	fmt.Println()

	endpoint := getEnvDefault("ARKILIAN_S3_ENDPOINT", "http://127.0.0.1:9000")
	bucket := getEnvDefault("ARKILIAN_S3_BUCKET", "arkilian-test-bucket")
	region := getEnvDefault("ARKILIAN_S3_REGION", "us-east-1")
	accessKey := getEnvDefault("ARKILIAN_S3_ACCESS_KEY", "minioadmin")
	secretKey := getEnvDefault("ARKILIAN_S3_SECRET_KEY", "minioadmin")
	prefix := getEnvDefault("ARKILIAN_S3_PREFIX", "go-demo")
	hmacKey := getEnvDefault("ARKILIAN_MANIFEST_HMAC_KEY", "super-secret-hmac-key-for-manifests")

	// Configure environment variables for the Arkilian C sidecar worker
	os.Setenv("ARKILIAN_ENABLE_BACKUP", "1")
	os.Setenv("ARKILIAN_S3_ENDPOINT", endpoint)
	os.Setenv("ARKILIAN_S3_BUCKET", bucket)
	os.Setenv("ARKILIAN_S3_REGION", region)
	os.Setenv("ARKILIAN_S3_ACCESS_KEY", accessKey)
	os.Setenv("ARKILIAN_S3_SECRET_KEY", secretKey)
	os.Setenv("ARKILIAN_S3_PREFIX", prefix)
	os.Setenv("ARKILIAN_MANIFEST_HMAC_KEY", hmacKey)
	os.Setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1")
	os.Setenv("ARKILIAN_MANIFEST_INTERVAL_SEC", "1")

	fmt.Printf("[Config] Target S3 Endpoint: %s\n", endpoint)
	fmt.Printf("[Config] Bucket: %s | Prefix: %s\n\n", bucket, prefix)

	cleanupDB(primaryDB)
	cleanupDB(restoredDB)

	// -------------------------------------------------------------
	// Phase 1: Primary Database Workload with Real-Time S3 Streaming
	// -------------------------------------------------------------
	fmt.Println("[Phase 1] Opening Primary Node with S3 WAL Streaming...")
	db, err := arkilian.OpenDB(primaryDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open primary database: %v\n", err)
		os.Exit(1)
	}

	// Verify sidecar telemetry
	fmt.Printf("  Telemetry -> Sidecar Healthy: %v\n", db.BackupIsHealthy())
	fmt.Printf("  Telemetry -> Health Flags: 0x%x\n", db.BackupHealthFlags())
	fmt.Printf("  Telemetry -> Queue Depth: %d\n", db.BackupQueueDepth())

	fmt.Println("\n[Phase 1] Creating schema and streaming server telemetry log...")
	schemaSQL := `
		CREATE TABLE IF NOT EXISTS server_metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			host_id TEXT NOT NULL,
			cpu_percent REAL NOT NULL,
			memory_mb INTEGER NOT NULL,
			status TEXT NOT NULL,
			recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`
	if err := db.Exec(schemaSQL); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create schema: %v\n", err)
		os.Exit(1)
	}

	totalRecords := 25
	insertSQL := "INSERT INTO server_metrics (host_id, cpu_percent, memory_mb, status) VALUES (?, ?, ?, ?)"
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to prepare statement: %v\n", err)
		os.Exit(1)
	}

	if err := db.Begin(); err != nil {
		fmt.Fprintf(os.Stderr, "Begin transaction failed: %v\n", err)
		os.Exit(1)
	}

	for i := 1; i <= totalRecords; i++ {
		_ = stmt.Reset()
		_ = stmt.BindText(1, fmt.Sprintf("node-%02d", i%4))
		_ = stmt.BindDouble(2, 15.0+float64(i)*2.3)
		_ = stmt.BindInt(3, 4096+i*128)
		status := "HEALTHY"
		if i%5 == 0 {
			status = "WARNING"
		}
		_ = stmt.BindText(4, status)
		if _, err := stmt.Step(); err != nil {
			fmt.Fprintf(os.Stderr, "Insert error at row %d: %v\n", i, err)
			os.Exit(1)
		}
	}
	_ = stmt.Finalize()

	if err := db.Commit(); err != nil {
		fmt.Fprintf(os.Stderr, "Commit failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  ✓ Successfully committed %d metric records.\n", totalRecords)

	// Flush WAL frame chunk to outbox & allow sidecar worker to ship to S3
	fmt.Println("\n[Phase 1] Triggering explicit WAL flush to S3 sidecar...")
	db.FlushWAL()

	fmt.Println("  Waiting for S3 sidecar outbox to drain...")
	for attempt := 0; attempt < 20; attempt++ {
		time.Sleep(500 * time.Millisecond)
		if db.BackupQueueDepth() == 0 {
			break
		}
	}
	// Allow manifest upload to finalize
	time.Sleep(1500 * time.Millisecond)

	fmt.Printf("  Telemetry -> Queue Depth: %d\n", db.BackupQueueDepth())
	fmt.Printf("  Telemetry -> Sidecar Healthy: %v\n", db.BackupIsHealthy())

	// Check record count on primary
	qStmt, _ := db.Prepare("SELECT COUNT(*) FROM server_metrics")
	_, _ = qStmt.Step()
	primaryCount := qStmt.ColumnInt64(0)
	_ = qStmt.Finalize()
	fmt.Printf("  Primary DB confirmed record count: %d\n", primaryCount)

	fmt.Println("[Phase 1] Closing primary database connection.")
	_ = db.Close()

	// -------------------------------------------------------------
	// Phase 2: Disaster Simulation
	// -------------------------------------------------------------
	fmt.Println("\n[Phase 2] SIMULATING DISASTER: Primary host destroyed!")
	fmt.Printf("  Purging local files: %s...\n", primaryDB)
	cleanupDB(primaryDB)
	fmt.Println("  ✓ Primary local database destroyed. Zero local state remains.")

	// -------------------------------------------------------------
	// Phase 3: Cold-Start Hydration from MinIO S3
	// -------------------------------------------------------------
	fmt.Println("\n[Phase 3] Starting Cold-Start Hydration from MinIO S3...")
	fmt.Printf("  Target path: %s\n", restoredDB)

	hydrateCfg := arkilian.S3Config{
		Endpoint:  endpoint,
		Bucket:    bucket,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Prefix:    prefix,
	}

	if err := arkilian.HydrateS3(restoredDB, hydrateCfg); err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ Hydration failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("  ✓ Hydration completed successfully!")

	// -------------------------------------------------------------
	// Phase 4: Data Parity & Integrity Verification
	// -------------------------------------------------------------
	fmt.Println("\n[Phase 4] Opening Hydrated Database to verify integrity...")
	os.Setenv("ARKILIAN_ENABLE_BACKUP", "0")
	recoveredDB, err := arkilian.OpenDB(restoredDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open recovered database: %v\n", err)
		os.Exit(1)
	}
	defer recoveredDB.Close()

	rCountStmt, _ := recoveredDB.Prepare("SELECT COUNT(*) FROM server_metrics")
	_, _ = rCountStmt.Step()
	recoveredCount := rCountStmt.ColumnInt64(0)
	_ = rCountStmt.Finalize()
	fmt.Printf("  Recovered DB record count: %d\n", recoveredCount)

	if recoveredCount != primaryCount {
		fmt.Fprintf(os.Stderr, "  ✗ Data mismatch! Expected %d, got %d\n", primaryCount, recoveredCount)
		os.Exit(1)
	}

	fmt.Println("\n  Sample records from recovered instance:")
	sStmt, _ := recoveredDB.Prepare("SELECT id, host_id, cpu_percent, memory_mb, status FROM server_metrics LIMIT 5")
	for {
		hasRow, _ := sStmt.Step()
		if !hasRow {
			break
		}
		fmt.Printf("    - ID %d | Host: %s | CPU: %.1f%% | RAM: %d MB | Status: %s\n",
			sStmt.ColumnInt64(0), sStmt.ColumnText(1), sStmt.ColumnDouble(2), sStmt.ColumnInt(3), sStmt.ColumnText(4))
	}
	_ = sStmt.Finalize()

	cleanupDB(restoredDB)
	fmt.Println("\n=== Disaster Recovery & Verification Finished Successfully! ===")
}
