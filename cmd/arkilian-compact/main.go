// Package main implements the arkilian-compact service binary.
// This service handles background compaction of small partitions and garbage collection.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arkilian/arkilian/internal/compaction"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/server"
	"github.com/arkilian/arkilian/internal/storage"
)

// Config holds the service configuration.
type Config struct {
	HTTPAddr            string
	ManifestPath        string
	StoragePath         string
	WorkDir             string
	CheckInterval       time.Duration
	MinPartitionSize    int64
	MaxPartitionsPerKey int64
	TTLDays             int
}

func main() {
	cfg := parseFlags()

	log.Printf("Starting arkilian-compact service...")
	log.Printf("HTTP address: %s", cfg.HTTPAddr)
	log.Printf("Check interval: %v", cfg.CheckInterval)

	// Initialize storage
	store, err := storage.NewLocalStorage(cfg.StoragePath)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	log.Printf("Storage initialized at: %s", cfg.StoragePath)

	// Initialize manifest catalog
	catalog, err := manifest.NewCatalog(cfg.ManifestPath)
	if err != nil {
		log.Fatalf("Failed to initialize manifest catalog: %v", err)
	}
	defer catalog.Close()
	log.Printf("Manifest catalog initialized at: %s", cfg.ManifestPath)

	// Initialize compaction daemon
	compactConfig := compaction.CompactionConfig{
		MinPartitionSize:    cfg.MinPartitionSize,
		MaxPartitionsPerKey: cfg.MaxPartitionsPerKey,
		TTLDays:             cfg.TTLDays,
		CheckInterval:       cfg.CheckInterval,
		WorkDir:             cfg.WorkDir,
	}
	daemon := compaction.NewDaemon(compactConfig, catalog, store)
	log.Printf("Compaction daemon initialized with min_size=%dMB, max_partitions=%d, ttl=%d days",
		cfg.MinPartitionSize/(1024*1024), cfg.MaxPartitionsPerKey, cfg.TTLDays)

	// Initialize shutdown manager
	shutdownConfig := server.DefaultShutdownConfig()
	shutdownMgr := server.NewShutdownManager(shutdownConfig)

	// Setup HTTP server for health checks and manual triggers
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/trigger", triggerHandler(daemon))

	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start HTTP server
	go func() {
		log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Start compaction daemon
	ctx, cancel := context.WithCancel(context.Background())
	if err := daemon.Start(ctx); err != nil {
		log.Fatalf("Failed to start compaction daemon: %v", err)
	}
	log.Printf("Compaction daemon started")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	log.Printf("Received signal: %v, initiating graceful shutdown...", sig)

	// Stop compaction daemon
	cancel()
	if err := daemon.Stop(); err != nil {
		log.Printf("Daemon stop error: %v", err)
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := shutdownMgr.Shutdown(shutdownCtx, fmt.Sprintf("received signal: %v", sig)); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	log.Printf("arkilian-compact service stopped")
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.HTTPAddr, "http-addr", ":8082", "HTTP server address")
	flag.StringVar(&cfg.ManifestPath, "manifest", "./data/manifest.db", "Path to manifest database")
	flag.StringVar(&cfg.StoragePath, "storage", "./data/storage", "Path to object storage directory")
	flag.StringVar(&cfg.WorkDir, "work-dir", "./data/compaction", "Path to compaction work directory")
	flag.DurationVar(&cfg.CheckInterval, "check-interval", 5*time.Minute, "Interval between compaction checks")
	flag.Int64Var(&cfg.MinPartitionSize, "min-partition-size", 8*1024*1024, "Minimum partition size before compaction (bytes)")
	flag.Int64Var(&cfg.MaxPartitionsPerKey, "max-partitions-per-key", 100, "Maximum partitions per key before compaction")
	flag.IntVar(&cfg.TTLDays, "ttl-days", 7, "Days before compacted partitions are garbage collected")

	flag.Parse()

	// Create directories if they don't exist
	for _, dir := range []string{cfg.StoragePath, cfg.WorkDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	return cfg
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","service":"arkilian-compact"}`))
}

// triggerHandler creates a handler for manually triggering compaction.
func triggerHandler(daemon *compaction.Daemon) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		partitionKey := r.URL.Query().Get("partition_key")
		if partitionKey == "" {
			// Trigger a full compaction cycle
			log.Printf("Manual compaction triggered (full cycle)")
			go daemon.RunOnce(context.Background())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(`{"status":"accepted","message":"Full compaction cycle triggered"}`))
			return
		}

		// Trigger compaction for specific partition key
		log.Printf("Manual compaction triggered for partition_key=%s", partitionKey)
		go func() {
			if err := daemon.TriggerCompaction(context.Background(), partitionKey); err != nil {
				log.Printf("Manual compaction failed for %s: %v", partitionKey, err)
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(fmt.Sprintf(`{"status":"accepted","message":"Compaction triggered for partition_key=%s"}`, partitionKey)))
	}
}
