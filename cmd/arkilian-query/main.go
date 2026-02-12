// Package main implements the arkilian-query service binary.
// This service handles SQL query execution across partitions.
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

	httpapi "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/server"
	"github.com/arkilian/arkilian/internal/storage"
)

// Config holds the service configuration.
type Config struct {
	HTTPAddr     string
	ManifestPath string
	StoragePath  string
	DownloadDir  string
	Concurrency  int
	PoolSize     int
}

func main() {
	cfg := parseFlags()

	log.Printf("Starting arkilian-query service...")
	log.Printf("HTTP address: %s", cfg.HTTPAddr)

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

	// Initialize pruner with storage for bloom filter loading
	pruner := planner.NewPruner(catalog, store)

	// Initialize query planner
	queryPlanner := planner.NewPlannerWithPruner(catalog, pruner)

	// Initialize query executor
	execConfig := executor.ExecutorConfig{
		Concurrency: cfg.Concurrency,
		DownloadDir: cfg.DownloadDir,
		PoolConfig: executor.PoolConfig{
			MaxConnections:      10,
			MaxTotalConnections: cfg.PoolSize,
			IdleTimeout:         5 * time.Minute,
		},
	}
	queryExecutor, err := executor.NewParallelExecutor(queryPlanner, store, execConfig)
	if err != nil {
		log.Fatalf("Failed to initialize query executor: %v", err)
	}
	defer queryExecutor.Close()
	log.Printf("Query executor initialized with concurrency=%d, pool_size=%d", cfg.Concurrency, cfg.PoolSize)

	// Preload bloom filters for hot partitions on startup
	log.Printf("Preloading bloom filters for hot partitions...")
	if err := preloadBloomFilters(context.Background(), catalog, pruner); err != nil {
		log.Printf("Warning: Failed to preload bloom filters: %v", err)
	} else {
		stats := pruner.GetCacheStats()
		log.Printf("Bloom filter cache: %d filters loaded, %d bytes", stats.LRUFilters, stats.LRUMemoryBytes)
	}

	// Initialize shutdown manager
	shutdownConfig := server.DefaultShutdownConfig()
	shutdownMgr := server.NewShutdownManager(shutdownConfig)

	// Create HTTP handler
	queryHandler := httpapi.NewQueryHandler(queryExecutor)

	// Setup HTTP server with middleware
	mux := http.NewServeMux()
	middleware := httpapi.ChainMiddleware(
		server.ShutdownMiddleware(shutdownMgr),
		httpapi.RecoveryMiddleware,
		httpapi.RequestIDMiddleware,
		httpapi.CorrelationIDMiddleware,
		httpapi.ContentTypeMiddleware,
	)
	mux.Handle("/v1/query", middleware(queryHandler))
	mux.HandleFunc("/health", healthHandler)

	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start HTTP server
	go func() {
		log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	log.Printf("Received signal: %v, initiating graceful shutdown...", sig)

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := shutdownMgr.Shutdown(ctx, fmt.Sprintf("received signal: %v", sig)); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	log.Printf("arkilian-query service stopped")
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.HTTPAddr, "http-addr", ":8081", "HTTP server address")
	flag.StringVar(&cfg.ManifestPath, "manifest", "./data/manifest.db", "Path to manifest database")
	flag.StringVar(&cfg.StoragePath, "storage", "./data/storage", "Path to object storage directory")
	flag.StringVar(&cfg.DownloadDir, "download-dir", "./data/downloads", "Path to partition download directory")
	flag.IntVar(&cfg.Concurrency, "concurrency", 10, "Number of parallel partition queries")
	flag.IntVar(&cfg.PoolSize, "pool-size", 100, "Maximum number of SQLite connections")

	flag.Parse()

	// Create directories if they don't exist
	for _, dir := range []string{cfg.StoragePath, cfg.DownloadDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	return cfg
}

// preloadBloomFilters loads bloom filters for recent partitions into memory.
func preloadBloomFilters(ctx context.Context, catalog *manifest.SQLiteCatalog, pruner *planner.Pruner) error {
	// Get all active partitions
	partitions, err := catalog.FindPartitions(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get partitions: %w", err)
	}

	// Limit preloading to most recent partitions (e.g., last 1000)
	maxPreload := 1000
	if len(partitions) > maxPreload {
		partitions = partitions[len(partitions)-maxPreload:]
	}

	// Preload bloom filters for tenant_id and user_id columns
	columns := planner.GetBloomFilterColumns()
	return pruner.PreloadBloomFilters(ctx, partitions, columns)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","service":"arkilian-query"}`))
}
