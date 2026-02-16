// Package app provides the unified application lifecycle management for Arkilian.
package app

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/arkilian/arkilian/api/proto"
	grpcapi "github.com/arkilian/arkilian/internal/api/grpc"
	httpapi "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/compaction"
	"github.com/arkilian/arkilian/internal/config"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/server"
	"github.com/arkilian/arkilian/internal/storage"
	"google.golang.org/grpc"
)

// App manages all Arkilian service lifecycles.
type App struct {
	cfg *config.Config

	// Shared resources
	storage       storage.ObjectStorage
	catalog       manifest.Catalog        // Used by ingest, compaction, GC (full read/write)
	catalogReader manifest.CatalogReader   // Used by planner/pruner (read-only)
	shutdown      *server.ShutdownManager

	// Service components
	ingestServer  *http.Server
	queryServer   *http.Server
	compactServer *http.Server
	grpcServer    *grpc.Server
	grpcListener  net.Listener
	compactDaemon *compaction.Daemon

	// Query executor (needs explicit close)
	queryExecutor *executor.ParallelExecutor

	// Lifecycle
	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// New creates a new App with the given configuration.
func New(cfg *config.Config) (*App, error) {
	// Resolve paths and validate
	cfg.Resolve()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Ensure directories exist
	if err := cfg.EnsureDirectories(); err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}

	return &App{
		cfg: cfg,
	}, nil
}

// Start initializes shared resources and starts all configured services.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return fmt.Errorf("app is already running")
	}
	a.running = true
	a.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	// Initialize shared resources
	if err := a.initSharedResources(); err != nil {
		a.cleanup()
		return fmt.Errorf("failed to initialize shared resources: %w", err)
	}

	// Start services based on mode
	if a.cfg.ShouldRunIngest() {
		if err := a.startIngestService(ctx); err != nil {
			a.cleanup()
			return fmt.Errorf("failed to start ingest service: %w", err)
		}
	}

	if a.cfg.ShouldRunQuery() {
		if err := a.startQueryService(ctx); err != nil {
			a.cleanup()
			return fmt.Errorf("failed to start query service: %w", err)
		}
	}

	if a.cfg.ShouldRunCompact() {
		if err := a.startCompactService(ctx); err != nil {
			a.cleanup()
			return fmt.Errorf("failed to start compaction service: %w", err)
		}
	}

	log.Printf("Arkilian started in %s mode", a.cfg.Mode)
	return nil
}

// initSharedResources initializes storage, manifest catalog, and shutdown manager.
func (a *App) initSharedResources() error {
	var err error

	// Initialize storage
	switch a.cfg.Storage.Type {
	case "local":
		a.storage, err = storage.NewLocalStorage(a.cfg.Storage.Path)
	case "s3":
		s3Cfg := storage.DefaultS3Config()
		if a.cfg.Storage.S3.Region != "" {
			s3Cfg.Region = a.cfg.Storage.S3.Region
		}
		if a.cfg.Storage.S3.Endpoint != "" {
			s3Cfg.Endpoint = a.cfg.Storage.S3.Endpoint
		}
		a.storage, err = storage.NewS3Storage(
			context.Background(),
			a.cfg.Storage.S3.Bucket,
			s3Cfg,
		)
	default:
		return fmt.Errorf("unsupported storage type: %s", a.cfg.Storage.Type)
	}
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	log.Printf("Storage initialized: type=%s", a.cfg.Storage.Type)
	if a.cfg.Storage.Type == "s3" {
		log.Printf("S3 Config: Bucket=%s, Region=%s, Endpoint=%s", 
			a.cfg.Storage.S3.Bucket, a.cfg.Storage.S3.Region, a.cfg.Storage.S3.Endpoint)
	}

	// Initialize manifest catalog
	if a.cfg.Manifest.Sharded {
		sharded, err := manifest.NewShardedCatalog(a.cfg.ManifestDir(), a.cfg.Manifest.ShardCount)
		if err != nil {
			return fmt.Errorf("failed to initialize sharded manifest catalog: %w", err)
		}
		a.catalog = sharded
		a.catalogReader = sharded
		log.Printf("Sharded manifest catalog initialized: %d shards in %s", a.cfg.Manifest.ShardCount, a.cfg.ManifestDir())
	} else {
		single, err := manifest.NewCatalog(a.cfg.ManifestPath())
		if err != nil {
			return fmt.Errorf("failed to initialize manifest catalog: %w", err)
		}

		// Auto-migrate to sharded mode if partition count exceeds threshold.
		// This prevents the operational footgun where queries silently degrade
		// from 4ms to 245ms+ at 100K partitions without operator intervention.
		if a.cfg.Manifest.AutoShardThreshold > 0 {
			sharded, err := manifest.MigrateToSharded(
				single,
				a.cfg.ManifestDir(),
				a.cfg.Manifest.ShardCount,
				a.cfg.Manifest.AutoShardThreshold,
			)
			if err != nil {
				single.Close()
				return fmt.Errorf("failed to auto-migrate manifest to sharded mode: %w", err)
			}
			if sharded != nil {
				a.catalog = sharded
				a.catalogReader = sharded
				log.Printf("Auto-migrated manifest catalog to sharded mode: %d shards", a.cfg.Manifest.ShardCount)
			} else {
				a.catalog = single
				a.catalogReader = single
				log.Printf("Manifest catalog initialized: %s", a.cfg.ManifestPath())
			}
		} else {
			a.catalog = single
			a.catalogReader = single
			log.Printf("Manifest catalog initialized: %s", a.cfg.ManifestPath())
		}
	}

	// Initialize shutdown manager
	shutdownConfig := server.DefaultShutdownConfig()
	a.shutdown = server.NewShutdownManager(shutdownConfig)

	return nil
}

// startIngestService starts the ingest HTTP and gRPC servers.
func (a *App) startIngestService(ctx context.Context) error {
	// Initialize partition builder with static target size as baseline
	builder := partition.NewBuilder(a.cfg.Ingest.PartitionDir, a.cfg.Ingest.TargetPartitionSizeMB)
	metaGen := partition.NewMetadataGenerator()

	// Initialize adaptive sizer for dynamic partition sizing at scale
	var sizerOpts []partition.AdaptiveSizerOption
	asCfg := a.cfg.Ingest.AdaptiveSizing
	if asCfg.Enabled {
		sizerOpts = append(sizerOpts, partition.WithBoundsMB(asCfg.MinSizeMB, asCfg.MaxSizeMB))
		for _, tier := range asCfg.Tiers {
			sizerOpts = append(sizerOpts, partition.WithTierMB(tier.ThresholdGB, tier.TargetSizeMB))
		}
	}
	adaptiveSizer := partition.NewAdaptiveSizer(
		asCfg.Enabled,
		a.cfg.Ingest.TargetPartitionSizeMB,
		&catalogVolumeQuerier{reader: a.catalogReader},
		sizerOpts...,
	)

	if asCfg.Enabled {
		log.Printf("Adaptive partition sizing enabled: min=%dMB, max=%dMB, %d tiers",
			asCfg.MinSizeMB, asCfg.MaxSizeMB, len(asCfg.Tiers))
	}
	log.Printf("Partition builder initialized: %s", a.cfg.Ingest.PartitionDir)

	// Create HTTP handler
	ingestHandler := httpapi.NewIngestHandler(builder, metaGen, a.catalog, a.storage, adaptiveSizer)

	// Setup HTTP server with middleware
	mux := http.NewServeMux()
	middleware := httpapi.ChainMiddleware(
		server.ShutdownMiddleware(a.shutdown),
		httpapi.RecoveryMiddleware,
		httpapi.RequestIDMiddleware,
		httpapi.CorrelationIDMiddleware,
		httpapi.ContentTypeMiddleware,
	)
	mux.Handle("/v1/ingest", middleware(ingestHandler))
	mux.HandleFunc("/health", a.healthHandler("arkilian-ingest"))

	a.ingestServer = &http.Server{
		Addr:         a.cfg.HTTP.IngestAddr,
		Handler:      mux,
		ReadTimeout:  a.cfg.HTTP.ReadTimeout,
		WriteTimeout: a.cfg.HTTP.WriteTimeout,
		IdleTimeout:  a.cfg.HTTP.IdleTimeout,
	}

	// Start HTTP server
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		log.Printf("Ingest HTTP server listening on %s", a.cfg.HTTP.IngestAddr)
		if err := a.ingestServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Ingest HTTP server error: %v", err)
		}
	}()

	// Start gRPC server if enabled
	if a.cfg.GRPC.Enabled {
		a.grpcServer = grpc.NewServer()
		ingestServer := grpcapi.NewIngestServer(builder, metaGen, a.catalog, a.storage)
		proto.RegisterIngestServiceServer(a.grpcServer, ingestServer)

		var err error
		a.grpcListener, err = net.Listen("tcp", a.cfg.GRPC.Addr)
		if err != nil {
			return fmt.Errorf("failed to listen on gRPC address: %w", err)
		}

		a.shutdown.RegisterCloser(server.CloserFunc(func() error {
			a.grpcServer.GracefulStop()
			return nil
		}))

		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			log.Printf("gRPC server listening on %s", a.cfg.GRPC.Addr)
			if err := a.grpcServer.Serve(a.grpcListener); err != nil {
				log.Printf("gRPC server error: %v", err)
			}
		}()
	}

	return nil
}

// startQueryService starts the query HTTP server.
func (a *App) startQueryService(ctx context.Context) error {
	// Initialize pruner with storage for bloom filter loading
	bloomCacheBytes := int64(a.cfg.Query.BloomCacheSizeMB) * 1024 * 1024
	if bloomCacheBytes <= 0 {
		bloomCacheBytes = 1 << 30 // 1GB default
	}
	pruner := planner.NewPrunerWithCacheSize(a.catalogReader, a.storage, bloomCacheBytes)

	// Initialize query planner
	queryPlanner := planner.NewPlannerWithPruner(a.catalogReader, pruner)

	// Initialize query executor
	execConfig := executor.ExecutorConfig{
		Concurrency: a.cfg.Query.Concurrency,
		DownloadDir: a.cfg.Query.DownloadDir,
		PoolConfig: executor.PoolConfig{
			MaxConnections:      10,
			MaxTotalConnections: a.cfg.Query.PoolSize,
			IdleTimeout:         5 * time.Minute,
		},
	}
	var err error
	a.queryExecutor, err = executor.NewParallelExecutor(queryPlanner, a.storage, execConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize query executor: %w", err)
	}
	log.Printf("Query executor initialized: concurrency=%d, pool_size=%d",
		a.cfg.Query.Concurrency, a.cfg.Query.PoolSize)

	// Preload bloom filters for hot partitions
	log.Printf("Preloading bloom filters...")
	if err := a.preloadBloomFilters(ctx, pruner); err != nil {
		log.Printf("Warning: Failed to preload bloom filters: %v", err)
	} else {
		stats := pruner.GetCacheStats()
		log.Printf("Bloom filter cache: %d filters loaded, %d bytes",
			stats.LRUFilters, stats.LRUMemoryBytes)
	}

	// Create HTTP handler
	queryHandler := httpapi.NewQueryHandler(a.queryExecutor)

	// Setup HTTP server with middleware
	mux := http.NewServeMux()
	middleware := httpapi.ChainMiddleware(
		server.ShutdownMiddleware(a.shutdown),
		httpapi.RecoveryMiddleware,
		httpapi.RequestIDMiddleware,
		httpapi.CorrelationIDMiddleware,
		httpapi.ContentTypeMiddleware,
	)
	mux.Handle("/v1/query", middleware(queryHandler))
	mux.HandleFunc("/health", a.healthHandler("arkilian-query"))

	a.queryServer = &http.Server{
		Addr:         a.cfg.HTTP.QueryAddr,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start HTTP server
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		log.Printf("Query HTTP server listening on %s", a.cfg.HTTP.QueryAddr)
		if err := a.queryServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Query HTTP server error: %v", err)
		}
	}()

	return nil
}

// startCompactService starts the compaction daemon and HTTP server.
func (a *App) startCompactService(ctx context.Context) error {
	// Derive compaction min partition size from the ingest target partition size.
	// When adaptive sizing is enabled, the compaction daemon uses the sizer
	// to determine per-key thresholds instead of a single global value.
	minPartitionSize := a.cfg.Compaction.MinPartitionSize
	targetBytes := int64(a.cfg.Ingest.TargetPartitionSizeMB) * 1024 * 1024
	if targetBytes > minPartitionSize {
		minPartitionSize = targetBytes
	}

	// Build adaptive sizer for compaction (same config as ingest)
	var compactSizerOpts []partition.AdaptiveSizerOption
	if asCfg := a.cfg.Ingest.AdaptiveSizing; asCfg.Enabled {
		compactSizerOpts = append(compactSizerOpts, partition.WithBoundsMB(asCfg.MinSizeMB, asCfg.MaxSizeMB))
		for _, tier := range asCfg.Tiers {
			compactSizerOpts = append(compactSizerOpts, partition.WithTierMB(tier.ThresholdGB, tier.TargetSizeMB))
		}
	}
	compactAdaptiveSizer := partition.NewAdaptiveSizer(
		a.cfg.Ingest.AdaptiveSizing.Enabled,
		a.cfg.Ingest.TargetPartitionSizeMB,
		&catalogVolumeQuerier{reader: a.catalogReader},
		compactSizerOpts...,
	)

	// Initialize compaction daemon
	compactConfig := compaction.CompactionConfig{
		MinPartitionSize:    minPartitionSize,
		MaxPartitionsPerKey: a.cfg.Compaction.MaxPartitionsPerKey,
		TTLDays:             a.cfg.Compaction.TTLDays,
		CheckInterval:       a.cfg.Compaction.CheckInterval,
		WorkDir:             a.cfg.Compaction.WorkDir,
	}
	a.compactDaemon = compaction.NewDaemonWithBackpressure(compactConfig, a.catalog, a.storage, compactAdaptiveSizer,
		compaction.BackpressureConfig{
			MaxConcurrency:   a.cfg.Compaction.Backpressure.MaxConcurrency,
			MinConcurrency:   a.cfg.Compaction.Backpressure.MinConcurrency,
			FailureThreshold: a.cfg.Compaction.Backpressure.FailureThreshold,
			WindowDuration:   10 * time.Minute,
		})
	log.Printf("Compaction daemon initialized: min_size=%dMB, max_partitions=%d, ttl=%d days, max_concurrency=%d",
		minPartitionSize/(1024*1024),
		a.cfg.Compaction.MaxPartitionsPerKey,
		a.cfg.Compaction.TTLDays,
		a.cfg.Compaction.Backpressure.MaxConcurrency)

	// Setup HTTP server for health checks and manual triggers
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.healthHandler("arkilian-compact"))
	mux.HandleFunc("/trigger", a.triggerHandler())

	a.compactServer = &http.Server{
		Addr:         a.cfg.HTTP.CompactAddr,
		Handler:      mux,
		ReadTimeout:  a.cfg.HTTP.ReadTimeout,
		WriteTimeout: a.cfg.HTTP.WriteTimeout,
		IdleTimeout:  a.cfg.HTTP.IdleTimeout,
	}

	// Start HTTP server
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		log.Printf("Compaction HTTP server listening on %s", a.cfg.HTTP.CompactAddr)
		if err := a.compactServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Compaction HTTP server error: %v", err)
		}
	}()

	// Start compaction daemon
	if err := a.compactDaemon.Start(ctx); err != nil {
		return fmt.Errorf("failed to start compaction daemon: %w", err)
	}
	log.Printf("Compaction daemon started")

	return nil
}

// preloadBloomFilters loads bloom filters for recent partitions into memory.
func (a *App) preloadBloomFilters(ctx context.Context, pruner *planner.Pruner) error {
	partitions, err := a.catalog.FindPartitions(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get partitions: %w", err)
	}

	maxPreload := a.cfg.Query.MaxPreloadPartitions
	if len(partitions) > maxPreload {
		partitions = partitions[len(partitions)-maxPreload:]
	}

	columns := planner.GetBloomFilterColumns()
	return pruner.PreloadBloomFilters(ctx, partitions, columns)
}

// Stop gracefully stops all services and releases resources.
func (a *App) Stop(ctx context.Context) error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = false
	a.mu.Unlock()

	log.Printf("Initiating graceful shutdown...")

	// Cancel context to signal all services
	if a.cancel != nil {
		a.cancel()
	}

	// Stop compaction daemon first
	if a.compactDaemon != nil {
		if err := a.compactDaemon.Stop(); err != nil {
			log.Printf("Compaction daemon stop error: %v", err)
		}
	}

	// Shutdown HTTP servers
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if a.ingestServer != nil {
		if err := a.ingestServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Ingest server shutdown error: %v", err)
		}
	}

	if a.queryServer != nil {
		if err := a.queryServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Query server shutdown error: %v", err)
		}
	}

	if a.compactServer != nil {
		if err := a.compactServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Compact server shutdown error: %v", err)
		}
	}

	// Stop gRPC server
	if a.grpcServer != nil {
		a.grpcServer.GracefulStop()
	}

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines finished
	case <-shutdownCtx.Done():
		log.Printf("Shutdown timeout, some goroutines may not have finished")
	}

	// Cleanup resources
	a.cleanup()

	log.Printf("Arkilian stopped")
	return nil
}

// cleanup releases all shared resources.
func (a *App) cleanup() {
	if a.queryExecutor != nil {
		a.queryExecutor.Close()
	}

	if a.catalog != nil {
		a.catalog.Close()
	}
}

// healthHandler returns a health check handler for the given service.
func (a *App) healthHandler(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy","service":"%s","mode":"%s"}`, service, a.cfg.Mode)
	}
}

// triggerHandler returns a handler for manually triggering compaction.
func (a *App) triggerHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if a.compactDaemon == nil {
			http.Error(w, "Compaction daemon not running", http.StatusServiceUnavailable)
			return
		}

		partitionKey := r.URL.Query().Get("partition_key")
		if partitionKey == "" {
			log.Printf("Manual compaction triggered (full cycle)")
			go a.compactDaemon.RunOnce(context.Background())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(`{"status":"accepted","message":"Full compaction cycle triggered"}`))
			return
		}

		log.Printf("Manual compaction triggered for partition_key=%s", partitionKey)
		go func() {
			if err := a.compactDaemon.TriggerCompaction(context.Background(), partitionKey); err != nil {
				log.Printf("Manual compaction failed for %s: %v", partitionKey, err)
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status":"accepted","message":"Compaction triggered for partition_key=%s"}`, partitionKey)
	}
}

// WaitForShutdown blocks until a shutdown signal is received.
func (a *App) WaitForShutdown(ctx context.Context) error {
	return a.shutdown.ListenForSignals(ctx)
}

// catalogVolumeQuerier adapts manifest.CatalogReader to partition.VolumeQuerier.
type catalogVolumeQuerier struct {
	reader manifest.CatalogReader
}

func (q *catalogVolumeQuerier) TotalVolumeBytes(ctx context.Context, partitionKey string) (int64, error) {
	partitions, err := q.reader.FindPartitions(ctx, []manifest.Predicate{
		{Column: "partition_key", Operator: "=", Value: partitionKey},
	})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, p := range partitions {
		total += p.SizeBytes
	}
	return total, nil
}
