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
	"github.com/arkilian/arkilian/internal/cache"
	"github.com/arkilian/arkilian/internal/compaction"
	"github.com/arkilian/arkilian/internal/config"
	"github.com/arkilian/arkilian/internal/index"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/observability"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/router"
	"github.com/arkilian/arkilian/internal/schema"
	"github.com/arkilian/arkilian/internal/server"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
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
	walInstance   *wal.WAL                // WAL instance for lifecycle management
	flusher       *wal.Flusher            // Flusher for WAL background flush (needed for shutdown coordination)

	// Service components
	ingestServer  *http.Server
	queryServer   *http.Server
	compactServer *http.Server
	grpcServer    *grpc.Server
	grpcListener  net.Listener
	compactDaemon *compaction.Daemon

	// Query executor (needs explicit close)
	queryExecutor *executor.ParallelExecutor

	// Notification and caching components
	notifier      *router.Notifier
	nvmeCache     *cache.NVMeCache
	coAccessGraph *cache.CoAccessGraph

	// Materializer for JSON column materialization
	materializer *schema.Materializer

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
	log.Printf("Partition builder initialized: %s", a.cfg.Ingest.PartitionDir)

	// Initialize query statistics for materialized columns
	queryStats := observability.NewQueryStats(1 * time.Hour)
	a.materializer = schema.NewMaterializer(queryStats, 50, 20)
	log.Printf("Materializer initialized for JSON column materialization")

	// Initialize WAL
	log.Printf("WAL enabled: dir=%s, max_segment_size=%d, flush_interval=%v",
		a.cfg.WAL.Dir, a.cfg.WAL.MaxSegmentSize, a.cfg.WAL.FlushInterval)

	walInstance, err := wal.NewWAL(a.cfg.WAL.Dir, a.cfg.WAL.MaxSegmentSize)
	if err != nil {
		return fmt.Errorf("failed to initialize WAL: %w", err)
	}
	a.walInstance = walInstance
	log.Printf("WAL initialized: dir=%s", a.cfg.WAL.Dir)

	// Create notifier for write notifications
	notifier := router.NewNotifier(a.cfg.Router.BufferSize)
	a.notifier = notifier
	log.Printf("Write notifier enabled: buffer_size=%d", a.cfg.Router.BufferSize)

	// Create flusher
	a.flusher = wal.NewFlusher(a.walInstance, builder, a.storage, a.catalog, metaGen,
		a.cfg.WAL.FlushInterval, a.cfg.WAL.FlushBatchSize)
	a.flusher.SetNotifier(notifier)

	// Start flusher in background
	flusherCtx, flusherCancel := context.WithCancel(ctx)
	a.shutdown.RegisterCloser(server.CloserFunc(func() error {
		flusherCancel()
		return nil
	}))
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.flusher.Run(flusherCtx)
	}()
	log.Printf("WAL flusher started: interval=%v, batch_size=%d",
		a.cfg.WAL.FlushInterval, a.cfg.WAL.FlushBatchSize)

	// Run recovery to replay any unflushed entries from previous crash
	recovery := wal.NewRecovery(a.walInstance, a.flusher, a.catalog)
	recoveredCount, err := recovery.Recover(ctx)
	if err != nil {
		log.Printf("WAL recovery failed: %v", err)
	} else if recoveredCount > 0 {
		log.Printf("WAL recovery completed: %d entries replayed", recoveredCount)
	}

	// Create HTTP handler
	ingestHandler := httpapi.NewIngestHandler(builder, metaGen, a.catalog, a.storage, a.walInstance, a.materializer)

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
		ingestServer := grpcapi.NewIngestServer(a.walInstance)
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

	// Initialize co-access graph for predictive prefetch
	if a.cfg.Cache.PrefetchEnabled {
		a.coAccessGraph = cache.NewCoAccessGraph(0.95, 0.70, 10)
		log.Printf("Co-access graph initialized for predictive prefetch")
	}

	// Initialize NVMe cache for hot partitions
	var nvmeCache *cache.NVMeCache
	if a.cfg.Cache.NVMeDir != "" {
		var err error
		nvmeCache, err = cache.NewNVMeCache(a.cfg.Cache.NVMeDir, a.cfg.Cache.NVMeMaxBytes)
		if err != nil {
			return fmt.Errorf("failed to initialize NVMe cache: %w", err)
		}
		a.nvmeCache = nvmeCache
		log.Printf("NVMe cache initialized: dir=%s, max_bytes=%d", a.cfg.Cache.NVMeDir, a.cfg.Cache.NVMeMaxBytes)
	}

	// Initialize query planner with index lookup and notifier for write visibility
	indexCatalog, ok := a.catalog.(index.IndexCatalog)
	if !ok {
		return fmt.Errorf("catalog does not implement IndexCatalog interface")
	}
	indexLookup := index.NewLookup(a.storage, indexCatalog, a.cfg.Query.DownloadDir, a.cfg.Index.BucketCount)
	queryPlanner := planner.NewPlannerWithIndex(a.catalogReader, pruner, indexLookup)
	queryPlanner = planner.NewPlannerWithNotifierWithPlanner(queryPlanner, a.notifier)
	log.Printf("Query planner initialized with index lookup and notifier: bucket_count=%d", a.cfg.Index.BucketCount)

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
	a.queryExecutor, err = executor.NewParallelExecutor(queryPlanner, a.storage, execConfig, nil, a.coAccessGraph)
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

	// Initialize query statistics tracker (for automated index creation)
	queryStats := observability.NewQueryStats(1 * time.Hour)

	// Create HTTP handler
	queryHandler := httpapi.NewQueryHandler(a.queryExecutor, queryStats)

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

	// Start index policy (runs in compact service)
	log.Printf("Index policy enabled: create_threshold=%d, drop_threshold=%d, max_indexes=%d",
		a.cfg.Index.CreateThreshold, a.cfg.Index.DropThreshold, a.cfg.Index.MaxIndexes)

	// Type-assert to get IndexCatalog interface
	indexCatalog, ok := a.catalog.(index.IndexCatalog)
	if !ok {
		return fmt.Errorf("catalog does not implement IndexCatalog interface")
	}

	// Create index builder
	indexBuilder := index.NewBuilder(a.storage, indexCatalog, a.cfg.Compaction.WorkDir, a.cfg.Index.BucketCount)

	// Create partition provider adapter
	partitionProvider := &manifestPartitionProvider{catalog: a.catalog}

	// Create and start index policy
	indexPolicy := index.NewPolicy(
		nil, // queryStats - nil when observability disabled
		indexBuilder,
		indexCatalog,
		partitionProvider,
		a.storage,
		a.cfg.Index,
	)

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		indexPolicy.Run(ctx)
	}()
	log.Printf("Index policy started: check_interval=%v", a.cfg.Index.CheckInterval)

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
	// Flush remaining WAL entries before closing resources
	// This ensures all pending entries are persisted before shutdown
	if a.flusher != nil && a.walInstance != nil {
		// Create a background context for final flush (non-cancellable)
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer flushCancel()

		// Flush all entries up to the current LSN
		if err := a.flusher.FlushUpTo(flushCtx, a.walInstance.CurrentLSN()); err != nil {
			log.Printf("Final WAL flush error: %v", err)
		} else {
			log.Printf("Final WAL flush completed")
		}
	}

	// Close NVMe cache
	if a.nvmeCache != nil {
		a.nvmeCache.Close()
		log.Printf("NVMe cache closed")
	}

	// Close WAL
	if a.walInstance != nil {
		if err := a.walInstance.Close(); err != nil {
			log.Printf("WAL close error: %v", err)
		} else {
			log.Printf("WAL closed")
		}
	}

	// Unsubscribe notifier subscribers
	if a.notifier != nil {
		// The notifier uses sync.Map for subscribers
		// We iterate and close all subscriber channels for clean shutdown
		a.notifier.Close()
		log.Printf("Notifier cleanup complete")
	}

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

// manifestPartitionProvider adapts manifest.Catalog to index.PartitionProvider.
type manifestPartitionProvider struct {
	catalog manifest.Catalog
}

func (p *manifestPartitionProvider) GetPartitions(ctx context.Context) ([]*index.PartitionInfo, error) {
	partitions, err := p.catalog.FindPartitions(ctx, nil)
	if err != nil {
		return nil, err
	}

	result := make([]*index.PartitionInfo, len(partitions))
	for i, part := range partitions {
		result[i] = &index.PartitionInfo{
			PartitionID:  part.PartitionID,
			ObjectPath:   part.ObjectPath,
			RowCount:     part.RowCount,
			MinEventTime: part.MinEventTime,
			MaxEventTime: part.MaxEventTime,
		}
	}
	return result, nil
}
