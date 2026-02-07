// Package main implements the unified arkilian binary.
// This binary can run all three services (ingest, query, compact) concurrently
// or individual services based on the --mode flag.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/arkilian/arkilian/internal/app"
	"github.com/arkilian/arkilian/internal/config"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	// Parse command line flags
	var (
		configFile  string
		dataDir     string
		mode        string
		httpIngest  string
		httpQuery   string
		httpCompact string
		grpcAddr    string
		showVersion bool
		showHelp    bool
	)

	flag.StringVar(&configFile, "config", "", "Path to configuration file (YAML or JSON)")
	flag.StringVar(&dataDir, "data-dir", "", "Base directory for all data files")
	flag.StringVar(&mode, "mode", "all", "Service mode: all, ingest, query, compact")
	flag.StringVar(&httpIngest, "http-ingest", "", "HTTP address for ingest service")
	flag.StringVar(&httpQuery, "http-query", "", "HTTP address for query service")
	flag.StringVar(&httpCompact, "http-compact", "", "HTTP address for compaction service")
	flag.StringVar(&grpcAddr, "grpc-addr", "", "gRPC server address")
	flag.BoolVar(&showVersion, "version", false, "Show version information")
	flag.BoolVar(&showHelp, "help", false, "Show help message")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Arkilian - The Immutable Database Engine For Real Applications\n\n")
		fmt.Fprintf(os.Stderr, "Usage: arkilian [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  arkilian --data-dir /data/arkilian\n")
		fmt.Fprintf(os.Stderr, "  arkilian --mode ingest --data-dir /data/arkilian\n")
		fmt.Fprintf(os.Stderr, "  arkilian --config /etc/arkilian/config.yaml\n")
		fmt.Fprintf(os.Stderr, "\nEnvironment Variables:\n")
		fmt.Fprintf(os.Stderr, "  ARKILIAN_MODE           Service mode (all, ingest, query, compact)\n")
		fmt.Fprintf(os.Stderr, "  ARKILIAN_DATA_DIR       Base directory for data files\n")
		fmt.Fprintf(os.Stderr, "  ARKILIAN_HTTP_*_ADDR    HTTP addresses for services\n")
		fmt.Fprintf(os.Stderr, "  ARKILIAN_GRPC_ADDR      gRPC server address\n")
		fmt.Fprintf(os.Stderr, "  ARKILIAN_STORAGE_TYPE   Storage type (local, s3)\n")
	}

	flag.Parse()

	if showHelp {
		flag.Usage()
		os.Exit(0)
	}

	if showVersion {
		fmt.Printf("arkilian version %s (commit: %s)\n", version, commit)
		os.Exit(0)
	}

	// Load configuration
	cfg, err := loadConfig(configFile, dataDir, mode, httpIngest, httpQuery, httpCompact, grpcAddr)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Print startup banner
	printBanner(cfg)

	// Create and start the application
	application, err := app.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create application: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := application.Start(ctx); err != nil {
		log.Fatalf("Failed to start application: %v", err)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	log.Printf("Received signal: %v", sig)

	// Graceful shutdown
	if err := application.Stop(context.Background()); err != nil {
		log.Printf("Shutdown error: %v", err)
		os.Exit(1)
	}
}

// loadConfig loads configuration from file, environment, and command line flags.
func loadConfig(configFile, dataDir, mode, httpIngest, httpQuery, httpCompact, grpcAddr string) (*config.Config, error) {
	var cfg *config.Config
	var err error

	// Start with defaults or load from file
	if configFile != "" {
		cfg, err = config.LoadFromFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	// Apply environment variables
	config.LoadFromEnv(cfg)

	// Apply command line flags (highest priority)
	if dataDir != "" {
		cfg.DataDir = dataDir
	}
	if mode != "" {
		cfg.Mode = config.Mode(mode)
	}
	if httpIngest != "" {
		cfg.HTTP.IngestAddr = httpIngest
	}
	if httpQuery != "" {
		cfg.HTTP.QueryAddr = httpQuery
	}
	if httpCompact != "" {
		cfg.HTTP.CompactAddr = httpCompact
	}
	if grpcAddr != "" {
		cfg.GRPC.Addr = grpcAddr
	}

	return cfg, nil
}

// printBanner prints the startup banner with configuration summary.
func printBanner(cfg *config.Config) {
	log.Printf("╔═══════════════════════════════════════════════════════════╗")
	log.Printf("║                      ARKILIAN                             ║")
	log.Printf("║   The Immutable Database Engine For Real Applications     ║")
	log.Printf("╚═══════════════════════════════════════════════════════════╝")
	log.Printf("")
	log.Printf("Configuration:")
	log.Printf("  Mode:     %s", cfg.Mode)
	log.Printf("  Data Dir: %s", cfg.DataDir)
	log.Printf("  Storage:  %s", cfg.Storage.Type)
	log.Printf("")

	if cfg.ShouldRunIngest() {
		log.Printf("Ingest Service:")
		log.Printf("  HTTP: %s", cfg.HTTP.IngestAddr)
		if cfg.GRPC.Enabled {
			log.Printf("  gRPC: %s", cfg.GRPC.Addr)
		}
	}

	if cfg.ShouldRunQuery() {
		log.Printf("Query Service:")
		log.Printf("  HTTP: %s", cfg.HTTP.QueryAddr)
		log.Printf("  Concurrency: %d", cfg.Query.Concurrency)
	}

	if cfg.ShouldRunCompact() {
		log.Printf("Compaction Service:")
		log.Printf("  HTTP: %s", cfg.HTTP.CompactAddr)
		log.Printf("  Check Interval: %v", cfg.Compaction.CheckInterval)
	}

	log.Printf("")
}
