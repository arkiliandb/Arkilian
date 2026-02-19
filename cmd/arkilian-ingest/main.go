// Package main implements the arkilian-ingest service binary.
// This service handles batch ingestion of rows into SQLite micro-partitions.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arkilian/arkilian/api/proto"
	grpcapi "github.com/arkilian/arkilian/internal/api/grpc"
	httpapi "github.com/arkilian/arkilian/internal/api/http"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/server"
	"github.com/arkilian/arkilian/internal/storage"
	"google.golang.org/grpc"
)

// Config holds the service configuration.
type Config struct {
	HTTPAddr     string
	GRPCAddr     string
	ManifestPath string
	StoragePath  string
	PartitionDir string
}

func main() {
	cfg := parseFlags()

	log.Printf("Starting arkilian-ingest service...")
	log.Printf("HTTP address: %s", cfg.HTTPAddr)
	log.Printf("gRPC address: %s", cfg.GRPCAddr)

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

	// Initialize partition builder
	builder := partition.NewBuilder(cfg.PartitionDir, 0)
	metaGen := partition.NewMetadataGenerator()
	log.Printf("Partition builder initialized with output dir: %s", cfg.PartitionDir)

	// Initialize shutdown manager
	shutdownConfig := server.DefaultShutdownConfig()
	shutdownMgr := server.NewShutdownManager(shutdownConfig)

	// Create HTTP handler
	ingestHandler := httpapi.NewIngestHandler(builder, metaGen, catalog, store, nil, nil)

	// Setup HTTP server with middleware
	mux := http.NewServeMux()
	middleware := httpapi.ChainMiddleware(
		server.ShutdownMiddleware(shutdownMgr),
		httpapi.RecoveryMiddleware,
		httpapi.RequestIDMiddleware,
		httpapi.CorrelationIDMiddleware,
		httpapi.ContentTypeMiddleware,
	)
	mux.Handle("/v1/ingest", middleware(ingestHandler))
	mux.HandleFunc("/health", healthHandler)

	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	ingestServer := grpcapi.NewIngestServer(builder, metaGen, catalog, store, nil)
	proto.RegisterIngestServiceServer(grpcServer, ingestServer)

	// Register closers for graceful shutdown
	shutdownMgr.RegisterCloser(server.CloserFunc(func() error {
		grpcServer.GracefulStop()
		return nil
	}))

	// Start HTTP server
	go func() {
		log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Start gRPC server
	go func() {
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			log.Fatalf("Failed to listen on gRPC address: %v", err)
		}
		log.Printf("gRPC server listening on %s", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
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

	log.Printf("arkilian-ingest service stopped")
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.HTTPAddr, "http-addr", ":8080", "HTTP server address")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", ":9090", "gRPC server address")
	flag.StringVar(&cfg.ManifestPath, "manifest", "./data/manifest.db", "Path to manifest database")
	flag.StringVar(&cfg.StoragePath, "storage", "./data/storage", "Path to object storage directory")
	flag.StringVar(&cfg.PartitionDir, "partition-dir", "./data/partitions", "Path to partition output directory")

	flag.Parse()

	// Create directories if they don't exist
	for _, dir := range []string{cfg.StoragePath, cfg.PartitionDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	// Ensure manifest directory exists
	manifestDir := cfg.ManifestPath[:len(cfg.ManifestPath)-len("/manifest.db")]
	if manifestDir == "" {
		manifestDir = "."
	}
	if err := os.MkdirAll(manifestDir, 0755); err != nil {
		log.Fatalf("Failed to create manifest directory: %v", err)
	}

	return cfg
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","service":"arkilian-ingest"}`))
}
