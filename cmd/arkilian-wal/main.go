// Command arkilian-wal provides the WAL coordinator service for Arkilian V3.
// It runs a Raft-backed WAL coordinator that accepts gRPC from ingest nodes,
// manages WAL segments, handles Raft consensus, and uploads to S3.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/arkilian/arkilian/internal/config"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
	"google.golang.org/grpc"
)

func main() {
	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize storage
	s3, err := storage.NewS3Storage(
		context.Background(),
		cfg.Storage.S3.Bucket,
		storage.DefaultS3Config(),
	)
	if err != nil {
		log.Fatalf("Failed to initialize S3 storage: %v", err)
	}

	// Initialize WAL coordinator
	coordinator, err := wal.NewWalCoordinator(cfg.V3.SharedWAL.RaftDataDir, cfg.V3.SharedWAL.RaftPeers, s3)
	if err != nil {
		log.Fatalf("Failed to initialize WAL coordinator: %v", err)
	}
	defer coordinator.Close()

	log.Printf("WAL coordinator initialized: data_dir=%s, peers=%v", cfg.V3.SharedWAL.RaftDataDir, cfg.V3.SharedWAL.RaftPeers)

	// Initialize gRPC server
	grpcServer := grpc.NewServer()
	// TODO: Register WAL service

	// Start gRPC server
	grpcPort := cfg.V3.SharedWAL.GRPCPort
	if grpcPort == 0 {
		grpcPort = 9090
	}
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port: %v", err)
	}

	log.Printf("Prometheus metrics endpoint: :2112/metrics (to be implemented)")

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()
	}()

	// Start gRPC server in background
	go func() {
		log.Printf("gRPC server listening on :%d", grpcPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// Wait for shutdown
	<-ctx.Done()

	// Graceful shutdown
	log.Println("Shutting down gRPC server...")
	grpcServer.GracefulStop()

	log.Println("WAL coordinator stopped")
}

func loadConfig() (*config.Config, error) {
	// Load from environment variables
	cfg := config.DefaultConfig()
	config.LoadFromEnv(cfg)

	// Validate V3 config
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}
