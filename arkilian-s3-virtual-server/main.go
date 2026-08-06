package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"arkilian-s3-virtual-server/server"
)

func main() {
	port := flag.Int("port", 9000, "Port to listen on")
	dataDir := flag.String("data-dir", "./data", "Directory to store S3 bucket objects")
	accessKey := flag.String("access-key", "arkilian-access-key", "AWS SigV4 Access Key")
	secretKey := flag.String("secret-key", "arkilian-secret-key", "AWS SigV4 Secret Key")
	region := flag.String("region", "us-east-1", "AWS S3 Region")
	requireAuth := flag.Bool("require-auth", false, "Require SigV4 or Bearer authentication")
	flag.Parse()

	cfg := server.ServerConfig{
		Port:        *port,
		DataDir:     *dataDir,
		AccessKey:   *accessKey,
		SecretKey:   *secretKey,
		Region:      *region,
		RequireAuth: *requireAuth,
	}

	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	// Handle graceful shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil && err != fmt.Errorf("http: Server closed") {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-stopChan
	log.Println("Shutting down Arkilian S3 Virtual Server gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("Server stopped.")
}
