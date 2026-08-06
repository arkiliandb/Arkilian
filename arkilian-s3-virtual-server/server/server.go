package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// ServerConfig holds runtime flags and environment configuration.
type ServerConfig struct {
	Port        int
	DataDir     string
	AccessKey   string
	SecretKey   string
	Region      string
	RequireAuth bool
}

// Server encapsulates the HTTP server and underlying storage/handlers.
type Server struct {
	cfg        ServerConfig
	storage    *StorageEngine
	verifier   *SigV4Verifier
	s3Handler  *S3Handler
	cpHandler  *ControlPlaneHandler
	httpServer *http.Server
}

// NewServer builds and configures all components.
func NewServer(cfg ServerConfig) (*Server, error) {
	storage, err := NewStorageEngine(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage engine: %w", err)
	}

	verifier := NewSigV4Verifier(cfg.AccessKey, cfg.SecretKey, cfg.Region)
	s3Handler := NewS3Handler(storage, verifier, cfg.RequireAuth)

	serverURL := fmt.Sprintf("http://127.0.0.1:%d", cfg.Port)
	cpHandler := NewControlPlaneHandler(storage, serverURL, verifier)

	srv := &Server{
		cfg:       cfg,
		storage:   storage,
		verifier:  verifier,
		s3Handler: s3Handler,
		cpHandler: cpHandler,
	}

	mux := http.NewServeMux()

	// Control Plane endpoints
	mux.HandleFunc("/v1/upload/request", cpHandler.HandlePresignedURLRequest)
	mux.HandleFunc("/v1/wal/push", cpHandler.HandleWALPush)
	mux.HandleFunc("/v1/health", cpHandler.HandleHealth)
	mux.HandleFunc("/v1/stats", cpHandler.HandleStats)

	// Catch-all S3 handler
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasPrefix(path, "/v1/") {
			http.NotFound(w, r)
			return
		}
		s3Handler.ServeHTTP(w, r)
	})

	srv.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      srv.loggingMiddleware(mux),
		ReadTimeout:  30 * time.Minute, // Long timeout for multi-GB snapshot uploads
		WriteTimeout: 30 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	return srv, nil
}

// Start launches the HTTP server.
func (s *Server) Start() error {
	log.Printf("Arkilian S3 Virtual Server starting on port %d...", s.cfg.Port)
	log.Printf("Storage directory: %s", s.cfg.DataDir)
	log.Printf("Authentication required: %v", s.cfg.RequireAuth)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s (%v)", r.RemoteAddr, r.Method, r.URL.Path, time.Since(start))
	})
}
