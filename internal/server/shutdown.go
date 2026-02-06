// Package server provides server lifecycle management including graceful shutdown.
package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ShutdownManager handles graceful shutdown of server components.
// It coordinates signal handling, in-flight request tracking, and resource cleanup.
type ShutdownManager struct {
	// Configuration
	shutdownTimeout time.Duration
	drainTimeout    time.Duration

	// State
	shutdownCh    chan struct{}
	shutdownOnce  sync.Once
	inFlight      int64
	isShuttingDown int32

	// Closers to clean up on shutdown
	closers   []io.Closer
	closersMu sync.Mutex

	// Callbacks
	onShutdownStart []func()
	onShutdownEnd   []func()
	callbacksMu     sync.Mutex
}

// ShutdownConfig holds configuration for the shutdown manager.
type ShutdownConfig struct {
	// ShutdownTimeout is the maximum time to wait for graceful shutdown.
	// Default: 30 seconds
	ShutdownTimeout time.Duration

	// DrainTimeout is the time to wait for in-flight requests to complete.
	// Default: 15 seconds
	DrainTimeout time.Duration
}

// DefaultShutdownConfig returns the default shutdown configuration.
func DefaultShutdownConfig() ShutdownConfig {
	return ShutdownConfig{
		ShutdownTimeout: 30 * time.Second,
		DrainTimeout:    15 * time.Second,
	}
}

// NewShutdownManager creates a new shutdown manager with the given configuration.
func NewShutdownManager(config ShutdownConfig) *ShutdownManager {
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = 15 * time.Second
	}

	return &ShutdownManager{
		shutdownTimeout: config.ShutdownTimeout,
		drainTimeout:    config.DrainTimeout,
		shutdownCh:      make(chan struct{}),
	}
}

// RegisterCloser adds a closer to be called during shutdown.
// Closers are called in reverse order of registration (LIFO).
func (sm *ShutdownManager) RegisterCloser(closer io.Closer) {
	sm.closersMu.Lock()
	defer sm.closersMu.Unlock()
	sm.closers = append(sm.closers, closer)
}

// OnShutdownStart registers a callback to be called when shutdown begins.
func (sm *ShutdownManager) OnShutdownStart(fn func()) {
	sm.callbacksMu.Lock()
	defer sm.callbacksMu.Unlock()
	sm.onShutdownStart = append(sm.onShutdownStart, fn)
}

// OnShutdownEnd registers a callback to be called when shutdown completes.
func (sm *ShutdownManager) OnShutdownEnd(fn func()) {
	sm.callbacksMu.Lock()
	defer sm.callbacksMu.Unlock()
	sm.onShutdownEnd = append(sm.onShutdownEnd, fn)
}

// ListenForSignals starts listening for SIGTERM and SIGINT signals.
// When a signal is received, it initiates graceful shutdown.
// This method blocks until shutdown is complete.
func (sm *ShutdownManager) ListenForSignals(ctx context.Context) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		return sm.Shutdown(ctx, fmt.Sprintf("received signal: %v", sig))
	case <-ctx.Done():
		return sm.Shutdown(ctx, "context cancelled")
	case <-sm.shutdownCh:
		return nil
	}
}

// Shutdown initiates graceful shutdown with the given reason.
// It waits for in-flight requests to complete and closes all registered resources.
func (sm *ShutdownManager) Shutdown(ctx context.Context, reason string) error {
	var shutdownErr error

	sm.shutdownOnce.Do(func() {
		atomic.StoreInt32(&sm.isShuttingDown, 1)
		close(sm.shutdownCh)

		// Call shutdown start callbacks
		sm.callbacksMu.Lock()
		startCallbacks := sm.onShutdownStart
		sm.callbacksMu.Unlock()
		for _, fn := range startCallbacks {
			fn()
		}

		// Create shutdown context with timeout
		shutdownCtx, cancel := context.WithTimeout(ctx, sm.shutdownTimeout)
		defer cancel()

		// Wait for in-flight requests to drain
		if err := sm.drainInFlight(shutdownCtx); err != nil {
			shutdownErr = fmt.Errorf("drain failed: %w", err)
		}

		// Close all registered closers in reverse order
		sm.closersMu.Lock()
		closers := sm.closers
		sm.closersMu.Unlock()

		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i].Close(); err != nil {
				if shutdownErr == nil {
					shutdownErr = fmt.Errorf("close failed: %w", err)
				}
			}
		}

		// Call shutdown end callbacks
		sm.callbacksMu.Lock()
		endCallbacks := sm.onShutdownEnd
		sm.callbacksMu.Unlock()
		for _, fn := range endCallbacks {
			fn()
		}
	})

	return shutdownErr
}

// drainInFlight waits for all in-flight requests to complete.
func (sm *ShutdownManager) drainInFlight(ctx context.Context) error {
	drainCtx, cancel := context.WithTimeout(ctx, sm.drainTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if atomic.LoadInt64(&sm.inFlight) == 0 {
			return nil
		}

		select {
		case <-drainCtx.Done():
			remaining := atomic.LoadInt64(&sm.inFlight)
			if remaining > 0 {
				return fmt.Errorf("timeout waiting for %d in-flight requests", remaining)
			}
			return nil
		case <-ticker.C:
			// Continue checking
		}
	}
}

// TrackRequest increments the in-flight request counter.
// Returns false if shutdown is in progress and the request should be rejected.
func (sm *ShutdownManager) TrackRequest() bool {
	if atomic.LoadInt32(&sm.isShuttingDown) == 1 {
		return false
	}
	atomic.AddInt64(&sm.inFlight, 1)
	return true
}

// UntrackRequest decrements the in-flight request counter.
func (sm *ShutdownManager) UntrackRequest() {
	atomic.AddInt64(&sm.inFlight, -1)
}

// IsShuttingDown returns true if shutdown has been initiated.
func (sm *ShutdownManager) IsShuttingDown() bool {
	return atomic.LoadInt32(&sm.isShuttingDown) == 1
}

// InFlightCount returns the current number of in-flight requests.
func (sm *ShutdownManager) InFlightCount() int64 {
	return atomic.LoadInt64(&sm.inFlight)
}

// ShutdownCh returns a channel that is closed when shutdown begins.
func (sm *ShutdownManager) ShutdownCh() <-chan struct{} {
	return sm.shutdownCh
}

// GracefulHTTPServer wraps an http.Server with graceful shutdown support.
type GracefulHTTPServer struct {
	server   *http.Server
	shutdown *ShutdownManager
}

// NewGracefulHTTPServer creates a new graceful HTTP server.
func NewGracefulHTTPServer(server *http.Server, shutdown *ShutdownManager) *GracefulHTTPServer {
	return &GracefulHTTPServer{
		server:   server,
		shutdown: shutdown,
	}
}

// ListenAndServe starts the HTTP server and handles graceful shutdown.
func (gs *GracefulHTTPServer) ListenAndServe() error {
	// Register server for shutdown
	gs.shutdown.RegisterCloser(&httpServerCloser{server: gs.server})

	errCh := make(chan error, 1)
	go func() {
		if err := gs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for shutdown signal or server error
	select {
	case err := <-errCh:
		return err
	case <-gs.shutdown.ShutdownCh():
		// Shutdown initiated, server will be closed by shutdown manager
		return <-errCh
	}
}

// httpServerCloser wraps http.Server to implement io.Closer with graceful shutdown.
type httpServerCloser struct {
	server *http.Server
}

func (c *httpServerCloser) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.server.Shutdown(ctx)
}

// ShutdownMiddleware creates HTTP middleware that tracks in-flight requests
// and rejects new requests during shutdown.
func ShutdownMiddleware(sm *ShutdownManager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !sm.TrackRequest() {
				// Shutdown in progress, reject request
				w.Header().Set("Connection", "close")
				http.Error(w, "Service Unavailable - Shutting Down", http.StatusServiceUnavailable)
				return
			}
			defer sm.UntrackRequest()

			next.ServeHTTP(w, r)
		})
	}
}

// CloserFunc is an adapter to allow ordinary functions to be used as io.Closer.
type CloserFunc func() error

// Close calls the underlying function.
func (f CloserFunc) Close() error {
	return f()
}

// MultiCloser combines multiple closers into one.
type MultiCloser struct {
	closers []io.Closer
}

// NewMultiCloser creates a new multi-closer.
func NewMultiCloser(closers ...io.Closer) *MultiCloser {
	return &MultiCloser{closers: closers}
}

// Close closes all underlying closers, returning the first error encountered.
func (mc *MultiCloser) Close() error {
	var firstErr error
	for _, c := range mc.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
