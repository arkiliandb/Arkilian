// Package http provides HTTP API handlers for the Arkilian system.
package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

// Context keys for request metadata.
type contextKey string

const (
	// requestIDKey is the context key for the request ID.
	requestIDKey contextKey = "request_id"
	// correlationIDKey is the context key for the correlation ID.
	correlationIDKey contextKey = "correlation_id"
)

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}

// RequestIDMiddleware adds a unique request_id to each request.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if request_id is provided in header, otherwise generate one
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Add request_id to response header
		w.Header().Set("X-Request-ID", requestID)

		// Add request_id to context
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// CorrelationIDMiddleware adds a correlation ID for distributed tracing.
func CorrelationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if correlation_id is provided in header, otherwise use request_id
		correlationID := r.Header.Get("X-Correlation-ID")
		if correlationID == "" {
			// Fall back to request_id if available
			if reqID, ok := r.Context().Value(requestIDKey).(string); ok {
				correlationID = reqID
			} else {
				correlationID = uuid.New().String()
			}
		}

		// Add correlation_id to response header
		w.Header().Set("X-Correlation-ID", correlationID)

		// Add correlation_id to context
		ctx := context.WithValue(r.Context(), correlationIDKey, correlationID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RecoveryMiddleware recovers from panics and returns a 500 error.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				requestID, _ := r.Context().Value(requestIDKey).(string)
				writeError(w, http.StatusInternalServerError, "internal server error", requestID)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ContentTypeMiddleware ensures JSON content type for API requests.
func ContentTypeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set JSON content type for responses
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// ChainMiddleware chains multiple middleware functions together.
func ChainMiddleware(middlewares ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(final http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

// DefaultMiddleware returns the default middleware chain for API handlers.
func DefaultMiddleware() func(http.Handler) http.Handler {
	return ChainMiddleware(
		RecoveryMiddleware,
		RequestIDMiddleware,
		CorrelationIDMiddleware,
		ContentTypeMiddleware,
	)
}

// writeError writes an error response with the given status code.
func writeError(w http.ResponseWriter, statusCode int, message string, requestID ...string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	resp := ErrorResponse{
		Error: message,
	}
	if len(requestID) > 0 && requestID[0] != "" {
		resp.RequestID = requestID[0]
	}

	json.NewEncoder(w).Encode(resp)
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// GetRequestID retrieves the request ID from the context.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// GetCorrelationID retrieves the correlation ID from the context.
func GetCorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(correlationIDKey).(string); ok {
		return id
	}
	return ""
}
