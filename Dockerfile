# syntax=docker/dockerfile:1

# Stage 1: Build
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git build-base

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binaries for all services
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-w -s" -o /go/bin/arkilian-ingest ./cmd/arkilian-ingest && \
    CGO_ENABLED=1 GOOS=linux go build -ldflags="-w -s" -o /go/bin/arkilian-query ./cmd/arkilian-query && \
    CGO_ENABLED=1 GOOS=linux go build -ldflags="-w -s" -o /go/bin/arkilian-compact ./cmd/arkilian-compact

# Stage 2: Runtime
# Use distroless for security (no shell, no package manager)
FROM gcr.io/distroless/static-debian12

WORKDIR /

# Copy binaries from builder
COPY --from=builder /go/bin/arkilian-ingest /usr/local/bin/
COPY --from=builder /go/bin/arkilian-query /usr/local/bin/
COPY --from=builder /go/bin/arkilian-compact /usr/local/bin/

# Expose ports (Ingest: 8080. Query: 8081)
EXPOSE 8080 8081

# Default entrypoint (can be overridden to run other services)
ENTRYPOINT ["/usr/local/bin/arkilian-ingest"]
