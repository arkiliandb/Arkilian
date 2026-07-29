# Build stage
FROM golang:1.23-alpine AS builder

ENV GOTOOLCHAIN=auto

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o arkilian-server .

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

# Create data directory for SQLite
RUN mkdir -p /data

COPY --from=builder /app/arkilian-server /usr/local/bin/arkilian-server

EXPOSE 8080

ENTRYPOINT ["arkilian-server"]
