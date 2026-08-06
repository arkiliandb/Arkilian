# Arkilian S3 Virtual Server

`arkilian-s3-virtual-server` is a high-performance, S3-compatible virtual storage server and control plane observer written in Go. It is specifically engineered to handle multi-tenant snapshot storage and CDC streaming for Arkilian clients (`src/class.c`, Node.js, Go SDKs).

## Features

- **S3 Protocol Compatibility**:
  - `PUT /{bucket}/{key...}`: Supports direct uploads via SigV4 signed URLs or HTTP PUT.
  - `GET /{bucket}/{key...}`: Serves object snapshots and WAL chunks with byte-range (`Range: bytes=...`) support.
  - `HEAD /{bucket}/{key...}`: Retrieves object existence, size, and ETags.
  - `DELETE /{bucket}/{key...}`: Removes objects and metadata atomically.
  - `GET /{bucket}?prefix=...`: Lists bucket contents in standard S3 XML format (`ListBucketResult`).

- **AWS Signature V4 Authentication**:
  - Validates presigned query strings (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`).
  - Supports `Authorization: AWS4-HMAC-SHA256 ...` headers and Bearer token fallbacks.

- **Atomic Disk Engine**:
  - Writes uploads to temporary files first before performing atomic renames, preventing corrupt snapshot reads.
  - Path traversal protection (`filepath.Clean` and root boundary verification).

- **Control Plane Observer Endpoints**:
  - `/v1/upload/request`: Mints upload URLs for Arkilian clients.
  - `/v1/wal/push`: Receives raw CDC WAL streaming events.
  - `/v1/health` & `/v1/stats`: Observability and cluster metrics.

---

## Quick Start

### 1. Build and Run

```bash
cd arkilian-s3-virtual-server
go build -o arkilian-s3-virtual-server .

# Start the server on port 9000
./arkilian-s3-virtual-server -port=9000 -data-dir=./data
```

### 2. Command Line Flags

| Flag | Default | Description |
|---|---|---|
| `-port` | `9000` | HTTP port to listen on |
| `-data-dir` | `./data` | Local directory for storing S3 object files |
| `-access-key` | `arkilian-access-key` | AWS SigV4 Access Key ID |
| `-secret-key` | `arkilian-secret-key` | AWS SigV4 Secret Access Key |
| `-region` | `us-east-1` | S3 Region |
| `-require-auth` | `false` | Require SigV4 / Bearer authentication |

---

## Integration with Arkilian C Client

To configure an Arkilian instance to upload directly to this S3 virtual server:

```bash
export ARKILIAN_S3_ENDPOINT="http://localhost:9000"
export ARKILIAN_S3_BUCKET="arkilian-backups"
export ARKILIAN_S3_ACCESS_KEY="arkilian-access-key"
export ARKILIAN_S3_SECRET_KEY="arkilian-secret-key"
export ARKILIAN_CONTROL_URL="http://localhost:9000"
```

The C library (`src/class.c`) will calculate SigV4 signatures locally using `src/sha256.c` and stream snapshots directly to `arkilian-s3-virtual-server`.

---

## Docker Deployment

```bash
docker build -t arkilian-s3-virtual-server .
docker run -d -p 9000:9000 -v $(pwd)/data:/root/data arkilian-s3-virtual-server
```

---

## License

Arkilian S3 Virtual Server is licensed under the MIT License.
