# Dockerfile for Arkilian S3 Virtual Server
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o arkilian-s3-virtual-server .

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/arkilian-s3-virtual-server .

EXPOSE 9000

VOLUME ["/root/data"]

ENTRYPOINT ["./arkilian-s3-virtual-server"]
CMD ["-port=9000", "-data-dir=/root/data"]
