FROM golang:1.24.6 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build

# Copy go module files first
COPY go.mod go.sum ./

# Copy vendor directory if it exists
COPY vendor/ ./vendor/

# Copy source code
COPY . .

# Build the binary from local path (not module path)
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/proxy ./proxy

FROM busybox:latest
COPY --from=builder /build/bin/proxy /proxy
ENTRYPOINT ["/bin/sh"]
