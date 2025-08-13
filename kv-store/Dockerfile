# --- Build stage ---
FROM golang:1.24-alpine AS builder

# Install git (needed for go modules)
RUN apk add --no-cache git

# Set workdir
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN go build -o kvserver ./cmd/server/main.go

# --- Run stage ---
FROM alpine:3.18

# Set workdir
WORKDIR /app

# Copy binary from build stage
COPY --from=builder /app/kvserver .

# Expose default gRPC port (can be overridden in docker-compose)
EXPOSE 50051

# Command to run; can override via docker-compose command
CMD ["./kvserver", "-addr", "localhost:50051"]
