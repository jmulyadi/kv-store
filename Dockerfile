# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum, download deps
COPY go.mod go.sum ./
RUN go mod download

# Copy everything else
COPY . .

# Run this to tidy up dependencies (optional but recommended)
RUN go mod tidy

# Build the server binary
RUN go build -o kvserver ./cmd/server/

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/kvserver .

# Expose port (you can override in docker-compose)
EXPOSE 50051

ENTRYPOINT ["./kvserver"]
