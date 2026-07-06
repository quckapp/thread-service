# Build stage
# NOTE: Build context is repo root (.) to resolve replace directives for go-auth / promotion-gate-go
FROM golang:1.23-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git

# Copy local packages (referenced via replace directives)
COPY packages/go-auth /app/packages/go-auth
COPY packages/promotion-gate-go /app/packages/promotion-gate-go

# Copy service go module files and download dependencies
WORKDIR /app/services/thread-service
COPY services/thread-service/go.mod services/thread-service/go.sum ./
RUN go mod download

# Copy service source code
COPY services/thread-service/ .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /app/main ./cmd/main.go

FROM alpine:3.19
WORKDIR /app
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/main .
EXPOSE 5009
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD wget --spider -q http://127.0.0.1:5009/health || exit 1
CMD ["./main"]
