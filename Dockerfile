# P1-09 reproducible local deployment image.
# Builder and runtime base images are pinned; their digests are recorded in
# docs/P1-09验收报告.md. The runtime stage contains only the two release
# binaries, the migration files and CA certificates, and runs as a non-root
# user. No .env file, secret, git metadata, report or build artifact is copied.
FROM golang:1.26.3-alpine AS builder
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY trpcservice ./trpcservice
RUN go build -trimpath -ldflags "-s -w" -o /out/trpc-service ./cmd/trpc-service \
    && go build -trimpath -ldflags "-s -w" -o /out/trpc-migrate ./cmd/trpc-migrate

FROM alpine:3.20
RUN addgroup -S app && adduser -S -G app -h /home/app app \
    && apk add --no-cache ca-certificates su-exec
COPY --from=builder /out/trpc-service /usr/local/bin/trpc-service
COPY --from=builder /out/trpc-migrate /usr/local/bin/trpc-migrate
COPY migrations /app/migrations
USER app
ENV MIGRATIONS_DIR=/app/migrations \
    HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/trpc-service"]
