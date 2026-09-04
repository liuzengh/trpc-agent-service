# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.26-alpine AS build
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY} \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /src

# Cache module downloads first for faster incremental builds.
COPY go.mod go.sum ./
RUN go mod download

# Build the service binary.
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/trpc-service ./cmd/trpc-service

# ---- Runtime stage ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S app -g 10001 \
    && adduser -S -D -u 10001 -G app app

COPY --from=build /out/trpc-service /usr/local/bin/trpc-service
COPY configs/config.yaml /etc/trpc-service/config.yaml

USER app
EXPOSE 8080
ENTRYPOINT ["trpc-service"]
CMD ["--config", "/etc/trpc-service/config.yaml", "--role", "all"]
