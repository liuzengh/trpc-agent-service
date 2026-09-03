# syntax=docker/dockerfile:1.7
FROM golang:1.25-alpine AS build

ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' \
    -o /out/trpc-service ./cmd/trpc-service && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' \
    -o /out/trpc-migrate ./cmd/trpc-migrate

FROM alpine:3.22

COPY --from=build --chown=65534:65534 /out/trpc-service /usr/local/bin/trpc-service
COPY --from=build --chown=65534:65534 /out/trpc-migrate /usr/local/bin/trpc-migrate
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/trpc-service"]
