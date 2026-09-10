# syntax=docker/dockerfile:1.7
FROM node:24-alpine AS console
WORKDIR /console
COPY trpcservice/web/console/package.json trpcservice/web/console/package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY trpcservice/web/console/ ./
RUN npm run build

FROM golang:1.25-alpine AS build

ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
COPY --from=console /admin/ui/dist ./trpcservice/admin/ui/dist
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' \
    -o /out/trpc-service ./cmd/trpc-service && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' \
    -o /out/trpc-migrate ./cmd/trpc-migrate && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' \
    -o /out/trpc-init ./cmd/trpc-init

FROM alpine:3.22

COPY --from=build --chown=65534:65534 /out/trpc-service /usr/local/bin/trpc-service
COPY --from=build --chown=65534:65534 /out/trpc-migrate /usr/local/bin/trpc-migrate
COPY --from=build --chown=65534:65534 /out/trpc-init /usr/local/bin/trpc-init
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/trpc-service"]
