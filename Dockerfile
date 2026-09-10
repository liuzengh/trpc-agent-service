# syntax=docker/dockerfile:1

FROM node:22-bookworm-slim AS web-build

WORKDIR /src/trpcservice/web

COPY trpcservice/web/package.json trpcservice/web/package-lock.json ./
RUN npm ci

COPY trpcservice/web/ ./
RUN npm run build

FROM golang:1.21-bookworm AS build

ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /out/trpc-service ./cmd/trpc-service \
    && CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /out/trpc-healthcheck ./cmd/trpc-healthcheck

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/trpc-service /app/trpc-service
COPY --from=build /out/trpc-healthcheck /app/trpc-healthcheck
COPY --from=web-build /src/trpcservice/web/dist /app/web

USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 CMD ["/app/trpc-healthcheck", "http://127.0.0.1:8080/healthz"]
ENTRYPOINT ["/app/trpc-service"]
