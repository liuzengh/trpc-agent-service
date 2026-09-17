FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# BuildKit supplies these platform arguments for the requested target. Keep
# them declaration-only: defaults here would silently overwrite an explicit
# `docker build --platform` selection.
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /out/trpc-service ./cmd/trpc-service
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /out/trpc-healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot

COPY --chown=nonroot:nonroot --from=build /out/trpc-service /trpc-service
COPY --chown=nonroot:nonroot --from=build /out/trpc-healthcheck /trpc-healthcheck
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/trpc-healthcheck", "-path", "/readyz"]
ENTRYPOINT ["/trpc-service"]
CMD ["artifact"]

# A code-execution Worker is intentionally a separate image target. The
# regular service image stays distroless and cannot accidentally acquire a
# shell. This target carries only the upstream managed-sandbox prerequisites;
# the executed program is still placed in bubblewrap's clean environment with
# an unshared network namespace by trpc-agent-go.
FROM debian:bookworm-slim AS codeexec
RUN apt-get update \
    && apt-get install --no-install-recommends -y bash bubblewrap ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 trpc \
    && useradd --uid 65532 --gid 65532 --create-home --home-dir /var/lib/trpc --shell /usr/sbin/nologin trpc
COPY --from=build /out/trpc-service /trpc-service
COPY --from=build /out/trpc-healthcheck /trpc-healthcheck
USER 65532:65532
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/trpc-healthcheck", "-path", "/readyz"]
ENTRYPOINT ["/trpc-service"]
CMD ["worker"]
