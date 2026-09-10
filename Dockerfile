# Two stages: build in golang:1.24 (already present on this machine), run from
# scratch. There is deliberately no `# syntax=` directive at the top — it makes
# the builder fetch the Dockerfile frontend image from Docker Hub, which is
# unreachable here (docs/spec-deployment-fault-drill.md fact #1). Everything
# below works on the classic builder and pulls nothing at build time except the
# Go module cache.

ARG GO_IMAGE=golang:1.24

FROM ${GO_IMAGE} AS builder

# GOTOOLCHAIN=local: go.mod asks for go 1.24.1 and this image ships go1.24.13,
# so no toolchain download is ever needed. Pinning it means that if go.mod is
# bumped past the image's toolchain, the build fails loudly instead of hanging
# on a fetch that cannot complete offline.
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=0 \
    GOOS=linux

# goproxy.cn is the only module proxy reachable from here (fact #2). Override
# with --build-arg GOPROXY=... or set it to "off" once vendor/ is committed.
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /src

# go.mod and go.sum first: the dependency layer then survives source edits. If
# a vendor/ directory is ever committed, delete this line — Go switches to
# -mod=vendor on its own and the build becomes fully offline (fact #2).
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# One image, two binaries: the platform and the fault-injecting fake model the
# drills drive (cmd/fake-model). Both are static, so scratch can run either and
# the demo stack needs no python image (fact #11).
#
# /out/data exists only to be copied into the runtime stage below.
RUN mkdir -p /out/data && \
    go build -trimpath -ldflags="-s -w" -o /out/trpc-service ./cmd/trpc-service && \
    go build -trimpath -ldflags="-s -w" -o /out/fake-model ./cmd/fake-model

FROM scratch

# TLS roots: the platform calls model APIs and IM webhooks over HTTPS, and
# scratch ships nothing.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# audit.New opens its file with O_CREATE but never mkdirs the parent, and
# scratch has no shell to create it at runtime. Bake the directory in and hand
# it to the runtime UID. Compose and K8s mount a volume over /data, but a bare
# `docker run` must not die on a missing directory either.
COPY --from=builder --chown=65534:65534 /out/data /data

COPY --from=builder /out/trpc-service /trpc-service
COPY --from=builder /out/fake-model /fake-model

# scratch has no /etc/passwd, so a numeric UID is the only way to drop
# privileges. 65534 (nobody) is the conventional choice and matches the chown
# above.
USER 65534:65534

EXPOSE 8080

ENTRYPOINT ["/trpc-service"]

# A missing /config/config.yaml is not an error: config.Load falls back to the
# MODEL_* environment variables, so `docker run -e MODEL_API_KEY=... -e
# MODEL_NAME=... image` serves with no mounts at all. Mount a real config at
# /config/config.yaml for anything beyond a smoke test (deploy/README.md).
CMD ["-addr", ":8080", "-config", "/config/config.yaml"]

# --timeout must stay above the binary's own 8s probe budget
# (healthcheckTimeout in cmd/trpc-service/main.go). A shorter one makes Docker
# report a bare timeout and hides the 503 reasons the probe was built to
# surface. --start-period covers the session backend probe at boot.
HEALTHCHECK --interval=15s --timeout=9s --start-period=20s --retries=3 \
    CMD ["/trpc-service", "-addr", ":8080", "-healthcheck"]
