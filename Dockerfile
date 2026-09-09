# Multi-stage build for trpc-service. The runtime image is distroless: no
# shell, no package manager, non-root.
#
#   docker build -t trpc-agent-service:dev .
#   docker run --rm trpc-agent-service:dev            # all-in-one
#   docker run --rm trpc-agent-service:dev serve worker

FROM golang:1.27-bookworm AS build
WORKDIR /src
# wxbizmsgcrypt is vendored as a local module (go.mod replace) — copy it
# before go mod download.
COPY go.mod go.sum ./
COPY wxbizmsgcrypt/ wxbizmsgcrypt/
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/trpc-service ./cmd/trpc-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/trpc-service /usr/local/bin/trpc-service
USER nonroot
ENTRYPOINT ["trpc-service", "serve"]
CMD ["all"]
