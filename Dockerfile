FROM golang:1.21.13-alpine3.20 AS build
WORKDIR /src
ENV GOTOOLCHAIN=local GOTELEMETRY=off CGO_ENABLED=0
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build go build -trimpath -ldflags='-s -w' -o /out/trpc-service ./cmd/trpc-service
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build go build -trimpath -ldflags='-s -w' -o /out/mock-model ./cmd/mock-model
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build go build -trimpath -ldflags='-s -w' -o /out/mock-telegram ./cmd/mock-telegram

FROM alpine:3.20.3
RUN adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
ENTRYPOINT ["trpc-service"]
