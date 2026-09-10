FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/trpc-service ./cmd/trpc-service

FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S trpc \
    && adduser -S -G trpc trpc

COPY --from=build /out/trpc-service /usr/local/bin/trpc-service

USER trpc

ENTRYPOINT ["/usr/local/bin/trpc-service"]
