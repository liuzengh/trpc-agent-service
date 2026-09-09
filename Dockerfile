FROM golang:1.24.1-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/trpc-service ./cmd/trpc-service

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -S agent \
    && adduser -S -G agent agent
WORKDIR /app
COPY --from=build /out/trpc-service /app/trpc-service

USER agent
EXPOSE 8080
ENTRYPOINT ["/app/trpc-service"]
