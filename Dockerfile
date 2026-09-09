FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/trpc-service ./cmd/trpc-service

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/trpc-service /usr/local/bin/trpc-service
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/trpc-service"]
