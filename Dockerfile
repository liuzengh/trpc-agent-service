FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party ./third_party
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/trpc-service ./cmd/trpc-service

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/trpc-service /app/trpc-service
COPY configs /app/configs
USER nonroot:nonroot
ENTRYPOINT ["/app/trpc-service"]
CMD ["serve", "--config", "/app/configs/container.yaml", "--role", "all"]
