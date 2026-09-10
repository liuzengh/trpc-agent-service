# syntax=docker/dockerfile:1.7
FROM node:22-alpine AS web
WORKDIR /src/webui
COPY webui/package.json webui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY webui/ ./
RUN npm run build

FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /src/webui/dist ./webui/dist
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/trpc-agent-service ./cmd/trpc-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build --chown=nonroot:nonroot /out/trpc-agent-service /usr/local/bin/trpc-agent-service
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/trpc-agent-service"]
CMD ["serve"]
