FROM node:20-alpine AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=frontend /src/trpcservice/web/dist ./trpcservice/web/dist
RUN CGO_ENABLED=0 go build -o /out/trpc-service ./cmd/trpc-service
RUN CGO_ENABLED=0 go build -o /out/control-migrate ./cmd/control-migrate

FROM alpine:3.20
COPY --from=build /out/trpc-service /usr/local/bin/trpc-service
COPY --from=build /out/control-migrate /usr/local/bin/control-migrate
RUN mkdir /data && chown 65534:65534 /data
ENV TRPC_SERVICE_ADDR=:8080
EXPOSE 8080
USER 65534:65534
ENTRYPOINT ["trpc-service"]
