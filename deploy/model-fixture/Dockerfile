FROM golang:1.24-alpine AS build

WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/model-fixture main.go

FROM alpine:3.21
COPY --from=build /out/model-fixture /usr/local/bin/model-fixture
USER 65532:65532
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/model-fixture"]
