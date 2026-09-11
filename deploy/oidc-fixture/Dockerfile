FROM golang:1.24-alpine AS build
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/oidc-fixture main.go

FROM alpine:3.21
COPY --from=build /out/oidc-fixture /usr/local/bin/oidc-fixture
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/oidc-fixture"]
