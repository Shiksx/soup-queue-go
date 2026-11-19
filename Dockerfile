FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=secret,id=ca_bundle \
    if [ -f /run/secrets/ca_bundle ]; then \
      SSL_CERT_FILE=/run/secrets/ca_bundle go mod download; \
    else go mod download; fi
COPY *.go schema.sql ./
RUN CGO_ENABLED=0 go build -trimpath -o /out/queue .

FROM debian:bookworm-slim
COPY --from=build /out/queue /usr/local/bin/queue
USER 65532:65532
ENTRYPOINT ["queue"]
