# Build from the repository root:
# docker build -f cmd/relay/Dockerfile -t hypercerts-relay .
FROM golang:1.26-alpine3.22 AS build
RUN apk add --no-cache build-base

WORKDIR /src
COPY . .
RUN go build -tags timetzdata -o /relay ./cmd/relay
# hypercerts: Compile the shared bootstrap without adding a module dependency.
RUN CGO_ENABLED=0 go build -trimpath -o /container-entrypoint ./jetstream/cmd/container-entrypoint/main.go

FROM alpine:3.24
RUN apk add --no-cache ca-certificates dumb-init
ENTRYPOINT ["dumb-init", "--", "/container-entrypoint", "relay"]
WORKDIR /
RUN mkdir -p /data/relay
COPY --from=build /relay /relay
COPY --from=build /container-entrypoint /container-entrypoint
ENV GODEBUG=netdns=go
ENV TZ=Etc/UTC
EXPOSE 2470
CMD ["/relay", "serve"]

LABEL org.opencontainers.image.source=https://github.com/hypercerts-org/hypercerts-relay
LABEL org.opencontainers.image.description="Hypercerts AT Protocol Relay"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
