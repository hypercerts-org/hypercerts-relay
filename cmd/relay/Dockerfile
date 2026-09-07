# Build from the repository root:
# docker build -f cmd/relay/Dockerfile -t hypercerts-relay .
FROM golang:1.26-alpine3.22 AS build
RUN apk add --no-cache build-base

WORKDIR /src
COPY . .
RUN go build -tags timetzdata -o /relay ./cmd/relay

FROM alpine:3.24
RUN apk add --no-cache ca-certificates dumb-init
ENTRYPOINT ["dumb-init", "--"]
WORKDIR /
RUN mkdir -p /data/relay
COPY --from=build /relay /relay
ENV GODEBUG=netdns=go
ENV TZ=Etc/UTC
EXPOSE 2470
CMD ["/relay", "serve"]

LABEL org.opencontainers.image.source=https://github.com/hypercerts-org/hypercerts-relay
LABEL org.opencontainers.image.description="Hypercerts AT Protocol Relay"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
