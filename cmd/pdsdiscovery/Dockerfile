# Build from the repository root:
# docker build -f cmd/pdsdiscovery/Dockerfile -t hypercerts-pdsdiscovery .
ARG VERSION=0.10.1
ARG COMMIT=unknown
ARG DATE=unknown

FROM golang:1.27.3-alpine3.24 AS build
ARG VERSION

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -tags timetzdata \
    -ldflags "-X github.com/bluesky-social/indigo/hypercerts/version.Version=${VERSION}" \
    -o /pdsdiscovery ./cmd/pdsdiscovery
RUN CGO_ENABLED=0 go build -trimpath -o /container-entrypoint ./jetstream/cmd/container-entrypoint/main.go

FROM alpine:3.24
ARG VERSION
ARG COMMIT
ARG DATE
RUN apk add --no-cache ca-certificates dumb-init
ENTRYPOINT ["dumb-init", "--", "/container-entrypoint", "pdsdiscovery"]
WORKDIR /
COPY --from=build /pdsdiscovery /pdsdiscovery
COPY --from=build /container-entrypoint /container-entrypoint
ENV GODEBUG=netdns=go
ENV TZ=Etc/UTC
USER 65534:65534
CMD ["/pdsdiscovery"]

LABEL org.opencontainers.image.source=https://github.com/hypercerts-org/hypercerts-relay
LABEL org.opencontainers.image.description="Hypercerts automatic PDS discovery"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${COMMIT}"
LABEL org.opencontainers.image.created="${DATE}"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
