ARG VERSION=0.9.0
ARG COMMIT=unknown
ARG DATE=unknown

FROM golang:1.26-bookworm AS build-env
ARG VERSION

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
ENV GODEBUG="netdns=go"
ENV GOOS="linux"
ENV GOARCH="amd64"
ENV CGO_ENABLED="1"

WORKDIR /usr/src/rainbow

COPY . .

RUN go mod download && \
  go mod verify

RUN go build \
    -v  \
    -trimpath \
    -tags timetzdata \
    -ldflags "-X github.com/bluesky-social/indigo/hypercerts/version.Version=${VERSION}" \
    -o /rainbow-bin \
    ./cmd/rainbow

# hypercerts: Share the opt-in Railway bootstrap with the other images.
RUN CGO_ENABLED=0 go build -trimpath -o /container-entrypoint ./jetstream/cmd/container-entrypoint/main.go

FROM debian:bookworm-slim
ARG VERSION
ARG COMMIT
ARG DATE

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ=Etc/UTC
ENV GODEBUG="netdns=go"

RUN apt-get update && apt-get install --yes \
  dumb-init \
  ca-certificates \
  runit

WORKDIR /rainbow
COPY --from=build-env /rainbow-bin /usr/bin/rainbow
COPY --from=build-env /container-entrypoint /container-entrypoint

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/container-entrypoint", "rainbow"]
CMD ["/usr/bin/rainbow"]

LABEL org.opencontainers.image.source=https://github.com/hypercerts-org/hypercerts-relay
LABEL org.opencontainers.image.description="rainbow atproto firehose fanout service"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${COMMIT}"
LABEL org.opencontainers.image.created="${DATE}"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
