#!/usr/bin/env bash
set -euo pipefail

echo 'Testing Relay, Rainbow, collection directory and PDS discovery'
go test ./cmd/relay/... ./cmd/rainbow ./cmd/collectiondir ./cmd/pdsdiscovery

echo 'Running static checks'
go vet ./cmd/relay/... ./cmd/rainbow ./cmd/collectiondir ./cmd/pdsdiscovery

echo 'Building Relay, Rainbow, collection directory and PDS discovery'
go build ./cmd/relay ./cmd/rainbow ./cmd/collectiondir ./cmd/pdsdiscovery
