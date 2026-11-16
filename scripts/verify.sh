#!/usr/bin/env bash
set -euo pipefail

echo 'Testing Relay, Rainbow and collection directory'
go test ./cmd/relay/... ./cmd/rainbow ./cmd/collectiondir

echo 'Running static checks'
go vet ./cmd/relay/... ./cmd/rainbow ./cmd/collectiondir

echo 'Building Relay, Rainbow and collection directory'
go build ./cmd/relay ./cmd/rainbow ./cmd/collectiondir
