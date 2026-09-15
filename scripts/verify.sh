#!/usr/bin/env bash
set -euo pipefail

echo 'Testing Relay and Rainbow'
go test ./cmd/relay/... ./cmd/rainbow

echo 'Running static checks'
go vet ./cmd/relay/... ./cmd/rainbow

echo 'Building Relay and Rainbow'
go build ./cmd/relay ./cmd/rainbow
