#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../jetstream"
go test ./... -timeout 30m
go vet ./...
go build -o ../build/jetstream ./cmd/jetstream
# A separate process is required for each synctest oracle bubble.
JETSTREAM_ORACLE_MODE=stress go test ./internal/oracle -run '^TestOracle_DefaultLifecycle$' -count=1 -timeout 60m
go test ./internal/oracle -run '^TestOracle_Restart' -count=1 -timeout 60m
