#!/usr/bin/env bash
set -euo pipefail

# This exact baseline test fails in the local verification environment but passed
# on GitHub Actions. Keep it visible in CI while the environment difference is
# understood, then remove this exclusion.
known_upstream_failure='^TestClaimDueAccountLimitAlertsRepeatsAfterInterval$'

echo 'Testing Relay and Rainbow (excluding the documented local baseline test)'
go test ./cmd/relay/... ./cmd/rainbow -skip "$known_upstream_failure"

echo 'Running static checks'
go vet ./cmd/relay/... ./cmd/rainbow

echo 'Building Relay and Rainbow'
go build ./cmd/relay ./cmd/rainbow
