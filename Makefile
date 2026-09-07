SHELL = /bin/bash
.SHELLFLAGS = -o pipefail -c

.PHONY: build test lint fmt

build:
	go build ./cmd/relay
	go build ./cmd/rainbow

test:
	./scripts/verify.sh

lint:
	go vet ./cmd/relay/... ./cmd/rainbow
	test -z "$$(gofmt -l $$(git ls-files '*.go'))"

fmt:
	gofmt -w $$(git ls-files '*.go')
