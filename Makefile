.PHONY: build build-static test race fmt-check vet vulncheck test-monitoring test-contract test-system

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

build:
	CGO_ENABLED=1 go build -ldflags "-X main.version=$(VERSION)" -o loghub ./cmd/loghub

# A fully static binary that runs on any x86-64 Linux, e.g. to copy to a
# test VM whose glibc is older than the build machine's.
build-static:
	CGO_ENABLED=1 go build -tags netgo,osusergo -ldflags "-X main.version=$(VERSION) -linkmode external -extldflags -static" -o loghub ./cmd/loghub

test:
	go test ./...

# Monitoring contract and black-box endpoint/status scenarios.
test-monitoring:
	go test ./tests/monitoring -count=1 -v

# Plugin schema/factory contract.
test-contract:
	go test ./tests/contract -count=1 -v

# Real-binary system scenarios (SQLite, local_jsonl, HTTP, reload, CLI).
test-system:
	go test ./tests/contract ./tests/system -count=1 -v

race:
	go test -race ./...

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

# Fails on vulnerabilities reachable from LogHub's code. Pinned so CI
# results do not change with a new govulncheck release.
vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
