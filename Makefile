GO ?= go
GOLANGCI_LINT_VERSION := $(shell cat .golangci-lint-version)
GOLANGCI_LINT = $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: default build test vet fmt lint

default: build test vet lint

build:
	$(GO) build ./...

# Full suite with verbose output, race detection, and coverage; Docker is required.
test:
	mkdir -p .out/coverage
	$(GO) test -v -race -count=1 -timeout=5m -covermode=atomic -coverpkg=./... -coverprofile=.out/coverage/coverage.out ./...
	$(GO) tool cover -func=.out/coverage/coverage.out > .out/coverage/summary.txt
	cat .out/coverage/summary.txt
	$(GO) tool cover -html=.out/coverage/coverage.out -o .out/coverage/coverage.html
	@awk '/^total:/ { print "Total coverage: " $$3 }' .out/coverage/summary.txt

vet:
	$(GO) vet ./...

# Apply the same formatter and import grouping that lint checks.
fmt:
	$(GOLANGCI_LINT) fmt --config .golangci.yml ./...

lint:
	$(GOLANGCI_LINT) config verify --config .golangci.yml
	$(GOLANGCI_LINT) run --config .golangci.yml ./...

