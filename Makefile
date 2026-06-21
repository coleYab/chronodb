.PHONY: all help server agent test test-short test-verbose test-race \
        test-pkg vet lint fmt clean run run-agent install

GO         := go
GOFLAGS    :=
BINDIR     := build

BINARY_SERVER := chronodb
BINARY_AGENT  := chrono-agent
SERVER_PKG    := ./cmd/chronodb
AGENT_PKG     := ./cmd/chrono-agent

TEST_FLAGS     := -race -count=1 -timeout=120s
TEST_SHORT     := -count=1 -timeout=60s
TEST_VERBOSE   := -race -count=1 -timeout=120s -v
TEST_RACE      := -race -count=3 -timeout=120s

default: help

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Build:"
	@echo "  all              Build both binaries (server + agent)"
	@echo "  server           Build chronodb server binary ($(BINDIR)/$(BINARY_SERVER))"
	@echo "  agent            Build chrono-agent binary ($(BINDIR)/$(BINARY_AGENT))"
	@echo ""
	@echo "Test:"
	@echo "  test             Run all tests with race detector"
	@echo "  test-short       Quick test run (no race)"
	@echo "  test-verbose     Verbose test run with race detector"
	@echo "  test-race        Full race detection (3 counts)"
	@echo "  test-pkg PKG=X   Test a specific package (e.g. make test-pkg PKG=./internal/batcher)"
	@echo ""
	@echo "Quality:"
	@echo "  vet              Run go vet"
	@echo "  lint             Run golangci-lint"
	@echo "  fmt              Run go fmt"
	@echo ""
	@echo "Run:"
	@echo "  run              Run chronodb server (from binary)"
	@echo "  run-agent        Run chrono-agent (from binary)"
	@echo ""
	@echo "Housekeeping:"
	@echo "  clean            Remove build artifacts and data directory"
	@echo "  install          go install both binaries"

all: $(BINDIR)/$(BINARY_SERVER) $(BINDIR)/$(BINARY_AGENT)

server: $(BINDIR)/$(BINARY_SERVER)

agent: $(BINDIR)/$(BINARY_AGENT)

$(BINDIR):
	@mkdir -p $@

$(BINDIR)/$(BINARY_SERVER): $(BINDIR)
	$(GO) build $(GOFLAGS) -o $@ $(SERVER_PKG)

$(BINDIR)/$(BINARY_AGENT): $(BINDIR)
	$(GO) build $(GOFLAGS) -o $@ $(AGENT_PKG)

test:
	$(GO) test $(TEST_FLAGS) ./...

test-short:
	$(GO) test $(TEST_SHORT) ./...

test-verbose:
	$(GO) test $(TEST_VERBOSE) ./...

test-race:
	$(GO) test $(TEST_RACE) ./...

test-pkg:
	$(GO) test $(TEST_FLAGS) $(PKG)

vet:
	$(GO) vet ./...

lint:
	golangci-lint run ./...

fmt:
	$(GO) fmt ./...

run: $(BINDIR)/$(BINARY_SERVER)
	@echo "Starting chronodb server..."
	./$(BINDIR)/$(BINARY_SERVER)

run-agent: $(BINDIR)/$(BINARY_AGENT)
	@echo "Starting chrono-agent (requires chrono-agent.yaml)..."
	./$(BINDIR)/$(BINARY_AGENT) -config chrono-agent.yaml

clean:
	rm -rf $(BINDIR) data/

install:
	$(GO) install $(GOFLAGS) $(SERVER_PKG)
	$(GO) install $(GOFLAGS) $(AGENT_PKG)
