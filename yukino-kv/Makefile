GO      ?= go
BIN_DIR := bin

.PHONY: all build test race vet fmt tidy cluster clean

all: build

build:
	$(GO) build -o $(BIN_DIR)/kvserver ./cmd/kvserver
	$(GO) build -o $(BIN_DIR)/clerk ./cmd/clerk

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .

tidy:
	$(GO) mod tidy

# Bootstrap a local 3-node cluster (Ctrl-C to stop).
cluster: build
	$(BIN_DIR)/kvserver -n 3 -f test.conf -datadir ./data

clean:
	rm -rf $(BIN_DIR) data test.conf raftstatePersist*.txt snapshotPersist*.txt
