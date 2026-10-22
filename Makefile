VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG     := github.com/10dian-ai/newapi-channel-synchronization/internal/version
LDFLAGS := -s -w -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).Date=$(DATE)

.PHONY: build test itest vet fmt docker clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/newapi-sync ./cmd/newapi-sync

test:
	go test -race ./...

# 需要本地 MySQL / PostgreSQL，见 internal/itest
itest:
	go test -count=1 -v ./internal/itest/

vet:
	go vet ./...

fmt:
	gofmt -w .

docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) \
		-t ghcr.io/10dian-ai/newapi-channel-synchronization:$(VERSION) .

clean:
	rm -rf bin dist
