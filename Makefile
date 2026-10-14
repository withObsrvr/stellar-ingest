.PHONY: all build build-compat build-all build-offline run clean test test-race test-coverage \
        deps vendor fmt vet lint install parity-baseline golden golden-update \
        parity-export parity-live parity-report parity-matrix parity-baseline-update \
        nix-build nix-run nix-shell nix-check nix-docker \
        docker-build docker-buildx docker-push docker-run docker-shell docker-clean \
        ci-build release help

# ---- Variables --------------------------------------------------------------
#
# NOTE: stellar-ingest is a CLI BATCH TOOL, not a long-running service.
# `make run` passes ARGS=... through to the binary; there is no HEALTH_PORT.
# Example: make run ARGS="--start 1 --end 1000 --output /tmp/out ..."
#
# Two binaries are produced from the same package:
#   stellar-ingest          primary executable
#   stellar-history-loader  transitional compatibility name (identical flags)
# See docs/compatibility.md for the deprecation plan.

BINARY_NAME  := stellar-ingest
COMPAT_NAME  := stellar-history-loader

DOCKER_ORG       := withobsrvr
DOCKER_IMAGE     := $(DOCKER_ORG)/$(BINARY_NAME)
GIT_SHA          := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_TIMESTAMP  := $(shell date -u +%Y%m%d%H%M%S)
BUILD_DATE       := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VERSION_TAG      := $(GIT_SHA)-$(BUILD_TIMESTAMP)
DOCKER_TAG       ?= latest
DOCKER_PLATFORM  ?= linux/amd64

# DuckDB — CGO required. duckdb-go v2 bundles a newer DuckDB than the other
# Obsrvr services, which is why this module stayed outside ttp-processor-demo's
# go.work. GOWORK=off is retained here so builds behave identically whether or
# not a go.work exists above this directory.
GOCMD      := go
GOENV      := GOWORK=off CGO_ENABLED=1
GOBUILD    := $(GOENV) $(GOCMD) build
GOTEST     := $(GOENV) $(GOCMD) test
GOCLEAN    := $(GOCMD) clean
LDFLAGS    := -X main.Version=$(GIT_SHA)

all: build-all

# ---- Build ------------------------------------------------------------------

build:
	@echo "→ building $(BINARY_NAME) (sha: $(GIT_SHA))"
	@mkdir -p bin
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) → bin/$(BINARY_NAME)"

# Transitional: same package, legacy executable name. Existing operational
# scripts keep working unchanged during the migration.
build-compat:
	@mkdir -p bin
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(COMPAT_NAME) .
	@echo "✓ $(COMPAT_NAME) (compatibility name) → bin/$(COMPAT_NAME)"

build-all: build build-compat

build-offline:
	@mkdir -p bin
	GO111MODULE=on GOPROXY=off $(GOBUILD) -mod=vendor -ldflags "$(LDFLAGS)" -o bin/$(BINARY_NAME) .
	@echo "✓ $(BINARY_NAME) (offline/vendored) → bin/$(BINARY_NAME)"

# ---- Run --------------------------------------------------------------------

# Pass arguments via ARGS; e.g.
#   make run ARGS="--start 1 --end 1000 --storage-type FS --output /tmp/out"
run: build
	./bin/$(BINARY_NAME) $(ARGS)

# ---- Quality ----------------------------------------------------------------

test:
	$(GOTEST) -v ./...

test-race:
	$(GOTEST) -race -v ./...

test-coverage:
	$(GOTEST) -coverprofile=coverage.out -covermode=atomic ./...
	$(GOCMD) tool cover -func=coverage.out

# Regenerate the recorded CLI surface and diff it against the committed
# baseline. A non-empty diff means the flag contract changed — intentional
# changes must update docs/baseline/cli-help.txt in the same commit.
parity-baseline: build
	@./bin/$(BINARY_NAME) --help > /tmp/stellar-ingest-help.txt 2>&1 || true
	@sed -e 's|^Usage of .*:$$|Usage of stellar-ingest:|' /tmp/stellar-ingest-help.txt > /tmp/stellar-ingest-help.norm
	@diff -u docs/baseline/cli-help.txt /tmp/stellar-ingest-help.norm \
	  && echo "✓ CLI surface matches committed baseline" \
	  || { echo "✗ CLI surface drifted from docs/baseline/cli-help.txt"; exit 1; }

# Golden Bronze snapshot suite. Hermetic: fixtures are committed, no network
# or credentials required.
golden:
	$(GOTEST) -run 'TestGolden|TestFixtures|TestExtractionIsDeterministic|TestXDRStreamSource|Invariance|TestRerunCompleteRange|TestCorruptedXDR' -v ./...

# Regenerate goldens. Review the diff — never regenerate to make a failing
# test pass.
golden-update:
	$(GOTEST) -run TestGoldenBronzeOutputs -update ./...
	@echo "✓ goldens regenerated — review the diff before committing"

# ---- Ticket 04 Bronze parity ------------------------------------------------
#
# Compares this repository's Bronze mapping against the live
# stellar-postgres-ingester over identical authoritative XDR. The two pin
# different stellar-extract versions and cannot be linked into one binary, so
# each side exports canonical rows from its own process and the comparison
# runs over the artifacts. See docs/ticket-04-progress.md.
#
# Full run:
#   make parity-export && make parity-live && make parity-report
#
# PARITY_DIR holds the artifacts; the row sets are build output, not fixtures.
PARITY_DIR  ?= build/parity
LIVE_INGESTER ?= ../ttp-processor-demo/obsrvr-lake/stellar-postgres-ingester/go

# Historical side: decompress fixtures, extract twice (the second pass is the
# non-determinism control), write digests and canonical rows.
parity-export:
	PARITY_OUT=$(PARITY_DIR) $(GOTEST) -run TestParityExportHistorical -timeout 30m -v .

# Live side: runs in the sibling repository against the XDR just exported.
parity-live:
	@test -d $(LIVE_INGESTER) || { \
	  echo "✗ live ingester not found at $(LIVE_INGESTER); set LIVE_INGESTER=<path>"; exit 1; }
	cd $(LIVE_INGESTER) && PARITY_DIR=$(abspath $(PARITY_DIR)) go test -run TestParityExportLive -timeout 30m -v .

# Comparison over both sides' artifacts, including the row-level drill-down.
parity-report:
	PARITY_OUT=$(PARITY_DIR) $(GOTEST) -run TestParityCompare -timeout 30m -v .
	@echo "✓ report → $(PARITY_DIR)/report/summary.md"

# Regenerate the reviewed matrix from the committed digests. Hermetic.
parity-matrix:
	$(GOTEST) -run TestParityMatrixUpdate -update .
	@echo "✓ docs/ticket-04-parity-matrix.md regenerated — review the diff"

# Promote a fresh export to the committed baselines. Never run this to make a
# failing regression test pass: the baselines are the reviewed record of what
# each side produced at a pinned revision.
parity-baseline-update: parity-export
	cp $(PARITY_DIR)/historical/*.digest.json testdata/parity/historical/
	@test -d $(PARITY_DIR)/live && cp $(PARITY_DIR)/live/*.digest.json testdata/parity/live/ \
	  || echo "note: no live digests in $(PARITY_DIR); run make parity-live first to refresh that side"
	$(MAKE) parity-matrix
	@echo "✓ baselines updated — review the diff before committing"

fmt:
	$(GOCMD) fmt ./...

vet:
	$(GOENV) $(GOCMD) vet ./...

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
	  echo "golangci-lint not installed; install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	  exit 1; }
	golangci-lint run ./...

deps:
	$(GOENV) $(GOCMD) mod download
	$(GOENV) $(GOCMD) mod tidy

vendor:
	$(GOENV) $(GOCMD) mod tidy && $(GOENV) $(GOCMD) mod vendor
	@echo "✓ vendored"

install:
	$(GOENV) $(GOCMD) install .

clean:
	rm -rf bin/
	rm -f coverage.out
	$(GOCLEAN)

# ---- Nix --------------------------------------------------------------------
#
# Flakes only see git-tracked files: a new or edited file must be at least
# `git add`-ed before nix will build it, or you get a confusing
# "not tracked by Git" error.

nix-build:
	nix build .#stellar-ingest

nix-run:
	nix run . -- $(ARGS)

nix-shell:
	nix develop

# Builds the package, which runs the hermetic test suite in the sandbox.
nix-check:
	nix flake check

nix-docker:
	nix build .#docker
	@echo "✓ image tarball at ./result — load with: docker load < result"

# ---- Docker -----------------------------------------------------------------

docker-build:
	@echo "→ building $(DOCKER_IMAGE)"
	@echo "  tags    : $(DOCKER_TAG), $(VERSION_TAG)"
	docker build \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):$(VERSION_TAG) \
		--build-arg VERSION=$(GIT_SHA) \
		--label org.opencontainers.image.version=$(VERSION_TAG) \
		--label org.opencontainers.image.revision=$(GIT_SHA) \
		--label org.opencontainers.image.created=$(BUILD_DATE) \
		--label org.opencontainers.image.source=https://github.com/withObsrvr/stellar-ingest \
		.
	@echo "✓ built $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

docker-buildx:
	docker buildx build --platform $(DOCKER_PLATFORM) \
		--build-arg VERSION=$(GIT_SHA) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):$(VERSION_TAG) .

docker-push: docker-build
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "✓ pushed $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

# Batch-tool docker-run: pass ARGS through, e.g.
#   make docker-run ARGS="--start 1 --end 1000 --output /out ..."
docker-run:
	docker run --rm -it $(DOCKER_IMAGE):$(DOCKER_TAG) $(ARGS)

docker-shell:
	docker run --rm -it --entrypoint bash $(DOCKER_IMAGE):$(DOCKER_TAG)

docker-clean:
	-docker rmi $(DOCKER_IMAGE):$(DOCKER_TAG) 2>/dev/null
	-docker rmi $(DOCKER_IMAGE):$(VERSION_TAG) 2>/dev/null

# ---- CI / Release -----------------------------------------------------------

ci-build: deps vet build-all test parity-baseline golden

release: docker-build docker-push
	@echo "✓ released $(DOCKER_IMAGE):$(VERSION_TAG)"

# ---- Help -------------------------------------------------------------------

help:
	@echo "stellar-ingest — Makefile targets"
	@echo ""
	@echo "NOTE: CLI BATCH TOOL (not a long-running service). Pass flags via ARGS:"
	@echo "  make run ARGS=\"--start 1 --end 1000 --output /tmp/out --storage-type FS\""
	@echo ""
	@echo "Build (CGO_ENABLED=1 for DuckDB; GOWORK=off):"
	@echo "  build          $(BINARY_NAME)"
	@echo "  build-compat   $(COMPAT_NAME) (transitional legacy name)"
	@echo "  build-all      both (default)"
	@echo "  build-offline  vendored/offline build"
	@echo ""
	@echo "Run:"
	@echo "  run ARGS=\"...\"                         Pass flags through"
	@echo ""
	@echo "Quality:"
	@echo "  test / test-race / test-coverage / fmt / vet / lint"
	@echo "  parity-baseline                        Diff CLI surface vs committed baseline"
	@echo "  golden / golden-update                 Bronze snapshot suite (hermetic)"
	@echo ""
	@echo "Ticket 04 Bronze parity (historical vs live ingester):"
	@echo "  parity-export                          Extract fixtures through this repo's mapping"
	@echo "  parity-live                            Extract the same XDR in the live ingester"
	@echo "  parity-report                          Compare both sides; writes summary.md"
	@echo "  parity-matrix                          Regenerate docs matrix from committed digests"
	@echo "  parity-baseline-update                 Promote a fresh export to the baselines"
	@echo "  deps / vendor / install / clean"
	@echo ""
	@echo "Docker (image = $(DOCKER_IMAGE)):"
	@echo "  docker-build / docker-buildx / docker-push"
	@echo "  docker-run ARGS=\"...\"                   Pass flags through"
	@echo "  docker-shell / docker-clean"
	@echo ""
	@echo ""
	@echo "Nix (flakes only see git-tracked files — git add new files first):"
	@echo "  nix-build / nix-run ARGS=\"...\" / nix-shell / nix-check / nix-docker"
	@echo ""
	@echo "CI / Release: ci-build / release"
