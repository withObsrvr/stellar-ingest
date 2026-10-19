.PHONY: all build build-compat build-all build-offline run clean test test-race test-coverage \
        deps vendor fmt vet lint install parity-baseline golden golden-update \
        parity-export parity-live parity-report parity-matrix parity-baseline-update \
        nix-build nix-run nix-shell nix-check nix-docker nix-vendor-hash \
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

# Build with whichever container CLI the host actually has. A hardcoded
# `docker` fails outright on a podman host.
CONTAINER_CLI    ?= $(shell docker info >/dev/null 2>&1 && echo docker || \
                      { command -v podman >/dev/null 2>&1 && echo podman; } || echo docker)

# Push destination. podman resolves an unqualified local tag to
# localhost/<name>, so `podman push withobsrvr/x:t` tries to reach a registry
# on localhost and fails; docker treats the same string as Docker Hub.
# Qualifying the destination makes both CLIs do the same thing.
REGISTRY         ?= docker.io

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

# Run the DuckLake integration tests against a throwaway PostgreSQL catalog
# and a local data directory. These are the only tests that exercise the real
# path the re-ingest runs: Parquet -> read_parquet -> a BLOB column -> a view
# that renders lower(hex(..)) and joins a parent. Everything else about the
# compact-identity change is covered without a database.
#
# Needs podman or docker for the catalog. The DuckDB extension directory must
# match the version the Go driver links (1.5.4), which is what
# $(HOME)/.duckdb/extensions holds once the duckdb CLI has installed ducklake,
# httpfs and postgres_scanner.
INTEGRATION_PG_PORT ?= 55432
INTEGRATION_PG_NAME ?= ingest-catalog
INTEGRATION_DATA_PATH ?= $(CURDIR)/build/lakedata/
CONTAINER_CLI ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)

integration-up:
	@test -n "$(CONTAINER_CLI)" || { echo "need podman or docker"; exit 1; }
	-$(CONTAINER_CLI) rm -f $(INTEGRATION_PG_NAME) >/dev/null 2>&1
	$(CONTAINER_CLI) run -d --name $(INTEGRATION_PG_NAME) \
	  -e POSTGRES_PASSWORD=ingest -e POSTGRES_USER=ingest -e POSTGRES_DB=ducklake_catalog \
	  -p $(INTEGRATION_PG_PORT):5432 docker.io/library/postgres:16-alpine
	@printf 'waiting for postgres'
	@for i in $$(seq 1 60); do \
	  if $(CONTAINER_CLI) exec $(INTEGRATION_PG_NAME) pg_isready -U ingest -d ducklake_catalog >/dev/null 2>&1; then \
	    echo " ready"; exit 0; fi; printf '.'; sleep 1; done; \
	  echo " timed out"; exit 1

integration-down:
	-$(CONTAINER_CLI) rm -f $(INTEGRATION_PG_NAME)

# Assumes integration-up has run. Kept separate so the catalog can be reused
# across runs while iterating.
integration-ducklake:
	@mkdir -p $(INTEGRATION_DATA_PATH)
	DUCKLAKE_INGESTION_TEST_POSTGRES="postgresql://ingest:ingest@localhost:$(INTEGRATION_PG_PORT)/ducklake_catalog?sslmode=disable" \
	DUCKLAKE_INGESTION_TEST_EXTENSION_DIR="$(HOME)/.duckdb/extensions" \
	DUCKLAKE_INGESTION_TEST_DATA_PATH="$(INTEGRATION_DATA_PATH)" \
	$(GOTEST) -count=1 -v -run 'TestDuckLakeManifestIngestion|TestCompatViewsRenderIdentityFromRealDuckLake' .

integration: integration-up
	$(MAKE) integration-ducklake; status=$$?; $(MAKE) integration-down; exit $$status

test-race:
	$(GOTEST) -race -v ./...

test-coverage:
	$(GOTEST) -coverprofile=coverage.out -covermode=atomic ./...
	$(GOCMD) tool cover -func=coverage.out

# Regenerate the recorded CLI surface and diff it against the committed
# baseline. A non-empty diff means the flag contract changed — intentional
# changes must update docs/baseline/cli-help.txt in the same commit.
# Two values in --help depend on where it ran, not on the flag contract, so
# they are normalised before the diff:
#
#   Usage of <path>   the binary's path on disk
#   --workers default runtime.NumCPU() on the recording machine
#
# The workers default is the interesting one. It made this check pass on a
# 16-core developer machine and fail on a 4-core CI runner, for every commit
# since the baseline was recorded -- a drift report about the hardware rather
# than the code. Normalising it to <NumCPU> keeps the rest of the line under
# comparison and states the machine dependency in the baseline itself.
# Pinning the default instead is a behaviour change; see "Known gaps" item 4
# in docs/roadmap.md.
parity-baseline: build
	@./bin/$(BINARY_NAME) --help > /tmp/stellar-ingest-help.txt 2>&1 || true
	@sed -e 's|^Usage of .*:$$|Usage of stellar-ingest:|' \
	     -e 's|^\(\s*Number of parallel workers (default \)[0-9][0-9]*)$$|\1<NumCPU>)|' \
	     /tmp/stellar-ingest-help.txt > /tmp/stellar-ingest-help.norm
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

# Recompute flake.nix's vendorHash after a dependency change.
#
# buildGoModule pins the fetched module set by content hash, so editing
# go.mod or go.sum invalidates it and the nix build fails with a mismatch.
# Nothing in `make test` catches that — the Go toolchain does not use the
# flake — so CI is the first thing to notice unless this is run.
#
# Works by asking for a hash that cannot be right and reading the real one
# out of the resulting error.
nix-vendor-hash:
	@echo "→ recomputing vendorHash (this fetches the module set)"
	@sed -i 's|vendorHash = "sha256-[^"]*";|vendorHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";|' flake.nix
	@HASH=$$(nix build .#stellar-ingest 2>&1 | awk '/got:/ {print $$2}' | head -1); \
	  if [ -z "$$HASH" ]; then \
	    echo "✗ no hash reported — the build may have failed for another reason"; \
	    git checkout flake.nix; exit 1; \
	  fi; \
	  sed -i "s|vendorHash = \"sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\";|vendorHash = \"$$HASH\";|" flake.nix; \
	  echo "✓ vendorHash = $$HASH"
	@git diff --stat flake.nix

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
	$(CONTAINER_CLI) build \
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
	$(CONTAINER_CLI) buildx build --platform $(DOCKER_PLATFORM) \
		--build-arg VERSION=$(GIT_SHA) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) -t $(DOCKER_IMAGE):$(VERSION_TAG) .

docker-push: docker-build
	$(CONTAINER_CLI) tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(REGISTRY)/$(DOCKER_IMAGE):$(DOCKER_TAG)
	$(CONTAINER_CLI) tag $(DOCKER_IMAGE):$(VERSION_TAG) $(REGISTRY)/$(DOCKER_IMAGE):$(VERSION_TAG)
	$(CONTAINER_CLI) push $(REGISTRY)/$(DOCKER_IMAGE):$(DOCKER_TAG)
	$(CONTAINER_CLI) push $(REGISTRY)/$(DOCKER_IMAGE):$(VERSION_TAG)
	@echo "✓ pushed $(DOCKER_IMAGE):{$(DOCKER_TAG),$(VERSION_TAG)}"

# Batch-tool docker-run: pass ARGS through, e.g.
#   make docker-run ARGS="--start 1 --end 1000 --output /out ..."
docker-run:
	$(CONTAINER_CLI) run --rm -it $(DOCKER_IMAGE):$(DOCKER_TAG) $(ARGS)

docker-shell:
	$(CONTAINER_CLI) run --rm -it --entrypoint bash $(DOCKER_IMAGE):$(DOCKER_TAG)

docker-clean:
	-$(CONTAINER_CLI) rmi $(DOCKER_IMAGE):$(DOCKER_TAG) 2>/dev/null
	-$(CONTAINER_CLI) rmi $(DOCKER_IMAGE):$(VERSION_TAG) 2>/dev/null

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
	@echo "  nix-vendor-hash                        Recompute vendorHash after a go.mod change"
	@echo ""
	@echo "CI / Release: ci-build / release"
