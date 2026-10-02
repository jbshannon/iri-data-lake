SHELL := /usr/bin/env bash
BIN   := cargo
PKG   := iri-lake

# ---- Configuration --------------------------------------------------------

# Default input root (symlinked at data/raw/). Override: make ingest-all IN=data/raw
IN      ?= data/raw
OUT     ?= data/lake

# Run an example with a small fixture file from Year1 (beer_drug is small).
FIXTURE_INPUT ?= data/raw/Year1/beer/beer_drug_1114_1165
FIXTURE_OUTPUT := $(OUT)/bronze/iri_sales/year=1/category=beer/channel=drug

# ---- Targets ---------------------------------------------------------------

.PHONY: help
help:
	@echo "iri-lake Makefile"
	@echo ""
	@echo "  make fmt            Run cargo fmt --all"
	@echo "  make clippy         Run cargo clippy --all-targets -- -D warnings"
	@echo "  make test           Run cargo test"
	@echo "  make bench          Run cargo bench (parser-only, slow)"
	@echo "  make build          Release build of the CLI"
	@echo "  make inventory      Walk $(IN) and print sales-file inventory"
	@echo "  make validate       Validate a single file ($(FIXTURE_INPUT))"
	@echo "  make ingest         Ingest a single file into $(FIXTURE_OUTPUT)"
	@echo "  make ingest-all     Walk $(IN) and ingest every eligible sales file"
	@echo "  make clean          cargo clean + remove criterion/ and target/"
	@echo "  make fixtures       Re-emit test fixtures into tests/fixtures/"
	@echo "  make readiness      Gated plan for the full 143 GB corpus run"

.PHONY: fmt
fmt:
	$(BIN) fmt --all

.PHONY: clippy
clippy:
	$(BIN) clippy --all-targets --all-features -- -D warnings

.PHONY: test
test:
	$(BIN) test --all-features

.PHONY: bench
bench:
	$(BIN) bench --bench parse_sales -- --warm-up-time 2 --measurement-time 5

.PHONY: build
build:
	$(BIN) build --release --locked

.PHONY: inventory
inventory:
	$(BIN) run -- inventory --input $(IN)

.PHONY: validate
validate:
	$(BIN) run -- validate $(FIXTURE_INPUT)

.PHONY: ingest
ingest:
	$(BIN) run -- ingest $(FIXTURE_INPUT) --output-root $(OUT)

.PHONY: ingest-all
ingest-all:
	$(BIN) run -- ingest-all --input $(IN) --output-root $(OUT)

.PHONY: clean
clean:
	$(BIN) clean
	rm -rf criterion/ benches/target/

.PHONY: fixtures
fixtures:
	$(BIN) test --test fixtures_emit -- --nocapture

# ---- Full-corpus run -------------------------------------------------------

.PHONY: readiness
readiness:
	@echo "See docs/corpus_readiness.md for the gated full-corpus plan."

# ---- Convenience for tracing ----------------------------------------------

RUST_LOG ?= iri_lake=info,info
export RUST_LOG
