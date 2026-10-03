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

# Input for `make sweep`. Override: make sweep SWEEP_FILE=data/raw/Year1/...
SWEEP_FILE ?= $(FIXTURE_INPUT)

# Age (hours) a `*.tmp` leftover under $(OUT) must reach before
# `make clean-tmp` deletes it. Matches the CLI default
# (--tmp-max-age-hours); the age gate protects a concurrent run.
TMP_MAX_AGE_HOURS ?= 24

# ---- Targets ---------------------------------------------------------------

.PHONY: help
help:
	@echo "iri-lake Makefile"
	@echo ""
	@echo "  make fmt            Run cargo fmt --all"
	@echo "  make clippy         Run cargo clippy --all-targets -- -D warnings"
	@echo "  make test           Run cargo test"
	@echo "  make bench          Run cargo bench (parser-only, slow)"
	@echo "  make sweep          Sweep compression codecs on $(FIXTURE_INPUT)"
	@echo "  make build          Release build of the CLI"
	@echo "  make inventory      Walk $(IN) and print sales-file inventory"
	@echo "  make validate       Validate a single file ($(FIXTURE_INPUT))"
	@echo "  make ingest         Ingest a single file into $(FIXTURE_OUTPUT)"
	@echo "  make ingest-all     Walk $(IN) and ingest every eligible sales file"
	@echo "  make clean          cargo clean + remove criterion/ and target/"
	@echo "  make clean-tmp      Delete stale *.tmp leftovers under $(OUT) (older than $(TMP_MAX_AGE_HOURS)h)"
	@echo "  make readiness      Gated plan for the full 143 GB corpus run"
	@echo "  make gate2          Validate every discovered source before ingesting"
	@echo "  make gate5          Reconcile $(LAKE) against its manifest (DuckDB)"
	@echo "  make sql SQL=...    Run an arbitrary file from sql/ against $(LAKE)"

.PHONY: fmt
fmt:
	$(BIN) fmt --all

.PHONY: clippy
clippy:
	$(BIN) clippy --all-targets --all-features -- -D warnings

.PHONY: test
test:
	$(BIN) test --release --all-features

.PHONY: bench
bench:
	$(BIN) bench --bench parse_sales -- --warm-up-time 2 --measurement-time 5

.PHONY: sweep
sweep:
	$(BIN) run --release --example compression_sweep -- $(SWEEP_FILE)

.PHONY: build
build:
	$(BIN) build --release --locked

.PHONY: inventory
inventory:
	$(BIN) run --release -- inventory --input $(IN)

# Gate 2: validate every discovered source (header + every record's
# alignment). Writes nothing. Exits non-zero on any failure.
.PHONY: gate2
gate2:
	./scripts/gate2_validate.sh

.PHONY: validate
validate:
	$(BIN) run --release -- validate $(FIXTURE_INPUT)

.PHONY: ingest
ingest:
	$(BIN) run --release -- ingest $(FIXTURE_INPUT) --output-root $(OUT)

.PHONY: ingest-all
ingest-all:
	$(BIN) run --release -- ingest-all --input $(IN) --output-root $(OUT)

.PHONY: clean
clean:
	$(BIN) clean
	rm -rf criterion/ benches/target/

.PHONY: clean-tmp
clean-tmp:
	@find $(OUT) -type f -name '*.tmp' -mmin +$$(( $(TMP_MAX_AGE_HOURS) * 60 )) -print -delete 2>/dev/null; \
		echo "clean-tmp: swept $(OUT) for *.tmp older than $(TMP_MAX_AGE_HOURS)h"

# ---- SQL / analytics layer ---------------------------------------------------
#
# DuckDB reads the Parquet lake; nothing in the Rust build depends on it.
# `uv run` syncs .venv from uv.lock on demand, so there is no separate
# install step.

LAKE ?= $(OUT)
SQL  ?= sql/gate5_reconciliation.sql

.PHONY: sql
sql:
	uv run python scripts/run_sql.py $(SQL) --lake $(LAKE)

# Gate 5 of docs/corpus_readiness.md. Exits non-zero if any check fails,
# so it is usable from a script or CI, not just for reading.
.PHONY: gate5
gate5:
	uv run python scripts/run_sql.py sql/gate5_reconciliation.sql --lake $(LAKE)

# ---- Full-corpus run -------------------------------------------------------

.PHONY: readiness
readiness:
	@echo "See docs/corpus_readiness.md for the gated full-corpus plan."

# ---- Convenience for tracing ----------------------------------------------

RUST_LOG ?= iri_lake=info,info
export RUST_LOG
