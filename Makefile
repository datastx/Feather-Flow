.PHONY: build build-release test lint fmt check doc clean ci build-target checksums \
        ff-parse ff-parse-json ff-parse-deps ff-compile ff-compile-verbose \
        ff-run ff-run-full-refresh ff-run-select ff-ls ff-ls-json ff-ls-tree \
        ff-test ff-test-verbose ff-test-fail-fast ff-seed ff-seed-full-refresh \
        ff-docs ff-docs-json ff-validate ff-validate-strict ff-sources ff-help \
        dev-cycle dev-validate dev-fresh help

# =============================================================================
# Configuration
# =============================================================================

# Default project for testing CLI commands
PROJECT_DIR ?= tests/fixtures/sample_project

# =============================================================================
# Development
# =============================================================================

build:
	cargo build --workspace

build-release:
	cargo build --workspace --release

run:
	cargo run -p ff-cli --

watch:
	cargo watch -x 'build --workspace'

# =============================================================================
# Rust Testing
# =============================================================================

test:
	cargo test --workspace --all-features

test-verbose:
	cargo test --workspace -- --nocapture

test-integration:
	cargo test --test '*' -- --test-threads=1

test-unit:
	cargo test --workspace --lib

# =============================================================================
# Code Quality
# =============================================================================

lint: fmt-check clippy

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

check:
	cargo check --workspace --all-targets

# =============================================================================
# Documentation
# =============================================================================

doc:
	cargo doc --workspace --no-deps

doc-open:
	cargo doc --workspace --no-deps --open

# =============================================================================
# CLI Commands - Featherflow (ff)
# Each CLI subcommand has a dedicated make target for easy testing
# =============================================================================

## ff parse - Parse SQL files and output AST/dependencies
ff-parse:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse

ff-parse-json:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse --output json

ff-parse-deps:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse --output deps

## ff compile - Render Jinja templates to SQL, extract dependencies
ff-compile:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile

ff-compile-verbose:
	cargo run -p ff-cli -- --verbose --project-dir $(PROJECT_DIR) compile

## ff run - Execute compiled SQL against database
ff-run:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run

ff-run-full-refresh:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --full-refresh

ff-run-select:
	@echo "Usage: make ff-run-select MODELS='+model_name'"
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --select $(MODELS)

## ff ls - List models with dependencies and materialization
ff-ls:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls

ff-ls-json:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output json

ff-ls-tree:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output tree

## ff test - Run schema tests (unique, not_null, etc.)
ff-test:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) test

ff-test-verbose:
	cargo run -p ff-cli -- --verbose --project-dir $(PROJECT_DIR) test

ff-test-fail-fast:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) test --fail-fast

## ff seed - Load CSV seed files into database
ff-seed:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) seed

ff-seed-full-refresh:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) seed --full-refresh

## ff docs - Generate documentation from schema files
ff-docs:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs

ff-docs-json:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs --format json

## ff validate - Validate project without execution
ff-validate:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) validate

ff-validate-strict:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) validate --strict

## ff sources - List sources (future: ff sources command)
ff-sources:
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output json | jq '.[] | select(.type == "source")'

## ff help - Show CLI help
ff-help:
	cargo run -p ff-cli -- --help

# =============================================================================
# Workflows - Common development workflows
# =============================================================================

## Full development cycle: seed -> run -> test
dev-cycle: ff-seed ff-run ff-test
	@echo "Development cycle complete!"

## Quick validation: compile -> validate
dev-validate: ff-compile ff-validate
	@echo "Validation complete!"

## Full pipeline with fresh data
dev-fresh: ff-seed-full-refresh ff-run-full-refresh ff-test
	@echo "Fresh pipeline complete!"

# =============================================================================
# Maintenance
# =============================================================================

clean:
	cargo clean
	rm -rf $(PROJECT_DIR)/target

update:
	cargo update

# =============================================================================
# CI (local verification)
# =============================================================================

ci: fmt-check clippy test doc
	@echo "CI checks passed!"

ci-quick: check fmt-check clippy
	@echo "Quick CI checks passed!"

ci-full: ci ff-compile ff-validate
	@echo "Full CI checks passed!"

# =============================================================================
# Release
# =============================================================================

install:
	cargo install --path crates/ff-cli

# Build release binary for a specific target (used in CI)
# Usage: make build-target TARGET=x86_64-unknown-linux-gnu
build-target:
	cargo build --release --target $(TARGET) -p ff-cli

# Create checksums for release artifacts (used in CI)
# Usage: make checksums ARTIFACTS_DIR=artifacts
checksums:
	cd $(ARTIFACTS_DIR) && \
	for dir in */; do \
		cd "$$dir" && \
		for file in *; do \
			sha256sum "$$file" > "$${file}.sha256"; \
		done && \
		cd ..; \
	done

# =============================================================================
# Claude Code
# =============================================================================

claude-auto-run:
	claude --dangerously-skip-permissions

# =============================================================================
# Help
# =============================================================================

help:
	@echo "Featherflow Development Makefile"
	@echo ""
	@echo "Development:"
	@echo "  make build              Build all crates"
	@echo "  make build-release      Build release binaries"
	@echo "  make watch              Watch and rebuild on changes"
	@echo ""
	@echo "Testing:"
	@echo "  make test               Run all tests"
	@echo "  make test-verbose       Run tests with output"
	@echo "  make test-integration   Run integration tests only"
	@echo "  make test-unit          Run unit tests only"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint               Run fmt-check and clippy"
	@echo "  make fmt                Format code"
	@echo "  make clippy             Run clippy linter"
	@echo ""
	@echo "CLI Commands (use PROJECT_DIR=path to override):"
	@echo "  make ff-parse           Parse SQL files"
	@echo "  make ff-parse-json      Parse with JSON output"
	@echo "  make ff-parse-deps      Parse with deps output"
	@echo "  make ff-compile         Compile Jinja to SQL"
	@echo "  make ff-run             Execute models"
	@echo "  make ff-run-full-refresh  Execute with full refresh"
	@echo "  make ff-ls              List models"
	@echo "  make ff-ls-json         List models as JSON"
	@echo "  make ff-ls-tree         List models as tree"
	@echo "  make ff-test            Run schema tests"
	@echo "  make ff-seed            Load seed data"
	@echo "  make ff-seed-full-refresh  Load seeds with full refresh"
	@echo "  make ff-docs            Generate documentation"
	@echo "  make ff-validate        Validate project"
	@echo "  make ff-validate-strict Validate with strict mode"
	@echo "  make ff-sources         List sources (JSON filtered)"
	@echo ""
	@echo "Workflows:"
	@echo "  make dev-cycle          seed -> run -> test"
	@echo "  make dev-validate       compile -> validate"
	@echo "  make dev-fresh          Full refresh pipeline"
	@echo ""
	@echo "CI:"
	@echo "  make ci                 Full CI check"
	@echo "  make ci-quick           Quick CI check"
	@echo "  make ci-full            CI + compile + validate"
