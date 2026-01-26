.PHONY: build build-release test lint fmt check doc clean ci

# Development
build:
	cargo build --workspace

build-release:
	cargo build --workspace --release

run:
	cargo run -p ff-cli --

watch:
	cargo watch -x 'build --workspace'

# Testing
test:
	cargo test --workspace --all-features

test-verbose:
	cargo test --workspace -- --nocapture

test-integration:
	cargo test --test '*' -- --test-threads=1

# Code Quality
lint: fmt-check clippy

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

check:
	cargo check --workspace --all-targets

# Documentation
doc:
	cargo doc --workspace --no-deps

doc-open:
	cargo doc --workspace --no-deps --open

# Maintenance
clean:
	cargo clean

update:
	cargo update

# CI (local verification)
ci: fmt-check clippy test doc
	@echo "CI checks passed!"

ci-quick: check fmt-check clippy
	@echo "Quick CI checks passed!"

# Release
install:
	cargo install --path crates/ff-cli


claude-auto-run:
	claude --dangerously-skip-permissions