#!/bin/bash
# verify-release.sh - Verify the build is ready for release
# Run this script before creating a new release tag

set -e

echo "=== Featherflow Release Verification ==="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

pass() {
    echo -e "${GREEN}✓${NC} $1"
}

fail() {
    echo -e "${RED}✗${NC} $1"
    exit 1
}

# 1. Build check
echo "Step 1: Build verification"
cargo build --workspace || fail "Build failed"
pass "Debug build succeeded"

cargo build --workspace --release || fail "Release build failed"
pass "Release build succeeded"

# 2. Lint check
echo ""
echo "Step 2: Lint verification"
cargo fmt --all -- --check || fail "Code formatting check failed"
pass "Code formatting OK"

cargo clippy --workspace --all-targets -- -D warnings || fail "Clippy check failed"
pass "Clippy check OK"

# 3. Test check
echo ""
echo "Step 3: Test verification"
cargo test --workspace --all-features || fail "Tests failed"
pass "All tests passed"

# 4. Documentation check
echo ""
echo "Step 4: Documentation verification"
cargo doc --workspace --no-deps || fail "Documentation build failed"
pass "Documentation built successfully"

# 5. CLI verification against sample project
echo ""
echo "Step 5: CLI verification"
PROJECT_DIR="tests/fixtures/sample_project"

# Parse
cargo run -p ff-cli -- --project-dir "$PROJECT_DIR" parse > /dev/null 2>&1 || fail "ff parse failed"
pass "ff parse works"

# Compile
cargo run -p ff-cli -- --project-dir "$PROJECT_DIR" compile > /dev/null 2>&1 || fail "ff compile failed"
pass "ff compile works"

# List
cargo run -p ff-cli -- --project-dir "$PROJECT_DIR" ls > /dev/null 2>&1 || fail "ff ls failed"
pass "ff ls works"

# Validate
cargo run -p ff-cli -- --project-dir "$PROJECT_DIR" validate > /dev/null 2>&1 || fail "ff validate failed"
pass "ff validate works"

# Seed
cargo run -p ff-cli -- --project-dir "$PROJECT_DIR" seed > /dev/null 2>&1 || fail "ff seed failed"
pass "ff seed works"

# Docs
cargo run -p ff-cli -- --project-dir "$PROJECT_DIR" docs > /dev/null 2>&1 || fail "ff docs failed"
pass "ff docs works"

# 6. Version check
echo ""
echo "Step 6: Version verification"
VERSION=$(grep "^version" Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')
echo "Current version: $VERSION"

# Summary
echo ""
echo "=== Release Verification Complete ==="
echo -e "${GREEN}All checks passed!${NC}"
echo ""
echo "Next steps:"
echo "  1. Update CHANGELOG.md"
echo "  2. Create release tag: git tag v$VERSION"
echo "  3. Push tag: git push origin v$VERSION"
