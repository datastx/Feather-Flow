---
name: add-feature
description: Add a new feature following project conventions
---

When adding a new feature:

1. First, read the relevant existing code to understand patterns
2. Create a plan listing files to modify/create
3. Implement with proper error handling (thiserror for libs)
4. Add unit tests in the same file
5. Run `make lint` and fix any issues
6. Run `make test` to verify

Always check existing similar code first for patterns.

## Code Style Guidelines

- Use `?` for error propagation, add `.context()` at boundaries
- Prefer `impl Trait` over `Box<dyn Trait>` where possible
- All public items need rustdoc comments
- No unwrap() except in tests

## Testing Requirements

- Unit tests: Add to the same file under `#[cfg(test)]` module
- Integration tests: Add to `tests/integration_tests.rs`
- Test fixtures: Use `tests/fixtures/sample_project/`

## Verification Commands

```bash
make fmt        # Format code
make clippy     # Run linter
make test       # Run all tests
make ci         # Full CI check
```
