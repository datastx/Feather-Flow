---
name: test-model
description: Test a model implementation following project conventions
---

Feather-Flow is a schema validation framework — see **[HOW_FEATHERFLOW_WORKS.md](../../HOW_FEATHERFLOW_WORKS.md)** for the full architecture. Testing a model means validating that it compiles against its declared schema, passes static analysis, and executes correctly.

When testing a model:

1. First, verify the model SQL parses correctly:
   ```bash
   make ff-parse
   ```

2. Compile the model to check Jinja rendering:
   ```bash
   make ff-compile
   ```

3. Run the full development cycle (seed -> run -> test):
   ```bash
   make dev-cycle
   ```

4. If tests fail, check the test output for sample failing rows

5. Validate the project structure:
   ```bash
   make ff-validate
   ```

Always verify:
- SQL syntax is valid for the configured dialect
- Dependencies are correctly extracted from the AST
- Schema tests (unique, not_null) pass
- Model materializes as expected (view or table)
