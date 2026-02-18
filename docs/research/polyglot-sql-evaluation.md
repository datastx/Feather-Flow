# Research: polyglot-sql as AST Backend for Feather-Flow

**Date:** 2026-02-18
**Repo:** https://github.com/tobilg/polyglot
**Crate:** `polyglot-sql` on crates.io / docs.rs

---

## Executive Summary

`polyglot-sql` is a Rust-native SQL transpiler inspired by Python's sqlglot. It supports 33 SQL dialects, can parse/generate/transpile SQL, and ships as a native Rust crate (no Python dependency). Its killer feature for Feather-Flow would be **dialect transpilation** — e.g., converting Snowflake SQL into DuckDB SQL automatically. However, adopting it would require a **significant rewrite** of `ff-sql` due to completely different AST types, and the library is **very new** (v0.1.x, released ~Feb 2026) with unclear production maturity.

---

## What polyglot-sql Offers

### Core Capabilities
| Capability | polyglot-sql | sqlparser-rs (current) |
|---|---|---|
| SQL parsing → AST | Yes (200+ expression types) | Yes (mature, well-tested) |
| AST → SQL generation | Yes (any target dialect) | Yes (via `Display` trait) |
| **Cross-dialect transpilation** | **Yes — 33 dialects** | **No** |
| AST visitor/traversal | `DfsIter`, `BfsIter`, `ExpressionWalk` | `visit_relations`, `visit_relations_mut` |
| Table extraction | `get_tables()`, `get_table_names()` | `visit_relations()` |
| Column extraction | `get_columns()`, `get_column_names()` | Manual AST walk (lineage.rs) |
| AST mutation | `rename_tables()`, `qualify_columns()`, `add_where()`, etc. | `visit_relations_mut()` |
| Column lineage | Built-in lineage module | Custom (lineage.rs, ~680 lines) |
| Scope analysis | `Scope`, `ScopeType`, `build_scope()` | Not available |
| Type annotation | `annotate_types()`, `TypeAnnotator` | Not available (uses DataFusion separately) |
| Schema resolution | `Schema` trait, `MappingSchema`, `Resolver` | Not available |
| Query builder | Fluent builder API (`select().from().where_()`) | Not available |
| Dialect count | 33 (Snowflake, DuckDB, BigQuery, Redshift, etc.) | ~20+ (per sqlparser docs) |
| Test suite | 9,159 tests (100% sqlglot fixture compliance) | Well-established, widely used |
| Maturity | **Very new (v0.1.x, Feb 2026)** | **Mature (v0.61, years of production use)** |

### Transpilation — The Key Feature
```rust
use polyglot_sql::{transpile, DialectType};

// Snowflake → DuckDB
let duckdb_sql = transpile(
    "SELECT IFNULL(col, 0), DATEADD(day, 7, created_at) FROM my_table",
    DialectType::Snowflake,
    DialectType::DuckDb,
)?;
// Automatically rewrites function names, syntax, etc.
```

This is something sqlparser-rs fundamentally cannot do — it parses dialect-specific SQL but has no concept of cross-dialect translation.

---

## Current Feather-Flow Integration Points (ff-sql)

### Files That Would Need Rewriting

| File | sqlparser-rs Usage | Effort |
|---|---|---|
| `parser.rs` | `Parser::parse_sql()`, `Statement` type | Medium — swap parser call |
| `dialect.rs` | `Dialect` trait, `Ident`, `ObjectName`, `ObjectNamePart` | High — completely different type system |
| `extractor.rs` | `visit_relations()`, `Query`, `Statement`, `With` | Medium — replace with `get_tables()` |
| `qualify.rs` | `visit_relations_mut()`, `ObjectName` mutation | Medium — replace with `rename_tables()` |
| `inline.rs` | `Parser`, `Statement`, `Cte`, `With`, `TableAlias`, AST construction | **High** — manual CTE injection via AST nodes |
| `lineage.rs` | `SelectItem`, `Expr`, `FunctionArg`, `TableFactor`, etc. (deep AST walk) | **High** — or replace with polyglot's built-in lineage |
| `validator.rs` | `Query`, `SetExpr`, `TableFactor` | Medium — different AST types |
| `lib.rs` | `ObjectName` helper | Low |

### What Gets Easier with polyglot-sql
- **Table extraction**: `get_tables()` / `get_table_names()` replaces custom `visit_relations` + CTE filtering
- **Table qualification**: `rename_tables()` / `qualify_columns()` replaces custom `visit_relations_mut` code
- **Column lineage**: Built-in lineage module could replace the 680-line `lineage.rs`
- **Scope analysis**: Built-in scoping could simplify dependency resolution

### What Gets Harder or Riskier
- **CTE injection (inline.rs)**: Currently builds `Cte`, `With`, `TableAlias` AST nodes directly. polyglot-sql's `Expression` enum may or may not expose the same level of AST construction. The builder API offers `select().from()` but unclear if CTE injection into existing parsed queries is supported.
- **Validator (validator.rs)**: Pattern-matches specific AST node types (`TableFactor::Derived`, `Query.with`). Need equivalent type checks in polyglot-sql's `Expression` enum.
- **Error messages**: sqlparser-rs provides structured line/column info. polyglot-sql's error reporting quality is unknown.

---

## Compatibility Assessment

### Can polyglot-sql Replace sqlparser-rs?

**Technically, yes** — it covers all the same parsing/generation capabilities and adds transpilation.

**Practically, risky** — here's why:

1. **Different AST type system**: polyglot-sql uses a single `Expression` enum with 200+ variants. sqlparser-rs uses a rich type hierarchy (`Statement`, `Query`, `Select`, `SetExpr`, `TableFactor`, etc.). Every pattern match in ff-sql would need rewriting.

2. **Maturity**: polyglot-sql is v0.1.x (first release ~Feb 2026). sqlparser-rs is v0.61 with years of production use in DataFusion, DuckDB-rs, and hundreds of other projects. Edge cases in SQL parsing are infinite.

3. **Breaking changes**: A v0.1.x crate will likely have frequent breaking API changes. sqlparser-rs has a more stable (though still evolving) API.

4. **DataFusion compatibility**: Feather-Flow also depends on `datafusion-sql` (v52) which itself depends on sqlparser-rs. Having two SQL parsers with different AST types in the same project creates friction. DataFusion's type system expects sqlparser-rs types.

5. **Community and ecosystem**: sqlparser-rs has a large contributor base and is the de facto Rust SQL parser. polyglot-sql is a solo-author project.

### Can They Coexist?

**Yes, as a transpilation layer.** The most pragmatic architecture:

```
User SQL (Snowflake syntax)
    ↓
polyglot-sql::transpile(sql, Snowflake, DuckDB)   ← NEW: dialect conversion
    ↓
DuckDB-compatible SQL string
    ↓
sqlparser-rs::parse(sql)                            ← EXISTING: AST extraction
    ↓
visit_relations() → dependency extraction           ← EXISTING: unchanged
    ↓
qualify_table_references()                           ← EXISTING: unchanged
    ↓
DuckDB execution                                    ← EXISTING: unchanged
```

This approach:
- Adds transpilation without touching the existing AST pipeline
- Keeps sqlparser-rs for all structural analysis (where it's battle-tested)
- Uses polyglot-sql as a string-in/string-out preprocessor
- Zero risk to existing functionality
- Can be feature-flagged

---

## Recommendations

### Option A: Use polyglot-sql as a Transpilation Preprocessor (Recommended)

**What**: Add `polyglot-sql` as a dependency, use it solely for `transpile()` before the existing sqlparser-rs pipeline.

**Effort**: Low. Add one function call in the compilation pipeline.

**Risk**: Low. polyglot-sql bugs only affect transpilation, not core functionality. Can be feature-gated.

**Benefit**: Users can write Snowflake SQL and have it auto-converted to DuckDB for execution. No changes to dependency extraction, lineage, validation, etc.

```toml
# Cargo.toml addition
polyglot-sql = { version = "0.1", optional = true }

[features]
transpile = ["polyglot-sql"]
```

### Option B: Full Migration to polyglot-sql

**What**: Replace sqlparser-rs entirely with polyglot-sql.

**Effort**: High. Rewrite parser.rs, dialect.rs, extractor.rs, qualify.rs, inline.rs, lineage.rs, validator.rs. ~2,000+ lines of code changes.

**Risk**: High. polyglot-sql is v0.1.x with unknown edge cases. DataFusion compatibility issues. Potential for regressions in core functionality.

**Benefit**: Unified parsing + transpilation. Built-in lineage, scope analysis. Cleaner API for some operations.

**When to consider**: After polyglot-sql reaches v1.0+ and has significant production adoption.

### Option C: Gradual Migration

**What**: Start with Option A, then incrementally move specific modules to polyglot-sql as it matures.

**Migration order**:
1. Transpilation preprocessor (Option A) — immediate
2. Replace `extractor.rs` with `get_tables()` — after validating correctness
3. Replace `qualify.rs` with `rename_tables()` — after API stabilizes
4. Replace `lineage.rs` with built-in lineage — after validating feature parity
5. Replace `inline.rs` and `validator.rs` — last, most complex

---

## Key Questions to Investigate Before Adopting

1. **CTE construction**: Can polyglot-sql's `Expression` enum be used to programmatically build and inject CTEs into an existing parsed query? (Critical for inline.rs)
2. **Error quality**: Does polyglot-sql provide line/column information in parse errors?
3. **Edge cases**: How does it handle DuckDB-specific syntax (e.g., `COPY`, `ATTACH`, `PRAGMA`, lambda functions, list/struct types)?
4. **DataFusion conflict**: Can `polyglot-sql` and `sqlparser` v0.61 coexist in the same dependency tree without version conflicts?
5. **Performance**: Any benchmarks comparing parse speed to sqlparser-rs? (Probably fine for this use case, but worth checking.)

---

## References

- [polyglot GitHub](https://github.com/tobilg/polyglot)
- [polyglot-sql docs.rs](https://docs.rs/polyglot-sql/latest/polyglot_sql/)
- [Introductory blog post](https://tobilg.com/posts/introducing-polyglot-a-rust-wasm-sql-transpilation-library/)
- [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)
- [sqlglot (Python original)](https://github.com/tobymao/sqlglot)
