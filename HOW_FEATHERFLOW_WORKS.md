# Feather-Flow Nodes: A Schema Validation Framework

## What Makes Feather-Flow Different

Feather-Flow is a **schema validation framework** for data transformation pipelines. Not a build tool with validation bolted on. Not a SQL runner with optional checks. Schema validation and static analysis are the load-bearing architecture — everything else (templating, execution, DAG scheduling) exists to serve them.

The design principle is simple and non-negotiable: **every node declares its output schema, and the compiler validates every edge in the DAG against those declarations before anything executes.** There is no opt-out. There is no "run without schemas" mode. If a node lacks a schema, it does not compile. If a schema contradicts the SQL, compilation fails. This is deliberate.

Where tools like dbt treat testing as an optional layer added after transformations are written and run, Feather-Flow inverts the relationship. The schema declaration comes first. The compiler validates the transformation against it. Only then does execution proceed. Testing is not a post-hoc check — it is a precondition.

## Static Analysis as a First-Class Citizen

Static analysis in Feather-Flow is not a linter, not an advisory tool, and not something you enable with a flag. It is an integral stage of the compile pipeline that **blocks execution** when it finds structural violations.

Concretely, this means:

1. **The compiler plans every SQL model through DataFusion** — not to execute it, but to build a `LogicalPlan` that reveals the output schema, column types, nullability, and join semantics. This is the same query planning that a database engine performs, repurposed as a static analysis pass.

2. **Schema mismatches are compile errors, not warnings** — if a YAML declaration says a model produces `customer_id INTEGER` but the SQL output does not contain that column, compilation halts with SA01 (MissingFromSql). The pipeline does not run with missing columns.

3. **Type inference propagates through the entire DAG** — the compiler walks models in topological order, feeding each model's inferred output schema into the catalog before planning downstream models. This means a type error in `stg_customers` is caught when `dim_customers` references it, even if `dim_customers` itself is well-formed in isolation.

4. **Analysis is not optional** — `ff run` executes static analysis before touching the database. The `--skip-static-analysis` flag exists as an escape hatch, but the default is enforcement. The system is designed so that if analysis passes, execution should succeed structurally (data quality is a separate concern).

5. **Non-SQL nodes participate through typed stubs** — Python scripts, seeds, sources, and (planned) Docker containers cannot be statically analyzed internally, but their YAML schemas register in the catalog as typed stubs. This means downstream SQL nodes that reference a Python model's output get the same static validation as if the Python node were pure SQL. The schema contract is the interface; the runtime is opaque.

The result: Feather-Flow catches type mismatches, missing columns, broken references, join key incompatibilities, nullability violations, and cross-model inconsistencies at compile time. The pipeline is validated end-to-end before a single row moves.

The core insight: if every transformation declares what it produces, you can validate the entire pipeline at compile time.

## What Is a Node?

A node is the fundamental unit of work in a Feather-Flow project. Each node:

1. **Lives in its own directory** under `nodes/`
2. **Has a mandatory `.yml` file** with the same name as the directory
3. **Declares a `kind:`** that determines what companion file accompanies it
4. **Defines its output schema** — the columns, types, and constraints it produces

The name of the node comes from the directory name. This name is immutable — it cannot be overridden — because it is the identity by which the compiler locates the node in dependency space.

```
nodes/
├── stg_customers/
│   ├── stg_customers.yml    # Schema definition (kind: sql)
│   └── stg_customers.sql    # SQL transformation
├── raw_customers/
│   ├── raw_customers.yml    # Schema definition (kind: seed)
│   └── raw_customers.csv    # CSV data
├── raw_ecommerce/
│   └── raw_ecommerce.yml    # Schema definition (kind: source, no companion file)
├── cents_to_dollars/
│   ├── cents_to_dollars.yml # Schema definition (kind: function)
│   └── cents_to_dollars.sql # SQL function body
├── py_enriched/
│   ├── py_enriched.yml      # Schema definition (kind: python)
│   └── py_enriched.py       # Python script
└── ml_predictions/          # (planned)
    ├── ml_predictions.yml   # Schema definition (kind: docker)
    └── ml_predictions.dockerfile
```

## The `kind:` System

The `kind` field in the YAML file is the discriminator that tells Feather-Flow what type of resource this node represents and, critically, **what schema validation techniques to apply**.

| kind       | companion file | schema validation approach                                    |
|------------|----------------|---------------------------------------------------------------|
| `sql`      | `<name>.sql`   | SQL AST parsing → DataFusion planning → inferred schema cross-checked against YAML declaration |
| `seed`     | `<name>.csv`   | CSV column types inferred or declared in YAML, validated at load time |
| `source`   | *(none)*       | Schema declared in YAML with typed columns; serves as the trust boundary for external data |
| `function` | `<name>.sql`   | Argument types, return type, and function signature validated; registered as typed stubs for static analysis |
| `python`   | `<name>.py`    | YAML columns registered as typed stubs in schema catalog; downstream SQL nodes validated against declared schema; dependencies declared explicitly |
| `docker`   | `<name>.dockerfile` *(planned)* | Same stub pattern as Python — YAML declares output schema and explicit dependencies; container image config defines the runtime |

Legacy kind values (`model`, `sources`, `functions`) are automatically normalized to their modern equivalents (`sql`, `source`, `function`).

### Enforcement

The compiler strictly enforces this structure:

- **No loose files**: A `.sql` file sitting directly under `nodes/` is a hard error. Every resource must be in its own directory.
- **Name matching**: The directory name, YAML filename, and companion filename must all match exactly. `nodes/stg_customers/stg_customers.sql` — not `stg_customers/model.sql`.
- **Kind-specific companion**: If `kind: sql`, the compiler expects `<name>.sql`. If `kind: seed`, it expects `<name>.csv`. Missing the expected file is a hard error.
- **YAML is mandatory**: A node directory without a matching `.yml` file fails with `NodeMissingYaml`.
- **Kind is mandatory**: A YAML file without a `kind:` field fails with `NodeMissingKind`.
- **CTEs are banned**: Any SQL model containing a `WITH` clause (Common Table Expression) fails with S005 (CteNotAllowed). This is a hard error, not a warning.
- **Derived tables are banned**: Inline subqueries in `FROM` clauses (e.g., `SELECT * FROM (SELECT ...)`) fail with S006 (DerivedTableNotAllowed). Scalar subqueries in `SELECT`, `WHERE`, and `HAVING` are still permitted.

## Why CTEs and Derived Tables Are Banned

CTEs and derived tables are banned because they are fundamentally at odds with what Feather-Flow is trying to do.

A CTE is an inline, anonymous transformation. It has no name in the DAG, no YAML schema, no declared output columns, no type contract, and no lineage tracking. It is invisible to the compiler. When a developer writes a 200-line model with four CTEs chained together, they have created four transformations that the schema validation framework cannot see, cannot validate independently, and cannot reuse.

The tradeoff CTEs represent is: **convenience for the author at the cost of correctness for the system.** A CTE lets you avoid creating a separate node. That feels productive in the moment. But what you've actually done is:

1. **Hidden a transformation from static analysis** — the compiler cannot cross-check intermediate CTE outputs against declared schemas, because CTEs don't have schemas. Type errors, nullability violations, and missing columns inside CTEs are only caught if they propagate to the final SELECT. Errors in intermediate steps are invisible.

2. **Made the pipeline harder to debug** — when a model fails, you're debugging the entire CTE chain as a monolith. If those same transformations were separate nodes, each one is independently testable, independently compilable, and independently traceable through lineage.

3. **Created untestable intermediate logic** — you cannot write a schema test against a CTE. You cannot add a contract to a CTE. You cannot tag, classify, or document a CTE's output columns. The CTE exists only inside the model that contains it.

4. **Traded performance for laziness** — this is the core issue. A CTE is almost always a staging transformation that should be its own node. The reason it isn't is that creating a node requires a directory, a YAML file, and a schema declaration. That feels like overhead. But that "overhead" is exactly the schema contract that makes the pipeline reliable. Skipping it is choosing convenience over correctness.

5. **Broken the one-model-one-transformation principle** — Feather-Flow's architecture assumes each node does one thing: take declared inputs, apply a single transformation, produce a declared output. CTEs violate this by nesting multiple transformations inside one node. The DAG becomes a lie — it shows one edge where there are actually four hidden steps.

The same reasoning applies to derived tables (`SELECT * FROM (SELECT ...)`). They are anonymous inline transformations with no schema contract.

**The alternative is always the same: make it a node.** If a transformation is worth writing, it is worth declaring. Create `nodes/stg_whatever/`, add the YAML with its schema, write the SQL as a standalone model. Now the compiler can validate it. Now it has lineage. Now it can be tested, documented, and reused. The marginal cost is one directory and one YAML file. The return is full static analysis coverage.

Scalar subqueries in `SELECT`, `WHERE`, and `HAVING` remain allowed because they are expressions, not transformations — they produce a single value, not a result set that should have its own schema.

### A note on recursive CTEs

Recursive CTEs (`WITH RECURSIVE`) are a legitimate computational primitive. Graph traversal, hierarchy walking, connected components, and iterative convergence algorithms cannot be expressed as a chain of prep tables — the recursion depth is data-dependent and unknowable at compile time. Feather-Flow does not currently support recursive CTEs, but we recognize they should be supported in the future.

When we do, recursive CTEs will be their own `kind:` of node — not a special case inside `kind: sql`. The semantics of recursive CTEs vary significantly between database engines (DuckDB's `USING KEY` upsert semantics, Postgres's optimization fences and cycle detection, Snowflake's iteration limits). Burying those differences inside a generic SQL node would hide complexity that the compiler needs to reason about. A dedicated `kind: recursive` (or similar) gives the compiler the right place to handle engine-specific compilation, validate termination conditions, and apply appropriate static analysis — without compromising the simplicity of the `kind: sql` path.

The ban today is unconditional for simplicity, but recursive CTEs are a genuine gap, not a philosophical disagreement.

## Schema Definition: The Heart of Feather-Flow

Every node declares its output schema in the YAML file. This is what makes Feather-Flow a schema validation framework rather than just a build tool.

### SQL Node Schema (`kind: sql`)

```yaml
version: 1
kind: sql

description: "Staged customers from raw source"
owner: data-team
tags: [staging, customers]

columns:
  - name: customer_id
    type: INTEGER
    description: "Unique identifier for the customer"
    classification: pii
    tests:
      - unique
      - not_null
  - name: customer_name
    type: VARCHAR
    description: "Full name of the customer"
    classification: pii
  - name: email
    type: VARCHAR
    description: "Customer email address"
  - name: signup_date
    type: DATE
    description: "Date the customer signed up"
  - name: customer_tier
    type: VARCHAR
    description: "Customer tier (gold, silver, bronze)"
```

Each column declares:
- **`name`**: The column name (matched case-insensitively against SQL output)
- **`type`**: The SQL data type (e.g., `INTEGER`, `VARCHAR`, `DECIMAL(10,2)`)
- **`description`**: Human- and machine-readable documentation for the column (see [Why Descriptions Are Mandatory](#why-descriptions-are-mandatory))
- **`classification`**: Data governance classification (`pii`, `sensitive`, `internal`, `public`)
- **`constraints`**: Contract enforcement (`not_null`, `primary_key`, `unique`)
- **`tests`**: Runtime validation tests (`unique`, `not_null`, `accepted_values`, etc.)
- **`references`**: Foreign key relationships to other nodes

### Source Node Schema (`kind: source`)

Sources define the boundary between Feather-Flow and external data. They have no companion file — they describe tables that already exist:

```yaml
kind: source
version: 1
name: raw_ecommerce
schema: raw
database: main

tables:
  - name: raw_customers
    columns:
      - name: id
        type: INTEGER
      - name: name
        type: VARCHAR
      - name: email
        type: VARCHAR
      - name: created_at
        type: TIMESTAMP
      - name: tier
        type: VARCHAR
```

Source schemas are the **trust boundary** — they tell the compiler what types to expect from tables it doesn't control. The static analysis engine uses these declarations to validate that downstream SQL nodes reference correct column names and compatible types.

### Function Node Schema (`kind: function`)

Functions define typed interfaces for reusable SQL logic. There are two distinct function types — **scalar** and **table** — each with different schema validation characteristics.

#### Scalar Functions

A scalar function takes arguments and returns a single value. The YAML declares the argument types and the return type:

```yaml
kind: function
version: 1
name: cents_to_dollars
function_type: scalar
args:
  - name: amount
    data_type: BIGINT
    description: "Amount in cents"
returns:
  data_type: DECIMAL(10,2)
```

The companion SQL file contains just the expression body:
```sql
amount / 100.0
```

The compiler deploys this as `CREATE OR REPLACE MACRO cents_to_dollars(amount) AS (amount / 100.0)`. When a downstream SQL model calls `cents_to_dollars(payment_cents)`, the static analysis engine knows the argument must be compatible with `BIGINT` and the return type is `DECIMAL(10,2)` — enabling full type inference through the call.

#### Table Functions (Set-Returning)

A table function returns a set of rows with named, typed columns. Instead of a scalar `data_type`, the `returns` block declares `columns`:

```yaml
kind: function
version: 1
name: order_volume_by_status
description: "Returns order counts grouped by status, filtered by minimum count"
function_type: table
args:
  - name: min_count
    data_type: INTEGER
    description: "Minimum order count threshold"
returns:
  columns:
    - name: status
      data_type: VARCHAR
    - name: order_count
      data_type: INTEGER
```

The companion SQL file contains a query:

```sql
select status, order_count
from (select status, count(*) as order_count from fct_orders group by status)
where order_count >= min_count
```

Note: function SQL bodies are exempt from the CTE and derived table bans (S005/S006). The bans apply to `kind: sql` models, where the alternative is always "make it a node." Function bodies are different — they are expression or query bodies compiled into DuckDB macros, not standalone transformations in the DAG. A derived table inside a function body is contained within the function's typed interface, not hidden from the compiler.

This deploys as `CREATE OR REPLACE MACRO order_volume_by_status(min_count) AS TABLE (...)`. Table functions are special because they participate in the DAG — the compiler parses the function's SQL body to discover that `order_volume_by_status` depends on `fct_orders`, and propagates that dependency transitively to any model that calls the function. The declared output columns (`status VARCHAR`, `order_count INTEGER`) register as a typed stub so that `SELECT * FROM order_volume_by_status(5)` in a downstream model gets full schema validation.

#### Function Validation Summary

The compiler validates both function types:
- Argument names are unique with no gaps in default values
- Return type is specified (scalar `data_type` or table `columns`)
- Table functions must declare at least one output column
- A matching `.sql` file contains the function body (non-empty)
- The function name is a valid SQL identifier
- Functions register as typed stubs (`UserFunctionStub` / `UserTableFunctionStub`) in the DataFusion context for downstream static analysis

### Python Node Schema (`kind: python`)

Python nodes cannot be statically analyzed for SQL dependencies, so the schema must be fully explicit:

```yaml
version: 1
kind: python
depends_on:
  - stg_source

columns:
  - name: id
    type: INTEGER
  - name: name
    type: VARCHAR
  - name: score
    type: DOUBLE
```

Dependencies are declared via `depends_on` rather than extracted from code. The output schema is declared in `columns` and validated after execution.

### Docker Node Schema (`kind: docker`) — *Planned*

Docker nodes extend the same stub pattern to containerized workloads. Like Python nodes, the compiler cannot inspect what happens inside a container — so the YAML must fully declare the node's position in the DAG and its output contract.

```yaml
version: 1
kind: docker
description: "ML model that scores customers based on order history"

image: ml-team/customer-scoring:latest
# or build from companion Dockerfile:
# build:
#   dockerfile: ml_predictions.dockerfile

depends_on:
  - fct_orders
  - dim_customers

columns:
  - name: customer_id
    type: INTEGER
    description: "Customer identifier"
  - name: score
    type: DOUBLE
    description: "Predicted lifetime value score"
  - name: segment
    type: VARCHAR
    description: "Assigned customer segment"
  - name: scored_at
    type: TIMESTAMP
    description: "When the prediction was generated"
```

The design follows the same principles that make Python nodes work:

1. **`depends_on` declares DAG position**: The compiler can't parse container internals for table references, so dependencies are explicit. This tells the compiler that `ml_predictions` must run after `fct_orders` and `dim_customers` — placing it correctly in the DAG.

2. **`columns` declares the output stub**: The column definitions register in the schema catalog exactly like Python and source nodes. Any downstream SQL model that does `SELECT score FROM ml_predictions` gets full static analysis — type inference, nullability checking, cross-model consistency — validated against the declared `DOUBLE` type.

3. **`image` or `build` declares the runtime**: This is the docker-specific config that tells the executor *how* to run the node. The companion `.dockerfile` (if building locally) follows the same naming convention as every other kind — `<name>.dockerfile` in the node's directory.

4. **Name is immutable**: `nodes/ml_predictions/` → the node name is `ml_predictions`. The compiler qualifies it as `schema.ml_predictions` for dependency resolution, same as any other node.

The key insight: from the compiler's perspective, a docker node is just another opaque transformation with a typed contract. The schema validation framework doesn't need to understand what happens inside the container — it only needs to know what goes in (`depends_on`) and what comes out (`columns`). That's enough to validate every edge in the DAG that touches this node.

This is the same pattern that scales to *any* future runtime kind. Whether the transformation runs as SQL, Python, a Docker container, a Spark job, or an API call — as long as it declares its dependencies and output schema, the compiler can validate the full pipeline statically.

## How Schema + Name = Identity

The compiler uses **schema + name** to determine where each node exists in dependency space. Here's how:

1. **Name comes from the directory** — `nodes/stg_customers/` → node name is `stg_customers`
2. **Schema comes from config** — either from the SQL `{{ config(schema="staging") }}` function or from the project default in `featherflow.yml`. There is no YAML-level schema override for SQL models — config is set exclusively via the `{{ config() }}` Jinja function in the SQL file itself
3. **The compiler qualifies bare table references** — after Jinja rendering and SQL parsing, `stg_customers` in a downstream model becomes `staging.stg_customers` (or `main.staging.stg_customers` for cross-database references)

This qualification happens via AST manipulation using `visit_relations_mut` from sqlparser. Only single-part (bare) names are qualified; already-qualified references are left unchanged.

The name cannot be overridden because it would break the deterministic relationship between filesystem layout and dependency resolution. The schema *can* be overridden via `config()` in the SQL template, because the compiler resolves it before building the DAG.

## The Compile Pipeline

The compile pipeline processes nodes through a series of stages, each building on the previous:

```
1. Project Discovery
   └─ Walk nodes/ directory
   └─ Probe each YAML for kind:
   └─ Dispatch to kind-specific loader
   └─ Enforce naming, structure, companion files

2. Jinja Templating
   └─ Render SQL templates with config(), vars, macros
   └─ Extract config() overrides (schema, materialization, etc.)
   └─ No I/O — only text transformation

3. SQL AST Parsing
   └─ Parse rendered SQL with sqlparser
   └─ Extract dependencies via visit_relations (AST walk)
   └─ Categorize: model deps vs. source deps vs. external

4. DAG Construction
   └─ Build dependency graph from extracted relationships
   └─ Topological sort for execution order
   └─ Detect circular dependencies

5. Table Qualification
   └─ Rewrite bare table names to schema.table
   └─ Uses the qualification map (model name → QualifiedRef)
   └─ AST manipulation, not string replacement

6. Schema Propagation (Static Analysis)
   └─ Walk DAG in topological order
   └─ Plan each model via DataFusion
   └─ Infer output schema from LogicalPlan
   └─ Cross-check inferred schema against YAML declaration
   └─ Feed inferred schemas forward for downstream models

7. Analysis Passes
   └─ Type inference (A002-A005)
   └─ Nullability propagation (A010-A012)
   └─ Join key analysis (A030-A033)
   └─ Unused column detection (A020-A021)
   └─ Cross-model consistency (A040-A041)
   └─ Schema mismatch detection (SA01, SA02)
```

The key insight: **Jinja templating completes before dependency extraction begins.** This means you can use Jinja to dynamically set the schema, materialization, tags, and other config — but you cannot use Jinja to dynamically generate table references that would affect the dependency graph. The dependencies are extracted from the *rendered* SQL AST, not from Jinja expressions.

This is a deliberate design choice. It ensures the DAG is deterministic — the compiler can always figure out the full dependency graph from the rendered SQL without needing to execute any I/O or runtime logic.

### Self-Referencing Models

Incremental models commonly need to reference their own table — for example, a LEFT JOIN against the target table to identify only new rows that haven't been loaded yet:

```sql
{{ config(materialized="incremental", unique_key="order_id", incremental_strategy="merge") }}

select o.order_id, o.customer_id, o.order_date, o.amount, o.status
from stg_orders o
left join fct_orders_incremental existing
    on o.order_id = existing.order_id
where existing.order_id is null
```

Here `fct_orders_incremental` references itself. The dependency extractor (step 3 above) correctly finds this table reference — it has no concept of "self" and simply reports every relation in the AST. But if this self-reference were fed into DAG construction (step 4), it would create a cycle: the model depends on itself, which makes topological sorting impossible.

The compile layer handles this by **filtering out self-references after dependency categorization and before DAG construction.** When the extracted dependencies for a model include the model's own name, that entry is silently removed. The model still compiles and executes normally — the self-reference is a runtime concern (the table either exists from a prior run or doesn't yet), not a structural dependency.

This is different from how dbt solves the same problem. dbt requires the `{{ this }}` Jinja keyword to reference the current model's table, making self-references syntactically distinct from cross-model references. Feather-Flow doesn't need this because it uses plain SQL with natural table names — the compiler already knows every model's name and can detect when a reference points back to the model being compiled. No special syntax required. You write `fct_orders_incremental` in the SQL for `fct_orders_incremental`, and the compiler understands it's a self-reference rather than a circular dependency.

**Key invariant:** self-references are excluded from the DAG only. They are *not* removed from the extracted SQL — the rendered output still contains the original table name, and the database resolves it at execution time.

## Schema Validation by Kind

Each node kind receives different schema validation techniques, tailored to what the compiler can know about that kind of resource:

### SQL Nodes (`kind: sql`) — Full Static Analysis

SQL nodes get the richest validation because the compiler can parse and plan the SQL. Since most nodes in a typical project are SQL, this is where Feather-Flow's schema validation framework does the heaviest lifting. The compiler doesn't just check that outputs match declarations — it walks the SQL AST to understand *how* columns are transformed, joined, filtered, and aggregated, then uses that understanding to validate the entire transformation chain.

**Column-Level Lineage via AST Walking**

The DataFusion `LogicalPlan` is walked to classify how every column is used:

- **Copy**: A direct column pass-through (`SELECT customer_id FROM stg_customers`) — the type and nullability are inherited from the source
- **Transform**: A column used in a computation (`SELECT amount / 100.0 AS amount_dollars`) — the output type is inferred from the expression
- **Inspect**: A column read but not in the output (`WHERE status = 'active'`, `JOIN ON a.id = b.id`, `GROUP BY region`) — used for nullability and join key analysis

This AST-level understanding is what enables the deep validation passes below. The compiler knows not just *what* columns exist, but *where they came from* and *what happened to them*:

1. **Schema Cross-Check (SA01/SA02)**: The DataFusion planner infers the output schema from the SQL `LogicalPlan`. This inferred schema is compared column-by-column against the YAML declaration:
   - **SA01 (MissingFromSql)**: A column declared in YAML doesn't appear in the SQL output — **hard error**, blocks execution
   - **SA02 (ExtraInSql / TypeMismatch / NullabilityMismatch)**: SQL output has extra columns, type differences, or nullability differences — **warning**

2. **Type Inference (A002-A005)**: Because the compiler walks every expression in the AST, it can validate type compatibility through transformations:
   - UNION branch type mismatches (column types must be compatible across branches)
   - UNION branch column count mismatches
   - Aggregate functions on incompatible types (e.g., `SUM(customer_name)` where `customer_name` is `VARCHAR`)
   - Lossy implicit casts (e.g., `DECIMAL` to `INTEGER`)

3. **Nullability Propagation (A010-A012)**: The AST walk tracks which columns become nullable through JOINs and flags unguarded usage:
   - A `LEFT JOIN` makes the right side nullable — if a downstream SELECT uses that column without `COALESCE` or `IS NOT NULL`, the compiler warns
   - YAML declares `NOT NULL` on a column that the AST shows is nullable from a JOIN — mismatch flagged
   - Redundant `IS NULL` checks on columns that are already proven non-null

4. **Join Key Analysis (A030-A033)**: The compiler inspects `JOIN ON` expressions (Inspect-kind edges) to validate:
   - Join key type mismatches between tables (e.g., joining `INTEGER` to `VARCHAR`)
   - Cross joins (potentially unintentional cartesian products)
   - Non-equi joins (inequality conditions that may signal logic errors)

5. **Cross-Model Consistency (A040-A041)**: Compares schemas across the DAG:
   - Same logical column (e.g., `customer_id`) with different types across models
   - Same logical column with different nullability across models

6. **Contract Validation**: If a model defines `contract: { enforced: true }`, the compiler validates at runtime that the actual database output matches the contract — missing columns, type mismatches, and extra columns are flagged.

7. **Unused Column Detection (A020-A021)**: By tracking which columns are referenced downstream in the DAG, the compiler identifies SELECT columns that nothing ever reads — dead columns that add cost without value.

### Seed Nodes (`kind: seed`) — Type Declaration and Inference

Seeds are CSV files, so the compiler validates:
- Column types can be explicitly declared in `column_types` or inferred from CSV data
- The seed schema feeds into the catalog so downstream SQL models can be validated against it
- Seeds participate in the DAG as data sources with known schemas

### Source Nodes (`kind: source`) — Trust Boundary Definition

Sources are the interface between Feather-Flow and the outside world:
- Column names and types declared in the YAML are taken as ground truth
- These declarations populate the schema catalog for static analysis
- Downstream models that `SELECT` from source tables are validated against the declared source schema
- If a source declares columns `(id INTEGER, name VARCHAR)` but a downstream model references `email`, the static analysis will catch it

### Function Nodes (`kind: function`) — Typed Interface Validation

Functions get interface-level validation with different behavior for scalar vs. table functions:

**Scalar functions** (`function_type: scalar`):
- Argument types and return type validated at definition time
- Registered as `UserFunctionStub` in the DataFusion context
- When a SQL model calls `cents_to_dollars(payment_cents)`, the engine validates that the argument is compatible with `BIGINT` and infers the return as `DECIMAL(10,2)`
- No DAG participation — scalar functions are pure expressions, they don't reference tables

**Table functions** (`function_type: table`):
- Output columns are declared and validated (must have at least one)
- Registered as `UserTableFunctionStub` in the DataFusion context
- **Transitive DAG participation**: the compiler parses the function's SQL body to discover table references. If `order_volume_by_status` queries `fct_orders`, any model calling `order_volume_by_status()` inherits that dependency — the function's internal dependencies are propagated transitively through `resolve_function_dependencies()`
- When a SQL model does `SELECT * FROM order_volume_by_status(5)`, the engine validates against the declared output columns (`status VARCHAR`, `order_count INTEGER`)

Both types: argument names must be unique, defaults must trail non-defaults, function name must be a valid SQL identifier, companion `.sql` file must exist and be non-empty.

### Python Nodes (`kind: python`) — Schema Stubs for Cross-Language Validation

Python scripts can't be parsed for SQL, but they still participate fully in the schema validation framework. The YAML schema acts as a **typed stub** — the same way source schemas define a trust boundary for external data, Python node schemas define a contract for non-SQL transformations.

Here's how it works:

1. **Schema registration**: During `build_schema_catalog()`, the Python model's YAML columns are converted to `TypedColumn`s and inserted into the `SchemaCatalog` — exactly like SQL models. This means the Python node's declared output schema is visible to the static analysis engine.

2. **Downstream validation**: When a SQL node references a Python model's output table, DataFusion plans that SQL against the schema catalog. The Python model's declared columns and types are used for type inference, nullability checking, and cross-model consistency — just as if the Python node were a SQL model.

3. **Planning exclusion**: Python models are excluded from DataFusion *planning* (there's no SQL to plan), but they are NOT excluded from the catalog. The filter happens at `run_pre_execution_analysis()` where `!model.is_python` filters the `sql_sources` fed to the planner, not the catalog that feeds schema propagation.

4. **Explicit dependencies**: Since Python code can't be AST-parsed for table references, dependencies must be declared in `depends_on`. These are resolved during project loading and used for DAG construction.

5. **Always table materialization**: Python models always materialize as tables (set automatically in `from_python_file()`).

The result: a Python node's YAML schema is a **contract stub**. If `py_enriched` declares `(id INTEGER, name VARCHAR, score DOUBLE)`, then any downstream SQL model that does `SELECT score + 1 FROM py_enriched` gets full type validation — the engine knows `score` is `DOUBLE` and validates accordingly. If the Python node's YAML says `score` is `VARCHAR` but a downstream model treats it as numeric, the static analysis catches the mismatch.

### Docker Nodes (`kind: docker`) — Container Stubs *(Planned)*

Docker nodes will follow the exact same stub pattern as Python nodes. From the schema validation framework's perspective, the only thing that changes is the runtime — the validation mechanics are identical:

- **YAML columns register as typed stubs** in the schema catalog, enabling downstream static analysis
- **`depends_on` declares DAG position** explicitly, since container internals are opaque
- **Cross-model validation applies**: type inference, nullability, and consistency checks all work against the declared output schema
- **The runtime is irrelevant to the compiler**: whether a node runs SQL in DuckDB, Python via `uv run`, or a scoring model inside a Docker container — the schema validation framework sees the same thing: a node with declared inputs (`depends_on`) and declared outputs (`columns`)

This is the extensibility model: any new `kind` that produces tabular output follows the same pattern. Declare the schema, declare the dependencies, and the compiler validates every edge.

## Why Descriptions Are Mandatory

Every node must have a description. Every column must have a description. This is not optional documentation — it is a first-class requirement of the schema contract.

The reason is simple: **data without descriptions is unusable by AI systems, and AI systems are now the primary consumers of data metadata.**

When an AI agent — whether it's generating SQL, building dashboards, answering business questions, or debugging a pipeline — encounters a column called `tier`, it needs to know: is this a customer loyalty tier? A pricing tier? A storage tier? A support tier? Without a description, the agent guesses. When it guesses wrong, the query is wrong, the dashboard is wrong, the answer is wrong. The failure mode is silent and invisible — the SQL runs, the results look plausible, and nobody notices the data is meaningless.

This is not a hypothetical problem. It is the defining problem of data platforms in the AI era. Every organization that has tried to build AI-powered analytics on top of undocumented data warehouses has hit the same wall: the AI can write SQL, but it cannot understand what the columns mean. Column names are not enough. `amt` could be dollars, cents, units, or a score. `status` could have three possible values or thirty. `date` could be creation date, modification date, ship date, or expiration date. Only the description resolves the ambiguity.

Feather-Flow enforces this through multiple mechanisms:

1. **Documentation validation (D001/D002)**: The `documentation:` config in `featherflow.yml` controls enforcement. When `require_model_descriptions: true` and `require_column_descriptions: true` are set, the compiler emits D001 (model missing description) and D002 (column missing description) as hard errors. These block compilation — a node without descriptions does not compile.

2. **Description drift detection (A050-A052)**: The static analysis engine tracks descriptions through column-level lineage. When a column is copied or renamed from an upstream model:
   - **A050**: A copy/rename column has no description — the compiler suggests inheriting it from the upstream source
   - **A051**: A copy/rename column has a modified description — potential documentation drift flagged for review
   - **A052**: A transformed column has no description — the compiler flags that new logic needs new documentation

3. **Governance enforcement (G002)**: Columns classified as `pii` must have descriptions. This is unconditional — PII data without documentation is a compliance risk, and the compiler treats it as one.

The design principle: **a schema without descriptions is structurally incomplete.** Types tell the compiler what a column *is*. Descriptions tell humans and AI systems what a column *means*. Both are required for correctness — type correctness for the compiler, semantic correctness for every system that consumes the metadata.

This is why Feather-Flow requires descriptions at both levels:

- **Node descriptions** answer: what does this transformation do? What business logic does it encode? What is its role in the pipeline? An AI agent building a query plan needs to know whether `stg_customers` is raw staging or enriched staging, whether `fct_orders` contains all orders or only completed ones.

- **Column descriptions** answer: what does this value represent? What are its units? What are its valid ranges? What business concept does it encode? An AI agent selecting columns needs to know that `revenue` is in USD cents (not dollars), that `status` values are `active|churned|suspended`, that `created_at` is the account creation timestamp (not the row insertion timestamp).

The cost of writing a description is one line of YAML. The cost of not writing it is every downstream consumer — human or AI — making assumptions about what the data means. In a world where AI agents are generating SQL against your schemas, those assumptions become silent errors at scale.

```yaml
# This is not enough:
columns:
  - name: amt
    type: DECIMAL(10,2)

# This is the minimum:
columns:
  - name: amt
    type: DECIMAL(10,2)
    description: "Order total in USD cents, excluding tax and shipping"
```

The description is not metadata about the schema. It *is* the schema. Without it, the column is a number with a name — technically valid, semantically meaningless.

### Description Provenance: `description_ai_generated`

AI agents are increasingly both the consumers and producers of data metadata. When an agent generates a description, downstream systems should know the provenance — human-written descriptions carry more weight than AI-generated ones. A human who wrote "Order total in USD cents, excluding tax and shipping" had domain knowledge. An agent that inferred the same description from column statistics and naming patterns may be right, but the confidence level is different.

Feather-Flow tracks this provenance with the `description_ai_generated` field — a tri-state value that sits as a sibling to every `description` field in the schema system:

- **`true`** — the description was generated by an AI agent
- **`false`** — the description was written by a human
- **omitted / `null`** — provenance is unknown (default for existing schemas)

```yaml
version: 1
kind: sql
description: "Staged customers from raw source"
description_ai_generated: true   # this description was AI-generated

columns:
  - name: customer_id
    type: INTEGER
    description: "Unique identifier for the customer"
    description_ai_generated: false   # explicitly human-written
  - name: email
    type: VARCHAR
    description: "Customer email address, used for login and notifications"
    # description_ai_generated omitted — provenance unknown
```

The field is available at every level where `description` exists:
- **Model/node level** (`ModelSchema.description_ai_generated`)
- **Column level** (`SchemaColumnDef.description_ai_generated`)
- **Source level** (`SourceFile.description_ai_generated`)
- **Source table level** (`SourceTable.description_ai_generated`)
- **Source column level** (`SourceColumn.description_ai_generated`)

The meta database tracks this field as a nullable boolean across `ff_meta.models`, `ff_meta.model_columns`, `ff_meta.sources`, `ff_meta.source_tables`, and `ff_meta.source_columns`.

This is purely metadata — it does not affect static analysis, validation, or execution. The D001/D002 documentation checks enforce that descriptions *exist*, regardless of provenance. The description drift passes (A050-A052) track how descriptions propagate through lineage, regardless of who wrote them. The `description_ai_generated` field is informational: it lets downstream systems (dashboards, AI agents, data catalogs) make informed decisions about how much to trust a description.

## Why This Matters

Feather-Flow exists to answer a single question: **is this pipeline structurally correct before I run it?**

Every design decision — mandatory YAML schemas, DataFusion-based planning, AST dependency extraction, typed stubs for non-SQL nodes — serves that question. The framework is built around the conviction that data pipeline failures caused by missing columns, type mismatches, and broken references are not runtime problems to be caught by tests. They are compile-time problems that should never reach execution.

1. **Compile-time correctness**: Type mismatches, missing columns, broken references, join key incompatibilities, and nullability violations are caught before any SQL executes. Static analysis is not a suggestion — it is a gate.

2. **Schema as enforced contract**: The YAML schema is not documentation. It is a contract that the compiler validates against the SQL AST. If the SQL produces columns that contradict the declaration, compilation fails. Documentation cannot drift from reality because they are the same artifact.

3. **Safe refactoring**: Rename a column and the compiler tells you exactly which downstream transformations break. The static analysis propagates through the entire DAG — a change in `stg_customers` surfaces errors in `dim_customers`, `fct_orders`, and every other model downstream. Refactoring a 50-model pipeline becomes a compile-check, not a prayer.

4. **Cross-model consistency**: The compiler verifies that the same logical concept (e.g., `customer_id`) has consistent types and nullability across the entire transformation chain. This is only possible because every node declares its schema and the analysis engine propagates types through the DAG.

5. **Kind-appropriate validation depth**: SQL models get full static analysis — AST walking, DataFusion planning, type inference, nullability propagation, join key analysis. Seeds get type inference from CSV data. Sources define typed trust boundaries. Functions get typed interface validation. Python and Docker nodes get explicit contract stubs. The validation depth varies by kind; the requirement to declare a schema does not.

6. **Deterministic DAGs**: Dependencies are extracted from rendered SQL AST (not runtime behavior), so the transformation graph is always deterministic and complete. The compiler can validate every edge without executing anything.

7. **AI-ready metadata**: Every node and every column carries a description that makes the schema semantically meaningful to AI systems. An AI agent querying your pipeline doesn't just see column names and types — it sees what the data *means*. This turns the schema from a structural contract into a semantic one, enabling AI-powered analytics, automated documentation, and intelligent query generation against a pipeline whose meaning is explicit and machine-readable.

## Where Feather-Flow Sits

Feather-Flow occupies a space between dbt and Airflow, but it is neither.

**dbt is a SQL execution framework.** It templates SQL, resolves `ref()` calls, builds a DAG, and runs queries against a warehouse. Schema tests exist but they are optional, run after execution, and bolt onto the side of the transformation logic. The core value proposition is "write SQL, dbt handles the execution order." The framework trusts your SQL and runs it.

**Airflow is a task orchestrator.** It schedules and coordinates arbitrary units of work — Python callables, Bash scripts, API calls, Spark jobs — with dependency management, retries, and monitoring. It has no opinion about what those tasks do or whether their outputs are structurally correct. The core value proposition is "define tasks and dependencies, Airflow handles scheduling and execution."

**Feather-Flow is a schema validation engine.** It does not exist to run SQL — DuckDB runs the SQL. It does not exist to orchestrate tasks — it has a DAG scheduler, but that is a means to an end, not the product. Feather-Flow exists to answer the question: *is this pipeline structurally correct?* Every node declares a typed schema. The compiler validates every edge in the DAG against those declarations. Execution only proceeds after static analysis confirms the pipeline is sound.

The distinction matters because it changes what the tool is responsible for:

| Concern | dbt | Airflow | Feather-Flow |
|---|---|---|---|
| **SQL execution** | Core responsibility | Delegates to operators | Delegates to DuckDB |
| **Task orchestration** | DAG of models | Core responsibility | DAG of nodes (minimal) |
| **Schema validation** | Optional post-hoc tests | None | Core responsibility |
| **Static analysis** | None | None | Compile-time gate |
| **Type propagation across DAG** | None | None | Full DAG-wide inference |
| **Cross-model consistency** | None | None | Enforced at compile time |

dbt asks: "did the SQL run?" Airflow asks: "did the task succeed?" Feather-Flow asks: "is the pipeline correct before we run anything?"

This is why Feather-Flow bans CTEs, requires YAML schemas on every node, and runs DataFusion planning before execution. These are not ergonomic choices — they are consequences of being a validation engine. A SQL execution framework can afford to be permissive because it will find out at runtime if something is wrong. A validation engine cannot, because the entire point is to find out before runtime.

## Scope

Feather-Flow is an opinionated framework. It enforces a specific way of writing data transformations: one transformation per node, mandatory schema declarations, no CTEs, static analysis before execution. Not every transformation workflow fits this model — but from years of data engineering experience, the vast majority do.

The transformations that don't fit are the edge cases: recursive graph traversals, highly dynamic schema-on-read pipelines, transformations where the output shape is unknowable until runtime. These are real but rare. The other 99% of data engineering work — staging raw data, joining dimensions to facts, aggregating metrics, building mart tables — is exactly the kind of structured, predictable, schema-stable work that benefits most from compile-time validation. Feather-Flow is built for that 99%.

The architecture supports multiple runtimes (SQL, Python, Docker) not for generality's sake, but because real transformation pipelines sometimes need to step outside SQL — for ML scoring, for complex Python logic, for containerized workloads. In every case, the transformation's output must be schema-declared so the compiler can validate the full pipeline. The runtime varies; the schema contract does not.
