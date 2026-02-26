# Feather-Flow Nodes: A Schema Validation Framework

## What Makes Feather-Flow Different

Feather-Flow is a **schema-validated transformation framework**. It is purpose-built for data teams writing transformation pipelines — taking raw data, reshaping it through a DAG of models, and producing clean, typed, validated output. While the architecture could theoretically extend to other domains, that is not the goal. The goal is to make data transformations reliable by treating schema as a first-class, compiler-enforced concept.

Where tools like dbt focus on SQL transformations with optional testing bolted on after the fact, Feather-Flow enforces that every node in the DAG has a clearly defined schema. This schema-first approach means that the compiler can statically verify that data flowing between nodes is type-safe, structurally correct, and contractually enforced — all **before** any SQL ever executes.

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
- **`description`**: Documentation for the column
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
2. **Schema comes from config** — either from the SQL `{{ config(schema="staging") }}` function, from the YAML, or from the project default in `featherflow.yml`
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

## Why This Matters

Feather-Flow is built for one thing: making data transformations reliable. Every design decision serves that goal.

1. **Catch errors at compile time**: Type mismatches, missing columns, and broken references are caught before any SQL executes. The transformation pipeline is validated end-to-end before a single row moves.

2. **Documentation as code**: The YAML schema isn't just metadata — it's a contract that the compiler enforces. Documentation can never drift from reality because the compiler won't let it.

3. **Safe refactoring**: Rename a column and the compiler tells you exactly which downstream transformations break. Refactoring a 50-model pipeline becomes a compile-check, not a prayer.

4. **Cross-model consistency**: The compiler verifies that the same logical concept (e.g., `customer_id`) has consistent types and nullability across the entire transformation chain.

5. **Kind-appropriate validation**: SQL models get deep static analysis through AST walking and DataFusion planning. Seeds get type inference. Sources define trust boundaries. Functions get typed interfaces. Python and Docker nodes get explicit contract stubs. Each kind gets the validation that makes sense for how it transforms data.

6. **Deterministic DAGs**: Because dependencies are extracted from rendered SQL AST (not runtime behavior), the transformation graph is always deterministic and complete.

## Scope

Feather-Flow is specialized for data transformation pipelines. It is not a general-purpose workflow orchestrator, a job scheduler, or an application framework. The node system, the schema validation, the AST-based lineage tracking — all of it is designed around the specific problem of taking data from sources, transforming it through a series of typed steps, and producing validated output.

The architecture supports multiple runtimes (SQL, Python, Docker) not for generality's sake, but because real transformation pipelines sometimes need to step outside SQL — for ML scoring, for complex Python logic, for containerized workloads. In every case, the transformation's output must be schema-declared so the compiler can validate the full pipeline. The runtime varies; the schema contract does not.
