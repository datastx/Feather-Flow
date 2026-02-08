# Configuration

## featherflow.yml

The project configuration file lives at the project root.

```yaml
name: my_project
version: "1.0.0"

# SQL dialect (currently only duckdb supported)
dialect: duckdb

# Default materialization (view, table, incremental, ephemeral)
materialization: view

# Default schema for models without explicit schema config
schema: analytics

# Output directory for compiled SQL and manifest
target_path: target

# Database connection
database:
  type: duckdb
  path: "target/dev.duckdb"

# Model paths (relative to project root)
model_paths:
  - models

# Seed data paths
seed_paths:
  - seeds

# Macro paths for custom Jinja macros
macro_paths:
  - macros

# Source definition paths (YAML files with kind: sources)
source_paths:
  - sources

# Snapshot configuration path
snapshot_paths:
  - snapshots

# Variables accessible via {{ var('key') }} in templates
vars:
  start_date: "2024-01-01"
  environment: dev

# External tables (tables not managed by Featherflow)
# These won't produce "unknown dependency" warnings
external_tables:
  - raw.orders
  - raw.customers
  - raw.products
  - raw.payments
```

## Configuration Fields

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Project name |
| `version` | string | `"1.0.0"` | Project version |
| `dialect` | string | `"duckdb"` | SQL dialect (only `duckdb` supported) |
| `materialization` | string | `"view"` | Default materialization (view, table, incremental, ephemeral) |
| `schema` | string | none | Default schema for models |
| `target_path` | string | `"target"` | Output directory for compiled SQL and manifest |
| `database` | object | `{ type: duckdb, path: dev.duckdb }` | Database connection config (type + path) |
| `model_paths` | list | `["models"]` | Directories containing models |
| `seed_paths` | list | `["seeds"]` | Directories containing seed CSVs |
| `macro_paths` | list | `["macros"]` | Directories containing Jinja macros |
| `source_paths` | list | `["sources"]` | Directories containing source YAML files |
| `snapshot_paths` | list | `["snapshots"]` | Directories containing snapshots |
| `vars` | map | `{}` | Template variables |
| `external_tables` | list | `[]` | Tables not managed by Featherflow |
| `on_run_start` | list | `[]` | SQL statements to execute before any model runs |
| `on_run_end` | list | `[]` | SQL statements to execute after all models complete |
| `pre_hook` | list | `[]` | SQL hooks to execute before each model |
| `post_hook` | list | `[]` | SQL hooks to execute after each model |
| `targets` | map | `{}` | Named target configs (dev, staging, prod) with database/schema/var overrides |

## Variables

Variables defined in `vars:` are accessible in SQL templates via `{{ var('key') }}`:

```sql
SELECT *
FROM raw.orders
WHERE order_date >= '{{ var("start_date") }}'
```

Variables can have defaults:

```sql
-- Uses the default if 'missing_key' is not in vars
{{ var("missing_key", "fallback_value") }}
```

Referencing an undefined variable without a default produces error J002.

## Target Environments

The `--target` flag selects a target environment. The database path can incorporate the target:

```yaml
target_path: "target/{{ target }}.duckdb"
```

```bash
ff run --target dev      # → target/dev.duckdb
ff run --target staging  # → target/staging.duckdb
ff run --target prod     # → target/prod.duckdb
```

## External Tables

Tables listed in `external_tables` are treated as known dependencies but are not managed by Featherflow. This prevents false "unknown dependency" warnings for tables that exist in DuckDB but aren't Featherflow models (e.g., raw data loaded by an external process).

## Sources

Sources are defined in separate YAML files within your `source_paths` directories (not inline in `featherflow.yml`). Each source file must have `kind: sources`:

```yaml
# sources/raw.yml
kind: sources
version: 1
name: raw
schema: raw
description: "Raw data from ETL pipeline"

tables:
  - name: orders
    description: "Raw order events"
    freshness:
      warn_after: "24 hours"
      error_after: "48 hours"
    columns:
      - name: order_id
        data_type: INTEGER
      - name: customer_id
        data_type: INTEGER
  - name: customers
  - name: products
```

Source tables are automatically added to the known dependency set. Freshness rules are checked by `ff freshness`.

## Built-in Jinja Macros

Featherflow provides 17 built-in macros:

### Date/Time

| Macro | Output |
| --- | --- |
| `{{ date_spine("2024-01-01", "2024-12-31") }}` | `GENERATE_SERIES(...)` |
| `{{ date_trunc("month", "order_date") }}` | `DATE_TRUNC('month', order_date)` |
| `{{ date_add("order_date", 7, "day") }}` | `order_date + INTERVAL '7 day'` |
| `{{ date_diff("day", "start", "end") }}` | `DATE_DIFF('day', start, end)` |

### String

| Macro | Output |
| --- | --- |
| `{{ slugify("name") }}` | `LOWER(REGEXP_REPLACE(...))` |
| `{{ clean_string("name") }}` | `TRIM(REGEXP_REPLACE(...))` |
| `{{ split_part("name", ",", 1) }}` | `SPLIT_PART(name, ',', 1)` |

### Math

| Macro | Output |
| --- | --- |
| `{{ safe_divide("revenue", "orders") }}` | `CASE WHEN orders=0 THEN NULL ELSE revenue/orders END` |
| `{{ round_money("amount") }}` | `ROUND(CAST(amount AS DOUBLE), 2)` |
| `{{ percent_of("part", "total") }}` | `CASE WHEN total=0 THEN 0.0 ELSE 100.0*part/total END` |

### Utility

| Macro | Output |
| --- | --- |
| `{{ hash("id") }}` | `MD5(CAST(id AS VARCHAR))` |
| `{{ hash_columns(["a", "b"]) }}` | `MD5(a\|\|'\|'\|\|b)` |
| `{{ surrogate_key(["a", "b"]) }}` | Same as hash_columns |
| `{{ coalesce_columns(["a", "b", "c"]) }}` | `COALESCE(a, b, c)` |
| `{{ not_null("col") }}` | `col IS NOT NULL` |

### Cross-DB

| Macro | Output |
| --- | --- |
| `{{ limit_zero() }}` | `LIMIT 0` |
| `{{ bool_or("flag") }}` | `BOOL_OR(flag)` |

## Custom Macros

Place `.sql` files in your `macro_paths` directories. Use standard Jinja macro syntax:

```sql
-- macros/custom.sql
{% macro calculate_margin(revenue, cost) %}
  CASE
    WHEN {{ cost }} = 0 THEN 0
    ELSE ({{ revenue }} - {{ cost }}) / {{ revenue }}
  END
{% endmacro %}
```

Use in models:

```sql
SELECT
    order_id,
    revenue,
    cost,
    {{ calculate_margin("revenue", "cost") }} AS margin
FROM staging.stg_orders
```

## Custom Tests

Define test macros with the `test_` prefix:

```sql
-- macros/tests.sql
{% macro test_positive_value(model, column) %}
SELECT *
FROM {{ model }}
WHERE {{ column }} <= 0
{% endmacro %}
```

Reference in `.yml`:

```yaml
columns:
  - name: amount
    tests:
      - custom:
          name: positive_value
```
