-- ============================================================
-- v001: Initial meta database schema
-- ============================================================
-- DuckDB does NOT support ON DELETE CASCADE on foreign keys.
-- Application-level cascade is implemented in MetaDb::clear_project_data()
-- and MetaDb::clear_models().

CREATE SCHEMA IF NOT EXISTS ff_meta;

-- ============================================================
-- Schema Version Tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS ff_meta.schema_version (
    version     INTEGER NOT NULL,
    applied_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- Core: Projects
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_project START 1;

CREATE TABLE ff_meta.projects (
    project_id      INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_project'),
    name            VARCHAR NOT NULL UNIQUE,
    version         VARCHAR NOT NULL DEFAULT '1.0.0',
    root_path       VARCHAR NOT NULL,
    schema_name     VARCHAR,
    materialization VARCHAR NOT NULL DEFAULT 'view'
        CHECK (materialization IN ('view', 'table', 'incremental')),
    wap_schema      VARCHAR,
    dialect         VARCHAR NOT NULL DEFAULT 'duckdb',
    db_path         VARCHAR NOT NULL,
    db_name         VARCHAR NOT NULL DEFAULT 'main',
    target_path     VARCHAR NOT NULL DEFAULT 'target',
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- Project: Hooks and Variables
-- ============================================================

CREATE TABLE ff_meta.project_hooks (
    project_id       INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    hook_type        VARCHAR NOT NULL CHECK (hook_type IN ('on_run_start', 'on_run_end')),
    sql_text         VARCHAR NOT NULL,
    ordinal_position INTEGER NOT NULL,
    PRIMARY KEY (project_id, hook_type, ordinal_position)
);

CREATE TABLE ff_meta.project_vars (
    project_id INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    key        VARCHAR NOT NULL,
    value      VARCHAR NOT NULL,
    value_type VARCHAR NOT NULL DEFAULT 'string',
    PRIMARY KEY (project_id, key)
);

-- ============================================================
-- Core: Models
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_model START 1;

CREATE TABLE ff_meta.models (
    model_id              INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_model'),
    project_id            INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    name                  VARCHAR NOT NULL,
    source_path           VARCHAR NOT NULL,
    compiled_path         VARCHAR,
    materialization       VARCHAR NOT NULL DEFAULT 'view'
        CHECK (materialization IN ('view', 'table', 'incremental')),
    schema_name           VARCHAR,
    description           VARCHAR,
    owner                 VARCHAR,
    deprecated            BOOLEAN NOT NULL DEFAULT false,
    deprecation_message   VARCHAR,
    base_name             VARCHAR,
    version_number        INTEGER,
    contract_enforced     BOOLEAN NOT NULL DEFAULT false,
    raw_sql               VARCHAR NOT NULL,
    compiled_sql          VARCHAR,
    sql_checksum          VARCHAR,
    created_at            TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE TABLE ff_meta.model_config (
    model_id              INTEGER PRIMARY KEY REFERENCES ff_meta.models(model_id),
    unique_key            VARCHAR,
    incremental_strategy  VARCHAR,
    on_schema_change      VARCHAR,
    wap_enabled           BOOLEAN DEFAULT false
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_hook START 1;

CREATE TABLE ff_meta.model_hooks (
    hook_id          INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_hook'),
    model_id         INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    hook_type        VARCHAR NOT NULL
        CHECK (hook_type IN ('pre_hook', 'post_hook')),
    sql_text         VARCHAR NOT NULL,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (model_id, hook_type, ordinal_position)
);

CREATE TABLE ff_meta.model_tags (
    model_id    INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    tag         VARCHAR NOT NULL,
    PRIMARY KEY (model_id, tag)
);

CREATE TABLE ff_meta.model_meta (
    model_id    INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    key         VARCHAR NOT NULL,
    value       VARCHAR NOT NULL,
    PRIMARY KEY (model_id, key)
);

-- ============================================================
-- Core: Model Columns
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_col START 1;

CREATE TABLE ff_meta.model_columns (
    column_id            INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_col'),
    model_id             INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    name                 VARCHAR NOT NULL,
    declared_type        VARCHAR,
    inferred_type        VARCHAR,
    nullability_declared VARCHAR
        CHECK (nullability_declared IS NULL OR nullability_declared IN ('not_null', 'nullable')),
    nullability_inferred VARCHAR
        CHECK (nullability_inferred IS NULL OR nullability_inferred IN ('not_null', 'nullable')),
    description          VARCHAR,
    is_primary_key       BOOLEAN NOT NULL DEFAULT false,
    classification       VARCHAR
        CHECK (classification IS NULL OR classification IN ('pii', 'sensitive', 'internal', 'public')),
    ordinal_position     INTEGER NOT NULL,
    UNIQUE (model_id, name)
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_constraint START 1;

CREATE TABLE ff_meta.model_column_constraints (
    constraint_id   INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_constraint'),
    column_id       INTEGER NOT NULL REFERENCES ff_meta.model_columns(column_id),
    constraint_type VARCHAR NOT NULL
        CHECK (constraint_type IN ('not_null', 'primary_key', 'unique')),
    UNIQUE (column_id, constraint_type)
);

CREATE TABLE ff_meta.model_column_references (
    column_id              INTEGER PRIMARY KEY REFERENCES ff_meta.model_columns(column_id),
    referenced_model_name  VARCHAR NOT NULL,
    referenced_column_name VARCHAR NOT NULL
);

-- ============================================================
-- Core: Dependencies
-- ============================================================

CREATE TABLE ff_meta.model_dependencies (
    model_id            INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    depends_on_model_id INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    PRIMARY KEY (model_id, depends_on_model_id),
    CHECK (model_id != depends_on_model_id)
);

CREATE TABLE ff_meta.model_external_dependencies (
    model_id    INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    table_name  VARCHAR NOT NULL,
    PRIMARY KEY (model_id, table_name)
);

-- ============================================================
-- Core: Sources
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_source START 1;

CREATE TABLE ff_meta.sources (
    source_id     INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_source'),
    project_id    INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    name          VARCHAR NOT NULL,
    description   VARCHAR,
    database_name VARCHAR,
    schema_name   VARCHAR NOT NULL,
    owner         VARCHAR,
    UNIQUE (project_id, name)
);

CREATE TABLE ff_meta.source_tags (
    source_id   INTEGER NOT NULL REFERENCES ff_meta.sources(source_id),
    tag         VARCHAR NOT NULL,
    PRIMARY KEY (source_id, tag)
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_src_table START 1;

CREATE TABLE ff_meta.source_tables (
    source_table_id INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_src_table'),
    source_id       INTEGER NOT NULL REFERENCES ff_meta.sources(source_id),
    name            VARCHAR NOT NULL,
    identifier      VARCHAR,
    description     VARCHAR,
    UNIQUE (source_id, name)
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_src_col START 1;

CREATE TABLE ff_meta.source_columns (
    source_column_id INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_src_col'),
    source_table_id  INTEGER NOT NULL REFERENCES ff_meta.source_tables(source_table_id),
    name             VARCHAR NOT NULL,
    data_type        VARCHAR NOT NULL,
    description      VARCHAR,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (source_table_id, name)
);

-- ============================================================
-- Core: Functions (UDFs)
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_func START 1;

CREATE TABLE ff_meta.functions (
    function_id    INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_func'),
    project_id     INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    name           VARCHAR NOT NULL,
    function_type  VARCHAR NOT NULL
        CHECK (function_type IN ('scalar', 'table')),
    description    VARCHAR,
    sql_body       VARCHAR NOT NULL,
    sql_path       VARCHAR NOT NULL,
    yaml_path      VARCHAR NOT NULL,
    schema_name    VARCHAR,
    deterministic  BOOLEAN NOT NULL DEFAULT true,
    return_type    VARCHAR,
    UNIQUE (project_id, name)
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_func_arg START 1;

CREATE TABLE ff_meta.function_args (
    arg_id           INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_func_arg'),
    function_id      INTEGER NOT NULL REFERENCES ff_meta.functions(function_id),
    name             VARCHAR NOT NULL,
    data_type        VARCHAR NOT NULL,
    default_value    VARCHAR,
    description      VARCHAR,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (function_id, name)
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_func_ret START 1;

CREATE TABLE ff_meta.function_return_columns (
    return_column_id INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_func_ret'),
    function_id      INTEGER NOT NULL REFERENCES ff_meta.functions(function_id),
    name             VARCHAR NOT NULL,
    data_type        VARCHAR NOT NULL,
    ordinal_position INTEGER NOT NULL,
    UNIQUE (function_id, name)
);

-- ============================================================
-- Core: Seeds
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_seed START 1;

CREATE TABLE ff_meta.seeds (
    seed_id       INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_seed'),
    project_id    INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    name          VARCHAR NOT NULL,
    path          VARCHAR NOT NULL,
    description   VARCHAR,
    schema_name   VARCHAR,
    delimiter     VARCHAR NOT NULL DEFAULT ',',
    enabled       BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (project_id, name)
);

CREATE TABLE ff_meta.seed_column_types (
    seed_id     INTEGER NOT NULL REFERENCES ff_meta.seeds(seed_id),
    column_name VARCHAR NOT NULL,
    data_type   VARCHAR NOT NULL,
    PRIMARY KEY (seed_id, column_name)
);

-- ============================================================
-- Core: Tests
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_test START 1;

CREATE TABLE ff_meta.tests (
    test_id         INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_test'),
    project_id      INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    test_type       VARCHAR NOT NULL
        CHECK (test_type IN ('not_null', 'unique', 'accepted_values', 'relationship',
                             'positive', 'non_negative', 'min_value', 'max_value',
                             'regex', 'custom')),
    model_id        INTEGER REFERENCES ff_meta.models(model_id),
    column_name     VARCHAR,
    source_table_id INTEGER REFERENCES ff_meta.source_tables(source_table_id),
    severity        VARCHAR NOT NULL DEFAULT 'error'
        CHECK (severity IN ('error', 'warn')),
    where_clause    VARCHAR,
    config_json     VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_singular START 1;

CREATE TABLE ff_meta.singular_tests (
    singular_test_id INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_singular'),
    project_id       INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    name             VARCHAR NOT NULL,
    path             VARCHAR NOT NULL,
    sql_text         VARCHAR NOT NULL,
    UNIQUE (project_id, name)
);

-- ============================================================
-- Analysis: Column Lineage
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_lineage START 1;

CREATE TABLE ff_meta.column_lineage (
    lineage_id       INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_lineage'),
    target_model_id  INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    target_column    VARCHAR NOT NULL,
    source_model_id  INTEGER REFERENCES ff_meta.models(model_id),
    source_table     VARCHAR,
    source_column    VARCHAR NOT NULL,
    lineage_kind     VARCHAR NOT NULL
        CHECK (lineage_kind IN ('copy', 'transform', 'inspect')),
    is_direct        BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX idx_lineage_target ON ff_meta.column_lineage (target_model_id, target_column);
CREATE INDEX idx_lineage_source ON ff_meta.column_lineage (source_model_id, source_column);

-- ============================================================
-- Analysis: Compilation Runs & Diagnostics
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_run START 1;

CREATE TABLE ff_meta.compilation_runs (
    run_id        INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_run'),
    project_id    INTEGER NOT NULL REFERENCES ff_meta.projects(project_id),
    run_type      VARCHAR NOT NULL
        CHECK (run_type IN ('compile', 'validate', 'run', 'analyze', 'rules')),
    started_at    TIMESTAMP NOT NULL DEFAULT now(),
    completed_at  TIMESTAMP,
    status        VARCHAR NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'error')),
    node_selector VARCHAR
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_diag START 1;

CREATE TABLE ff_meta.diagnostics (
    diagnostic_id INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_diag'),
    run_id        INTEGER NOT NULL REFERENCES ff_meta.compilation_runs(run_id),
    code          VARCHAR NOT NULL,
    severity      VARCHAR NOT NULL,
    message       VARCHAR NOT NULL,
    model_id      INTEGER REFERENCES ff_meta.models(model_id),
    column_name   VARCHAR,
    hint          VARCHAR,
    pass_name     VARCHAR NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_mismatch START 1;

CREATE TABLE ff_meta.schema_mismatches (
    mismatch_id    INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_mismatch'),
    run_id         INTEGER NOT NULL REFERENCES ff_meta.compilation_runs(run_id),
    model_id       INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    column_name    VARCHAR NOT NULL,
    mismatch_type  VARCHAR NOT NULL
        CHECK (mismatch_type IN ('extra_in_sql', 'type_mismatch', 'nullability_mismatch')),
    declared_value VARCHAR,
    inferred_value VARCHAR
);

-- ============================================================
-- Analysis: Rule Violations
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS ff_meta.seq_violation START 1;

CREATE TABLE ff_meta.rule_violations (
    violation_id  INTEGER PRIMARY KEY DEFAULT nextval('ff_meta.seq_violation'),
    run_id        INTEGER NOT NULL REFERENCES ff_meta.compilation_runs(run_id),
    rule_name     VARCHAR NOT NULL,
    rule_path     VARCHAR NOT NULL,
    severity      VARCHAR NOT NULL
        CHECK (severity IN ('error', 'warn')),
    entity_name   VARCHAR,
    message       VARCHAR NOT NULL,
    context_json  VARCHAR
);

-- ============================================================
-- State: Model Run Tracking
-- ============================================================

CREATE TABLE ff_meta.model_run_state (
    model_id         INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    run_id           INTEGER NOT NULL REFERENCES ff_meta.compilation_runs(run_id),
    status           VARCHAR NOT NULL
        CHECK (status IN ('success', 'error', 'skipped')),
    row_count        BIGINT,
    sql_checksum     VARCHAR,
    schema_checksum  VARCHAR,
    duration_ms      BIGINT,
    started_at       TIMESTAMP NOT NULL,
    completed_at     TIMESTAMP,
    PRIMARY KEY (model_id, run_id)
);

-- ============================================================
-- State: Input Checksums for Incremental Builds
-- ============================================================

CREATE TABLE ff_meta.model_run_input_checksums (
    model_id          INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    run_id            INTEGER NOT NULL REFERENCES ff_meta.compilation_runs(run_id),
    upstream_model_id INTEGER NOT NULL REFERENCES ff_meta.models(model_id),
    checksum          VARCHAR NOT NULL,
    PRIMARY KEY (model_id, run_id, upstream_model_id)
);

-- ============================================================
-- State: Config Snapshot for Drift Detection
-- ============================================================

CREATE TABLE ff_meta.model_run_config (
    model_id              INTEGER NOT NULL,
    run_id                INTEGER NOT NULL,
    materialization       VARCHAR NOT NULL
        CHECK (materialization IN ('view', 'table', 'incremental')),
    schema_name           VARCHAR,
    unique_key            VARCHAR,
    incremental_strategy  VARCHAR,
    on_schema_change      VARCHAR,
    PRIMARY KEY (model_id, run_id),
    FOREIGN KEY (model_id, run_id) REFERENCES ff_meta.model_run_state(model_id, run_id)
);

-- ============================================================
-- Views
-- ============================================================

CREATE VIEW ff_meta.model_latest_state AS
SELECT mrs.*
FROM ff_meta.model_run_state mrs
WHERE mrs.run_id = (
    SELECT mrs2.run_id
    FROM ff_meta.model_run_state mrs2
    JOIN ff_meta.compilation_runs cr ON mrs2.run_id = cr.run_id
    WHERE mrs2.model_id = mrs.model_id
      AND mrs2.status = 'success'
      AND cr.run_type = 'run'
    ORDER BY mrs2.run_id DESC
    LIMIT 1
);

CREATE VIEW ff_meta.v_models AS
SELECT
    m.model_id,
    m.name,
    m.materialization,
    m.schema_name,
    m.description,
    m.owner,
    m.deprecated,
    m.contract_enforced,
    m.source_path,
    m.sql_checksum,
    mc.unique_key,
    mc.incremental_strategy,
    mc.on_schema_change,
    mc.wap_enabled,
    list(DISTINCT mt.tag ORDER BY mt.tag) FILTER (WHERE mt.tag IS NOT NULL) AS tags,
    (SELECT COUNT(*) FROM ff_meta.model_dependencies d WHERE d.model_id = m.model_id) AS dependency_count,
    (SELECT COUNT(*) FROM ff_meta.model_dependencies d WHERE d.depends_on_model_id = m.model_id) AS dependent_count
FROM ff_meta.models m
LEFT JOIN ff_meta.model_config mc ON m.model_id = mc.model_id
LEFT JOIN ff_meta.model_tags mt ON m.model_id = mt.model_id
GROUP BY m.model_id, m.name, m.materialization, m.schema_name, m.description,
         m.owner, m.deprecated, m.contract_enforced, m.source_path, m.sql_checksum,
         mc.unique_key, mc.incremental_strategy, mc.on_schema_change, mc.wap_enabled;

CREATE VIEW ff_meta.v_columns AS
SELECT
    mc.column_id,
    m.name AS model_name,
    mc.name AS column_name,
    mc.declared_type,
    mc.inferred_type,
    mc.nullability_declared,
    mc.nullability_inferred,
    mc.description,
    mc.is_primary_key,
    mc.classification,
    mc.ordinal_position
FROM ff_meta.model_columns mc
JOIN ff_meta.models m ON mc.model_id = m.model_id;

CREATE VIEW ff_meta.v_lineage AS
SELECT
    cl.lineage_id,
    tgt.name AS target_model,
    cl.target_column,
    COALESCE(src.name, cl.source_table) AS source_model,
    cl.source_column,
    cl.lineage_kind,
    cl.is_direct,
    tgt_col.classification AS target_classification,
    src_col.classification AS source_classification
FROM ff_meta.column_lineage cl
JOIN ff_meta.models tgt ON cl.target_model_id = tgt.model_id
LEFT JOIN ff_meta.models src ON cl.source_model_id = src.model_id
LEFT JOIN ff_meta.model_columns tgt_col
    ON tgt_col.model_id = cl.target_model_id AND tgt_col.name = cl.target_column
LEFT JOIN ff_meta.model_columns src_col
    ON src_col.model_id = cl.source_model_id AND src_col.name = cl.source_column;

CREATE VIEW ff_meta.v_diagnostics AS
SELECT
    d.code,
    d.severity,
    d.message,
    m.name AS model_name,
    d.column_name,
    d.hint,
    d.pass_name,
    cr.run_type,
    cr.started_at AS run_started_at
FROM ff_meta.diagnostics d
JOIN ff_meta.compilation_runs cr ON d.run_id = cr.run_id
LEFT JOIN ff_meta.models m ON d.model_id = m.model_id;

CREATE VIEW ff_meta.v_source_columns AS
SELECT
    s.name AS source_name,
    s.database_name,
    s.schema_name,
    st.name AS table_name,
    st.identifier AS actual_table_name,
    sc.name AS column_name,
    sc.data_type,
    sc.description
FROM ff_meta.source_columns sc
JOIN ff_meta.source_tables st ON sc.source_table_id = st.source_table_id
JOIN ff_meta.sources s ON st.source_id = s.source_id;
