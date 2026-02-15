//! Interactive documentation server using axum + embedded static assets

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use ff_jinja::JinjaEnvironment;
use ff_sql::extractor::categorize_dependencies;
use ff_sql::{extract_dependencies, SqlParser};
use rust_embed::Embed;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path;
use std::sync::Arc;

use ff_core::Project;

use crate::cli::{DocsServeArgs, GlobalArgs};
use crate::commands::common::load_project;

use super::data::*;

/// Embedded static assets from the `static/` directory
#[derive(Embed)]
#[folder = "static/"]
struct StaticAssets;

/// Pre-computed application state shared across all handlers
struct AppState {
    /// Index JSON (model summaries, edges, stats)
    index_json: String,
    /// Full model docs keyed by name
    model_docs: HashMap<String, String>,
    /// Column-level lineage JSON
    lineage_json: String,
    /// Search index JSON
    search_index_json: String,
}

/// Model summary for the index endpoint
#[derive(Debug, Serialize)]
struct IndexModel {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    materialized: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    resource_type: String,
    column_count: usize,
    test_count: usize,
}

/// Source summary for the index endpoint
#[derive(Debug, Serialize)]
struct IndexSource {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    schema: String,
    table_count: usize,
    resource_type: String,
}

/// Edge in the dependency graph
#[derive(Debug, Serialize)]
struct Edge {
    from: String,
    to: String,
}

/// Index response returned by /api/index.json
#[derive(Debug, Serialize)]
struct IndexResponse {
    project_name: String,
    models: Vec<IndexModel>,
    sources: Vec<IndexSource>,
    edges: Vec<Edge>,
    stats: IndexStats,
}

/// Project-level stats
#[derive(Debug, Serialize)]
struct IndexStats {
    total_models: usize,
    total_sources: usize,
    total_tests: usize,
    total_columns: usize,
}

/// Search index entry
#[derive(Debug, Serialize)]
struct SearchEntry {
    name: String,
    resource_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    tags: Vec<String>,
    columns: Vec<String>,
}

/// Lineage edge for column-level lineage
#[derive(Debug, Serialize)]
struct LineageEntry {
    model: String,
    column: String,
    source_model: String,
    source_column: String,
    is_direct: bool,
}

/// Execute the docs serve command
pub async fn execute(args: &DocsServeArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    println!("Building documentation data...");

    let state = build_app_state(&project)?;

    // Static export mode
    if let Some(export_path) = &args.static_export {
        return export_static_site(&state, export_path);
    }

    let state = Arc::new(state);

    let app = Router::new()
        .route("/api/index.json", get(get_index))
        .route("/api/models/{name}", get(get_model))
        .route("/api/lineage.json", get(get_lineage))
        .route("/api/search-index.json", get(get_search_index))
        .fallback(get(static_handler))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .context("Invalid host:port")?;

    println!(
        "Serving documentation at http://{}:{}",
        args.host, args.port
    );

    if !args.no_browser {
        let url = format!("http://{}:{}", args.host, args.port);
        if open::that(&url).is_err() {
            eprintln!("Could not open browser automatically. Visit: {}", url);
        }
    }

    println!("Press Ctrl+C to stop.\n");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind to {}:{}", args.host, args.port))?;
    axum::serve(listener, app)
        .await
        .context("HTTP server error")?;

    Ok(())
}

/// Build all pre-computed state from the project
fn build_app_state(project: &Project) -> Result<AppState> {
    let model_count = project.models.len();
    let source_count = project.sources.len();
    let mut index_models = Vec::with_capacity(model_count);
    let mut index_sources = Vec::with_capacity(source_count);
    let mut edges = Vec::new();
    let mut model_docs_map: HashMap<String, String> = HashMap::with_capacity(model_count);
    let mut search_entries: Vec<SearchEntry> = Vec::with_capacity(model_count + source_count);
    let mut lineage_entries: Vec<LineageEntry> = Vec::new();
    let mut total_tests = 0;
    let mut total_columns = 0;

    // Pre-compute known model names and source tables for dependency categorization
    let known_models: HashSet<&str> = project.model_names().into_iter().collect();
    let external_tables = project.source_table_names();
    let parser = SqlParser::duckdb();
    let jinja = JinjaEnvironment::new(&project.config.vars);

    // Process models — render Jinja, then parse SQL to extract dependencies
    for name in project.model_names() {
        if let Some(model) = project.get_model(name) {
            let mut doc = build_model_doc(model);

            // Render Jinja template first, then parse SQL to extract dependencies
            let rendered = jinja.render(&model.raw_sql).ok();
            let sql = rendered.as_deref().unwrap_or(&model.raw_sql);
            if let Ok(stmts) = parser.parse(sql) {
                let raw_deps = extract_dependencies(&stmts);
                let (model_deps, ext_deps) =
                    categorize_dependencies(raw_deps, &known_models, &external_tables);
                doc.depends_on = model_deps;
                doc.external_deps = ext_deps;
            }

            let test_count: usize = doc.columns.iter().map(|c| c.tests.len()).sum();
            total_tests += test_count;
            total_columns += doc.columns.len();

            index_models.push(IndexModel {
                name: doc.name.clone(),
                description: doc.description.clone(),
                owner: doc.owner.clone(),
                materialized: doc.materialized.clone(),
                tags: doc.tags.clone(),
                resource_type: "model".to_string(),
                column_count: doc.columns.len(),
                test_count,
            });

            // Search entry
            search_entries.push(SearchEntry {
                name: doc.name.clone(),
                resource_type: "model".to_string(),
                description: doc.description.clone(),
                tags: doc.tags.clone(),
                columns: doc.columns.iter().map(|c| c.name.clone()).collect(),
            });

            collect_lineage_entries(&doc, &mut lineage_entries);

            // Edges from parsed dependencies
            for dep in &doc.depends_on {
                edges.push(Edge {
                    from: dep.clone(),
                    to: doc.name.clone(),
                });
            }
            for ext in &doc.external_deps {
                edges.push(Edge {
                    from: ext.clone(),
                    to: doc.name.clone(),
                });
            }

            // Serialize full model doc
            let json = serde_json::to_string(&doc)?;
            model_docs_map.insert(doc.name.clone(), json);
        }
    }

    // Process sources
    for source in &project.sources {
        let doc = build_source_doc(source);

        index_sources.push(IndexSource {
            name: doc.name.clone(),
            description: doc.description.clone(),
            schema: doc.schema.clone(),
            table_count: doc.tables.len(),
            resource_type: "source".to_string(),
        });

        // Search entries for source tables
        for table in &doc.tables {
            search_entries.push(SearchEntry {
                name: format!("{}.{}", doc.name, table.name),
                resource_type: "source".to_string(),
                description: table.description.clone(),
                tags: doc.tags.clone(),
                columns: table.columns.iter().map(|c| c.name.clone()).collect(),
            });
        }
    }

    let index = IndexResponse {
        project_name: project.config.name.clone(),
        models: index_models,
        sources: index_sources,
        edges,
        stats: IndexStats {
            total_models: project.models.len(),
            total_sources: project.sources.len(),
            total_tests,
            total_columns,
        },
    };

    Ok(AppState {
        index_json: serde_json::to_string(&index)?,
        model_docs: model_docs_map,
        lineage_json: serde_json::to_string(&lineage_entries)?,
        search_index_json: serde_json::to_string(&search_entries)?,
    })
}

/// GET /api/index.json
async fn get_index(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        state.index_json.clone(),
    )
}

/// GET /api/models/:name
async fn get_model(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.model_docs.get(&name) {
        Some(json) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            json.clone(),
        ),
        None => (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::json!({"error": format!("Model '{}' not found", name)}).to_string(),
        ),
    }
}

/// GET /api/lineage.json
async fn get_lineage(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        state.lineage_json.clone(),
    )
}

/// GET /api/search-index.json
async fn get_search_index(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        state.search_index_json.clone(),
    )
}

/// Fallback handler: serve embedded static assets
async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Default to index.html for root or SPA routes
    let path = if path.is_empty() { "index.html" } else { path };

    match StaticAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, mime),
                    (header::CACHE_CONTROL, "no-cache".to_string()),
                ],
                content.data.into_owned(),
            )
                .into_response()
        }
        None => {
            // For SPA routing, serve index.html for paths that don't match a file
            match StaticAssets::get("index.html") {
                Some(content) => (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "text/html".to_string()),
                        (header::CACHE_CONTROL, "no-cache".to_string()),
                    ],
                    content.data.into_owned(),
                )
                    .into_response(),
                None => (StatusCode::NOT_FOUND, "Not found").into_response(),
            }
        }
    }
}

fn collect_lineage_entries(doc: &ModelDoc, entries: &mut Vec<LineageEntry>) {
    for col_lineage in &doc.column_lineage {
        for src in &col_lineage.source_columns {
            let parts: Vec<&str> = src.splitn(2, '.').collect();
            if parts.len() != 2 {
                continue;
            }
            entries.push(LineageEntry {
                model: doc.name.clone(),
                column: col_lineage.output_column.clone(),
                source_model: parts[0].to_string(),
                source_column: parts[1].to_string(),
                is_direct: col_lineage.is_direct,
            });
        }
    }
}

/// Export the site as static files to a directory
fn export_static_site(state: &AppState, export_path: &str) -> Result<()> {
    use std::fs;

    let base = path::Path::new(export_path);
    let api_dir = base.join("api");
    fs::create_dir_all(&api_dir)?;

    // Write API JSON files
    fs::write(api_dir.join("index.json"), &state.index_json)?;
    fs::write(api_dir.join("lineage.json"), &state.lineage_json)?;
    fs::write(api_dir.join("search-index.json"), &state.search_index_json)?;

    // Write individual model docs
    let models_dir = api_dir.join("models");
    fs::create_dir_all(&models_dir)?;
    for (name, json) in &state.model_docs {
        fs::write(models_dir.join(format!("{}.json", name)), json)?;
    }

    // Write embedded static assets
    for file_path in StaticAssets::iter() {
        if let Some(content) = StaticAssets::get(file_path.as_ref()) {
            let out_path = base.join(file_path.as_ref());
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&out_path, content.data.as_ref())?;
        }
    }

    println!("Static site exported to: {}", export_path);
    Ok(())
}
