//! Static documentation generation (markdown, HTML, JSON)

use anyhow::{Context, Result};
use ff_jinja::{get_builtin_macros, get_macro_categories, MacroMetadata};
use std::collections::HashMap;
use std::fs;

use ff_core::Project;

use crate::cli::{DocsArgs, DocsFormat, GlobalArgs};
use crate::commands::common::load_project;

use super::data::*;

const CHECKMARK: char = '\u{2713}';

/// Execute static documentation generation
pub async fn execute(args: &DocsArgs, global: &GlobalArgs) -> Result<()> {
    let project = load_project(global)?;

    // Determine output directory
    let output_dir = match &args.output {
        Some(path) => project.root.join(path),
        None => project.target_dir().join("docs"),
    };

    // Create output directory
    fs::create_dir_all(&output_dir).context("Failed to create output directory")?;

    // Get models to document
    let models_to_doc: Vec<String> = if args.nodes.is_some() {
        let (_, dag) = crate::commands::common::build_project_dag(&project)?;
        crate::commands::common::resolve_nodes(&project, &dag, &args.nodes)?
    } else {
        project
            .model_names()
            .into_iter()
            .map(String::from)
            .collect()
    };

    if global.verbose {
        eprintln!(
            "[verbose] Generating docs for {} models to {}",
            models_to_doc.len(),
            output_dir.display()
        );
    }

    println!("Generating documentation...\n");

    let mut models_with_schema = 0;
    let mut models_without_schema = 0;
    let mut model_docs: Vec<ModelDoc> = Vec::new();
    let mut index_entries: Vec<ModelSummary> = Vec::new();

    for name in &models_to_doc {
        if let Some(model) = project.get_model(name.as_str()) {
            let has_schema = model.schema.is_some();

            if has_schema {
                models_with_schema += 1;
            } else {
                models_without_schema += 1;
            }

            // Build documentation for this model
            let doc = build_model_doc(model);

            // Add to index
            index_entries.push(ModelSummary {
                name: model.name.to_string(),
                description: doc.description.clone(),
                owner: doc.owner.clone(),
                has_schema,
            });

            match args.format {
                DocsFormat::Markdown => {
                    let md_content = generate_markdown(&doc);
                    let md_path = output_dir.join(format!("{}.md", name));
                    fs::write(&md_path, md_content)?;
                    println!("  {} {}.md", CHECKMARK, name);
                }
                DocsFormat::Json => {
                    // For JSON, we collect all docs and output at the end
                    model_docs.push(doc);
                }
                DocsFormat::Html => {
                    let html_content = generate_html(&doc);
                    let html_path = output_dir.join(format!("{}.html", name));
                    fs::write(&html_path, html_content)?;
                    println!("  {} {}.html", CHECKMARK, name);
                }
            }
        }
    }

    // Generate source documentation
    let mut source_docs: Vec<SourceDoc> = Vec::new();
    let mut source_entries: Vec<SourceSummary> = Vec::new();

    for source in &project.sources {
        let doc = build_source_doc(source);

        source_entries.push(SourceSummary {
            name: source.name.clone(),
            description: source.description.clone(),
            table_count: source.tables.len(),
        });

        match args.format {
            DocsFormat::Markdown => {
                let md_content = generate_source_markdown(&doc);
                let md_path = output_dir.join(format!("source_{}.md", source.name));
                fs::write(&md_path, md_content)?;
                println!("  {} source_{}.md", CHECKMARK, source.name);
            }
            DocsFormat::Json => {
                source_docs.push(doc);
            }
            DocsFormat::Html => {
                let html_content = generate_source_html(&doc);
                let html_path = output_dir.join(format!("source_{}.html", source.name));
                fs::write(&html_path, html_content)?;
                println!("  {} source_{}.html", CHECKMARK, source.name);
            }
        }
    }

    // Generate index/output
    match args.format {
        DocsFormat::Markdown => {
            // Generate index.md
            let index_content =
                generate_index_markdown(&project.config.name, &index_entries, &source_entries);
            let index_path = output_dir.join("index.md");
            fs::write(&index_path, index_content)?;
            println!("  {} index.md", CHECKMARK);
        }
        DocsFormat::Json => {
            // Output all docs as a single JSON file
            let docs_map: HashMap<String, ModelDoc> = model_docs
                .into_iter()
                .map(|d| (d.name.clone(), d))
                .collect();

            let sources_map: HashMap<String, SourceDoc> = source_docs
                .into_iter()
                .map(|d| (d.name.clone(), d))
                .collect();

            let json_output = serde_json::json!({
                "project_name": project.config.name,
                "models": docs_map,
                "sources": sources_map,
                "summary": {
                    "total_models": models_with_schema + models_without_schema,
                    "models_with_schema": models_with_schema,
                    "models_without_schema": models_without_schema,
                    "total_sources": source_entries.len(),
                }
            });

            let json_path = output_dir.join("docs.json");
            let json_content = serde_json::to_string_pretty(&json_output)?;
            fs::write(&json_path, json_content)?;
            println!("  {} docs.json", CHECKMARK);
        }
        DocsFormat::Html => {
            // Generate index.html
            let index_content =
                generate_index_html(&project.config.name, &index_entries, &source_entries);
            let index_path = output_dir.join("index.html");
            fs::write(&index_path, index_content)?;
            println!("  {} index.html", CHECKMARK);
        }
    }

    // Generate lineage diagram (DOT file)
    let lineage_content = generate_lineage_dot(&project);
    let lineage_path = output_dir.join("lineage.dot");
    fs::write(&lineage_path, lineage_content)?;
    println!("  {} lineage.dot", CHECKMARK);

    // Generate macro documentation
    match args.format {
        DocsFormat::Markdown => {
            let macros_content = generate_macros_markdown();
            let macros_path = output_dir.join("macros.md");
            fs::write(&macros_path, macros_content)?;
            println!("  {} macros.md", CHECKMARK);
        }
        DocsFormat::Html => {
            let macros_content = generate_macros_html();
            let macros_path = output_dir.join("macros.html");
            fs::write(&macros_path, macros_content)?;
            println!("  {} macros.html", CHECKMARK);
        }
        DocsFormat::Json => {
            // JSON already includes macros in the overall output, but let's also
            // add a separate macros.json with full metadata
            let macros_data = get_builtin_macros();
            let macros_json = serde_json::to_string_pretty(&macros_data)?;
            let macros_path = output_dir.join("macros.json");
            fs::write(&macros_path, macros_json)?;
            println!("  {} macros.json", CHECKMARK);
        }
    }

    let macro_count = get_builtin_macros().len();

    println!();
    println!(
        "Generated docs for {} models ({} with schema, {} without), {} sources, {} macros",
        models_with_schema + models_without_schema,
        models_with_schema,
        models_without_schema,
        source_entries.len(),
        macro_count
    );
    println!("Output: {}", output_dir.display());

    Ok(())
}

/// Generate markdown documentation for a model
fn generate_markdown(doc: &ModelDoc) -> String {
    let mut md = String::new();

    // Title
    md.push_str(&format!("# {}\n\n", doc.name));

    // Description
    if let Some(desc) = &doc.description {
        md.push_str(&format!("{}\n\n", desc));
    }

    // Metadata
    if doc.owner.is_some()
        || doc.team.is_some()
        || doc.contact.is_some()
        || !doc.tags.is_empty()
        || doc.materialized.is_some()
    {
        if let Some(owner) = &doc.owner {
            md.push_str(&format!("**Owner**: {}\n\n", owner));
        }
        if let Some(team) = &doc.team {
            md.push_str(&format!("**Team**: {}\n\n", team));
        }
        if let Some(contact) = &doc.contact {
            md.push_str(&format!("**Contact**: {}\n\n", contact));
        }
        if !doc.tags.is_empty() {
            md.push_str(&format!("**Tags**: {}\n\n", doc.tags.join(", ")));
        }
        if let Some(mat) = &doc.materialized {
            md.push_str(&format!("**Materialized**: {}\n\n", mat));
        }
        if let Some(schema) = &doc.schema {
            md.push_str(&format!("**Schema**: {}\n\n", schema));
        }
    }

    // Dependencies
    if !doc.depends_on.is_empty() || !doc.external_deps.is_empty() {
        md.push_str("## Dependencies\n\n");

        for dep in &doc.depends_on {
            md.push_str(&format!("- `{}`\n", dep));
        }
        for dep in &doc.external_deps {
            md.push_str(&format!("- `{}` (external)\n", dep));
        }
        md.push('\n');
    }

    // Columns
    if !doc.columns.is_empty() {
        md.push_str("## Columns\n\n");
        md.push_str("| Column | Type | Description | Classification | Tests |\n");
        md.push_str("|--------|------|-------------|----------------|-------|\n");

        for col in &doc.columns {
            let data_type = col.data_type.as_str();
            let desc = col.description.as_deref().unwrap_or("-");
            let cls = col.classification.as_deref().unwrap_or("-");
            let tests = if col.tests.is_empty() {
                "-".to_string()
            } else {
                col.tests.join(", ")
            };
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                col.name, data_type, desc, cls, tests
            ));
        }
        md.push('\n');

        // Relationships section
        let refs: Vec<_> = doc
            .columns
            .iter()
            .filter_map(|c| c.references.as_ref().map(|r| (&c.name, r)))
            .collect();
        if !refs.is_empty() {
            md.push_str("## Relationships\n\n");
            for (col_name, ref_info) in refs {
                md.push_str(&format!(
                    "- `{}` references `{}.{}`\n",
                    col_name, ref_info.model, ref_info.column
                ));
            }
            md.push('\n');
        }
    } else {
        md.push_str("*No schema file found for this model.*\n\n");
    }

    // Column Lineage section
    if !doc.column_lineage.is_empty() {
        md.push_str("## Column Lineage\n\n");
        md.push_str("| Output Column | Sources | Type | Direct |\n");
        md.push_str("|---------------|---------|------|--------|\n");

        for col in &doc.column_lineage {
            let sources = if col.source_columns.is_empty() {
                "-".to_string()
            } else {
                col.source_columns.join(", ")
            };
            let direct = if col.is_direct { "\u{2713}" } else { "-" };
            md.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                col.output_column, sources, col.expr_type, direct
            ));
        }
        md.push('\n');
    }

    // Test Suggestions section
    if !doc.test_suggestions.is_empty() {
        md.push_str("## Suggested Tests\n\n");
        md.push_str("| Column | Suggested Test | Reason |\n");
        md.push_str("|--------|----------------|--------|\n");

        for sugg in &doc.test_suggestions {
            md.push_str(&format!(
                "| {} | {} | {} |\n",
                sugg.column, sugg.test_type, sugg.reason
            ));
        }
        md.push('\n');
    }

    md
}

/// Generate markdown documentation for a source
fn generate_source_markdown(doc: &SourceDoc) -> String {
    let mut md = String::new();

    // Title
    md.push_str(&format!("# Source: {}\n\n", doc.name));

    // Description
    if let Some(desc) = &doc.description {
        md.push_str(&format!("{}\n\n", desc));
    }

    // Metadata
    md.push_str(&format!("**Schema**: {}\n\n", doc.schema));
    if let Some(owner) = &doc.owner {
        md.push_str(&format!("**Owner**: {}\n\n", owner));
    }
    if !doc.tags.is_empty() {
        md.push_str(&format!("**Tags**: {}\n\n", doc.tags.join(", ")));
    }

    // Tables
    md.push_str("## Tables\n\n");
    for table in &doc.tables {
        md.push_str(&format!("### {}\n\n", table.name));
        if let Some(desc) = &table.description {
            md.push_str(&format!("{}\n\n", desc));
        }

        if !table.columns.is_empty() {
            md.push_str("| Column | Type | Description | Tests |\n");
            md.push_str("|--------|------|-------------|-------|\n");

            for col in &table.columns {
                let data_type = col.data_type.as_str();
                let desc = col.description.as_deref().unwrap_or("-");
                let tests = if col.tests.is_empty() {
                    "-".to_string()
                } else {
                    col.tests.join(", ")
                };
                md.push_str(&format!(
                    "| {} | {} | {} | {} |\n",
                    col.name, data_type, desc, tests
                ));
            }
            md.push('\n');
        }
    }

    md
}

/// Generate markdown index file
fn generate_index_markdown(
    project_name: &str,
    models: &[ModelSummary],
    sources: &[SourceSummary],
) -> String {
    let mut md = String::new();

    md.push_str(&format!("# {} Documentation\n\n", project_name));

    let with_schema = models.iter().filter(|m| m.has_schema).count();
    let without_schema = models.len() - with_schema;

    md.push_str(&format!(
        "**Models**: {} total ({} with schema, {} without)\n\n",
        models.len(),
        with_schema,
        without_schema
    ));

    if !sources.is_empty() {
        let total_tables: usize = sources.iter().map(|s| s.table_count).sum();
        md.push_str(&format!(
            "**Sources**: {} ({} tables)\n\n",
            sources.len(),
            total_tables
        ));
    }

    let macro_count = get_builtin_macros().len();
    md.push_str(&format!(
        "**Macros**: {} built-in macros ([view documentation](macros.md))\n\n",
        macro_count
    ));

    md.push_str("## Models\n\n");
    md.push_str("| Model | Description | Owner | Has Schema |\n");
    md.push_str("|-------|-------------|-------|------------|\n");

    for model in models {
        let desc = model.description.as_deref().unwrap_or("-");
        let owner = model.owner.as_deref().unwrap_or("-");
        let has_schema = if model.has_schema { "\u{2713}" } else { "-" };
        md.push_str(&format!(
            "| [{}]({}.md) | {} | {} | {} |\n",
            model.name, model.name, desc, owner, has_schema
        ));
    }
    md.push('\n');

    if !sources.is_empty() {
        md.push_str("## Sources\n\n");
        md.push_str("| Source | Description | Tables |\n");
        md.push_str("|--------|-------------|--------|\n");

        for source in sources {
            let desc = source.description.as_deref().unwrap_or("-");
            md.push_str(&format!(
                "| [{}](source_{}.md) | {} | {} |\n",
                source.name, source.name, desc, source.table_count
            ));
        }
        md.push('\n');
    }

    md
}

/// Common HTML styles for documentation pages
fn html_styles() -> &'static str {
    r#"
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            color: #333;
        }
        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        h2 { color: #34495e; margin-top: 30px; }
        h3 { color: #7f8c8d; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #3498db; color: white; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        tr:hover { background-color: #f5f5f5; }
        code { background-color: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-family: monospace; }
        .metadata { background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin: 15px 0; }
        .metadata p { margin: 5px 0; }
        a { color: #3498db; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .badge { display: inline-block; padding: 3px 8px; border-radius: 3px; font-size: 12px; }
        .badge-pass { background-color: #27ae60; color: white; }
        .badge-schema { background-color: #9b59b6; color: white; }
        ul { list-style-type: disc; padding-left: 20px; }
        li { margin: 5px 0; }
        nav { background-color: #2c3e50; padding: 10px 20px; margin: -20px -20px 20px -20px; }
        nav a { color: white; margin-right: 15px; }
    </style>
"#
}

/// Generate HTML documentation for a model
fn generate_html(doc: &ModelDoc) -> String {
    let mut html = String::new();

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    html.push_str(&format!(
        "<meta charset=\"UTF-8\">\n<title>{}</title>\n",
        doc.name
    ));
    html.push_str(html_styles());
    html.push_str("</head>\n<body>\n");

    // Navigation
    html.push_str("<nav><a href=\"index.html\">Home</a></nav>\n");

    // Title
    html.push_str(&format!("<h1>{}</h1>\n", doc.name));

    // Description
    if let Some(desc) = &doc.description {
        html.push_str(&format!("<p>{}</p>\n", html_escape(desc)));
    }

    // Metadata
    if doc.owner.is_some()
        || doc.team.is_some()
        || doc.contact.is_some()
        || !doc.tags.is_empty()
        || doc.materialized.is_some()
        || doc.schema.is_some()
    {
        html.push_str("<div class=\"metadata\">\n");
        if let Some(owner) = &doc.owner {
            html.push_str(&format!(
                "<p><strong>Owner:</strong> {}</p>\n",
                html_escape(owner)
            ));
        }
        if let Some(team) = &doc.team {
            html.push_str(&format!(
                "<p><strong>Team:</strong> {}</p>\n",
                html_escape(team)
            ));
        }
        if let Some(contact) = &doc.contact {
            // Format contact as mailto link if it looks like an email
            if contact.contains('@') {
                html.push_str(&format!(
                    "<p><strong>Contact:</strong> <a href=\"mailto:{}\">{}</a></p>\n",
                    html_escape(contact),
                    html_escape(contact)
                ));
            } else {
                html.push_str(&format!(
                    "<p><strong>Contact:</strong> {}</p>\n",
                    html_escape(contact)
                ));
            }
        }
        if !doc.tags.is_empty() {
            let escaped_tags: Vec<String> = doc.tags.iter().map(|t| html_escape(t)).collect();
            html.push_str(&format!(
                "<p><strong>Tags:</strong> {}</p>\n",
                escaped_tags.join(", ")
            ));
        }
        if let Some(mat) = &doc.materialized {
            html.push_str(&format!("<p><strong>Materialized:</strong> {}</p>\n", mat));
        }
        if let Some(schema) = &doc.schema {
            html.push_str(&format!(
                "<p><strong>Schema:</strong> {}</p>\n",
                html_escape(schema)
            ));
        }
        html.push_str("</div>\n");
    }

    // Dependencies
    if !doc.depends_on.is_empty() || !doc.external_deps.is_empty() {
        html.push_str("<h2>Dependencies</h2>\n<ul>\n");
        for dep in &doc.depends_on {
            html.push_str(&format!(
                "<li><code><a href=\"{}.html\">{}</a></code></li>\n",
                url_encode_path(dep),
                html_escape(dep)
            ));
        }
        for dep in &doc.external_deps {
            html.push_str(&format!(
                "<li><code>{}</code> (external)</li>\n",
                html_escape(dep)
            ));
        }
        html.push_str("</ul>\n");
    }

    // Columns
    if !doc.columns.is_empty() {
        html.push_str("<h2>Columns</h2>\n");
        html.push_str("<table>\n<thead><tr><th>Column</th><th>Type</th><th>Description</th><th>Classification</th><th>Tests</th></tr></thead>\n<tbody>\n");

        for col in &doc.columns {
            let data_type = col.data_type.as_str();
            let desc = col.description.as_deref().unwrap_or("-");
            let cls = col.classification.as_deref().unwrap_or("-");
            let tests = if col.tests.is_empty() {
                "-".to_string()
            } else {
                col.tests.join(", ")
            };
            html.push_str(&format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                html_escape(&col.name),
                html_escape(data_type),
                html_escape(desc),
                html_escape(cls),
                html_escape(&tests)
            ));
        }
        html.push_str("</tbody></table>\n");

        // Relationships section
        let refs: Vec<_> = doc
            .columns
            .iter()
            .filter_map(|c| c.references.as_ref().map(|r| (&c.name, r)))
            .collect();
        if !refs.is_empty() {
            html.push_str("<h2>Relationships</h2>\n<ul>\n");
            for (col_name, ref_info) in refs {
                html.push_str(&format!(
                    "<li><code>{}</code> references <code><a href=\"{}.html\">{}</a>.{}</code></li>\n",
                    html_escape(col_name),
                    url_encode_path(&ref_info.model),
                    html_escape(&ref_info.model),
                    html_escape(&ref_info.column)
                ));
            }
            html.push_str("</ul>\n");
        }
    } else {
        html.push_str("<p><em>No schema file found for this model.</em></p>\n");
    }

    // Column Lineage section
    if !doc.column_lineage.is_empty() {
        html.push_str("<h2>Column Lineage</h2>\n");
        html.push_str("<table>\n<thead><tr><th>Output Column</th><th>Sources</th><th>Type</th><th>Direct</th></tr></thead>\n<tbody>\n");

        for col in &doc.column_lineage {
            let sources = if col.source_columns.is_empty() {
                "-".to_string()
            } else {
                col.source_columns
                    .iter()
                    .map(|s| format!("<code>{}</code>", html_escape(s)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let direct = if col.is_direct {
                "<span class=\"badge badge-pass\">Direct</span>"
            } else {
                "-"
            };
            html.push_str(&format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                col.output_column, sources, col.expr_type, direct
            ));
        }
        html.push_str("</tbody></table>\n");
    }

    // Test Suggestions section
    if !doc.test_suggestions.is_empty() {
        html.push_str("<h2>Suggested Tests</h2>\n");
        html.push_str("<table>\n<thead><tr><th>Column</th><th>Suggested Test</th><th>Reason</th></tr></thead>\n<tbody>\n");

        for sugg in &doc.test_suggestions {
            html.push_str(&format!(
                "<tr><td><code>{}</code></td><td><span class=\"badge badge-schema\">{}</span></td><td>{}</td></tr>\n",
                sugg.column, sugg.test_type, html_escape(&sugg.reason)
            ));
        }
        html.push_str("</tbody></table>\n");
    }

    html.push_str("</body>\n</html>\n");
    html
}

/// Generate HTML documentation for a source
fn generate_source_html(doc: &SourceDoc) -> String {
    let mut html = String::new();

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    html.push_str(&format!(
        "<meta charset=\"UTF-8\">\n<title>Source: {}</title>\n",
        doc.name
    ));
    html.push_str(html_styles());
    html.push_str("</head>\n<body>\n");

    // Navigation
    html.push_str("<nav><a href=\"index.html\">Home</a></nav>\n");

    // Title
    html.push_str(&format!("<h1>Source: {}</h1>\n", doc.name));

    // Description
    if let Some(desc) = &doc.description {
        html.push_str(&format!("<p>{}</p>\n", html_escape(desc)));
    }

    // Metadata
    html.push_str("<div class=\"metadata\">\n");
    html.push_str(&format!(
        "<p><strong>Schema:</strong> {}</p>\n",
        html_escape(&doc.schema)
    ));
    if let Some(owner) = &doc.owner {
        html.push_str(&format!(
            "<p><strong>Owner:</strong> {}</p>\n",
            html_escape(owner)
        ));
    }
    if !doc.tags.is_empty() {
        let escaped_tags: Vec<String> = doc.tags.iter().map(|t| html_escape(t)).collect();
        html.push_str(&format!(
            "<p><strong>Tags:</strong> {}</p>\n",
            escaped_tags.join(", ")
        ));
    }
    html.push_str("</div>\n");

    // Tables
    html.push_str("<h2>Tables</h2>\n");
    for table in &doc.tables {
        html.push_str(&format!("<h3>{}</h3>\n", table.name));
        if let Some(desc) = &table.description {
            html.push_str(&format!("<p>{}</p>\n", html_escape(desc)));
        }

        if !table.columns.is_empty() {
            html.push_str("<table>\n<thead><tr><th>Column</th><th>Type</th><th>Description</th><th>Tests</th></tr></thead>\n<tbody>\n");

            for col in &table.columns {
                let data_type = col.data_type.as_str();
                let desc = col.description.as_deref().unwrap_or("-");
                let tests = if col.tests.is_empty() {
                    "-".to_string()
                } else {
                    col.tests.join(", ")
                };
                html.push_str(&format!(
                    "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                    col.name,
                    html_escape(data_type),
                    html_escape(desc),
                    tests
                ));
            }
            html.push_str("</tbody></table>\n");
        }
    }

    html.push_str("</body>\n</html>\n");
    html
}

/// Generate HTML index file
fn generate_index_html(
    project_name: &str,
    models: &[ModelSummary],
    sources: &[SourceSummary],
) -> String {
    let mut html = String::new();

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    html.push_str(&format!(
        "<meta charset=\"UTF-8\">\n<title>{} Documentation</title>\n",
        project_name
    ));
    html.push_str(html_styles());
    html.push_str("</head>\n<body>\n");

    html.push_str(&format!("<h1>{} Documentation</h1>\n", project_name));

    let with_schema = models.iter().filter(|m| m.has_schema).count();
    let without_schema = models.len() - with_schema;

    html.push_str("<div class=\"metadata\">\n");
    html.push_str(&format!(
        "<p><strong>Models:</strong> {} total ({} with schema, {} without)</p>\n",
        models.len(),
        with_schema,
        without_schema
    ));

    if !sources.is_empty() {
        let total_tables: usize = sources.iter().map(|s| s.table_count).sum();
        html.push_str(&format!(
            "<p><strong>Sources:</strong> {} ({} tables)</p>\n",
            sources.len(),
            total_tables
        ));
    }

    let macro_count = get_builtin_macros().len();
    html.push_str(&format!(
        "<p><strong>Macros:</strong> {} built-in macros (<a href=\"macros.html\">view documentation</a>)</p>\n",
        macro_count
    ));

    html.push_str("</div>\n");

    html.push_str("<h2>Models</h2>\n");
    html.push_str("<table>\n<thead><tr><th>Model</th><th>Description</th><th>Owner</th><th>Has Schema</th></tr></thead>\n<tbody>\n");

    for model in models {
        let desc = model.description.as_deref().unwrap_or("-");
        let owner = model
            .owner
            .as_ref()
            .map(|o| html_escape(o))
            .unwrap_or_else(|| "-".to_string());
        let has_schema = if model.has_schema {
            "<span class=\"badge badge-schema\">Yes</span>"
        } else {
            "-"
        };
        html.push_str(&format!(
            "<tr><td><a href=\"{}.html\">{}</a></td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
            url_encode_path(&model.name),
            html_escape(&model.name),
            html_escape(desc),
            owner,
            has_schema
        ));
    }
    html.push_str("</tbody></table>\n");

    if !sources.is_empty() {
        html.push_str("<h2>Sources</h2>\n");
        html.push_str("<table>\n<thead><tr><th>Source</th><th>Description</th><th>Tables</th></tr></thead>\n<tbody>\n");

        for source in sources {
            let desc = source.description.as_deref().unwrap_or("-");
            html.push_str(&format!(
                "<tr><td><a href=\"source_{}.html\">{}</a></td><td>{}</td><td>{}</td></tr>\n",
                url_encode_path(&source.name),
                html_escape(&source.name),
                html_escape(desc),
                source.table_count
            ));
        }
        html.push_str("</tbody></table>\n");
    }

    html.push_str("</body>\n</html>\n");
    html
}

/// Escape HTML special characters
pub(super) fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Percent-encode a value for safe use in a URL path segment (href attribute).
///
/// Encodes everything except unreserved characters (`A-Z`, `a-z`, `0-9`, `-`, `_`, `.`, `~`).
fn url_encode_path(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(b as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", b));
            }
        }
    }
    encoded
}

/// Generate a DOT file for the model lineage graph
fn generate_lineage_dot(project: &Project) -> String {
    let mut dot = String::new();

    // Try to load manifest for dependency information
    let manifest = ff_core::manifest::Manifest::load(&project.manifest_path()).ok();

    dot.push_str("digraph lineage {\n");
    dot.push_str("    rankdir=LR;\n");
    dot.push_str("    node [shape=box, style=filled];\n\n");

    // Define node styles for different types
    dot.push_str("    // External/source nodes (grey)\n");
    for source in &project.sources {
        for table in &source.tables {
            let node_name = format!("{}_{}", source.name, table.name);
            dot.push_str(&format!(
                "    \"{}\" [label=\"{}.{}\" fillcolor=\"#d3d3d3\"];\n",
                node_name, source.schema, table.name
            ));
        }
    }

    dot.push_str(
        "\n    // Model nodes (blue for views, green for tables, gold for incremental, gray for ephemeral)\n",
    );
    if let Some(ref manifest) = manifest {
        for (name, model) in &manifest.models {
            let color = match model.materialized {
                ff_core::config::Materialization::Table => "#90EE90", // light green
                ff_core::config::Materialization::View => "#ADD8E6",  // light blue
                ff_core::config::Materialization::Incremental => "#FFD700", // gold
                ff_core::config::Materialization::Ephemeral => "#E8E8E8", // light gray
            };
            dot.push_str(&format!(
                "    \"{}\" [label=\"{}\" fillcolor=\"{}\"];\n",
                name, name, color
            ));
        }
    } else {
        for (name, model) in &project.models {
            let mat = model.materialization(project.config.materialization);
            let color = match mat {
                ff_core::config::Materialization::Table => "#90EE90", // light green
                ff_core::config::Materialization::View => "#ADD8E6",  // light blue
                ff_core::config::Materialization::Incremental => "#FFD700", // gold
                ff_core::config::Materialization::Ephemeral => "#E8E8E8", // light gray
            };
            dot.push_str(&format!(
                "    \"{}\" [label=\"{}\" fillcolor=\"{}\"];\n",
                name, name, color
            ));
        }
    }

    dot.push_str("\n    // Dependencies (edges)\n");
    if let Some(ref manifest) = manifest {
        // Use manifest for accurate dependencies
        for (name, model) in &manifest.models {
            // Model dependencies
            for dep in &model.depends_on {
                dot.push_str(&format!("    \"{}\" -> \"{}\";\n", dep, name));
            }

            // External/source dependencies
            for ext in &model.external_deps {
                // Try to find matching source table
                let source_node = project
                    .sources
                    .iter()
                    .flat_map(|s| s.tables.iter().map(move |t| (s, t)))
                    .find(|(_, t)| *ext == t.name)
                    .map(|(s, t)| format!("{}_{}", s.name, t.name))
                    .unwrap_or_else(|| ext.to_string());

                dot.push_str(&format!("    \"{}\" -> \"{}\";\n", source_node, name));
            }
        }
    } else {
        // Fall back to project model info (may be incomplete)
        for (name, model) in &project.models {
            for dep in &model.depends_on {
                dot.push_str(&format!("    \"{}\" -> \"{}\";\n", dep, name));
            }
            for ext in &model.external_deps {
                let source_node = project
                    .sources
                    .iter()
                    .flat_map(|s| s.tables.iter().map(move |t| (s, t)))
                    .find(|(_, t)| *ext == t.name)
                    .map(|(s, t)| format!("{}_{}", s.name, t.name))
                    .unwrap_or_else(|| ext.to_string());

                dot.push_str(&format!("    \"{}\" -> \"{}\";\n", source_node, name));
            }
        }
    }

    dot.push_str("}\n");
    dot
}

/// Format a snake_case category string as title case (e.g. "date_time" -> "Date time").
fn format_category_title(category: &str) -> String {
    category
        .chars()
        .enumerate()
        .map(|(i, c)| {
            if i == 0 {
                c.to_uppercase().next().unwrap_or(c)
            } else if c == '_' {
                ' '
            } else {
                c
            }
        })
        .collect()
}

/// Generate markdown documentation for built-in macros
fn generate_macros_markdown() -> String {
    let mut md = String::new();

    md.push_str("# Built-in Macros\n\n");
    md.push_str(
        "Featherflow provides a set of built-in macros that are available in all templates.\n\n",
    );

    // Get all macros grouped by category
    let categories = get_macro_categories();
    let all_macros = get_builtin_macros();

    for category in &categories {
        let category_macros: Vec<&MacroMetadata> = all_macros
            .iter()
            .filter(|m| &m.category == category)
            .collect();

        if category_macros.is_empty() {
            continue;
        }

        let category_title = format_category_title(category);

        md.push_str(&format!("## {} Macros\n\n", category_title));

        for macro_info in category_macros {
            md.push_str(&format!("### `{}`\n\n", macro_info.name));
            md.push_str(&format!("{}\n\n", macro_info.description));

            // Parameters
            if !macro_info.params.is_empty() {
                md.push_str("**Parameters:**\n\n");
                md.push_str("| Parameter | Type | Required | Description |\n");
                md.push_str("|-----------|------|----------|-------------|\n");
                for param in &macro_info.params {
                    let required = if param.required { "Yes" } else { "No" };
                    md.push_str(&format!(
                        "| `{}` | {} | {} | {} |\n",
                        param.name, param.param_type, required, param.description
                    ));
                }
                md.push('\n');
            }

            // Example
            md.push_str("**Example:**\n\n");
            md.push_str(&format!("```jinja\n{}\n```\n\n", macro_info.example));
            md.push_str("**Output:**\n\n");
            md.push_str(&format!("```sql\n{}\n```\n\n", macro_info.example_output));
            md.push_str("---\n\n");
        }
    }

    md
}

/// Generate HTML documentation for built-in macros
fn generate_macros_html() -> String {
    let mut html = String::new();

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    html.push_str("<meta charset=\"UTF-8\">\n<title>Built-in Macros</title>\n");
    html.push_str(html_styles());
    html.push_str(
        r#"
    <style>
        .macro-section { margin-bottom: 30px; }
        .macro-card { background: #f9f9f9; border: 1px solid #ddd; padding: 20px; margin: 15px 0; border-radius: 5px; }
        .macro-name { font-size: 1.3em; color: #2c3e50; margin-bottom: 10px; }
        .param-table { margin: 15px 0; }
        pre { background: #2d2d2d; color: #f8f8f2; padding: 15px; border-radius: 5px; overflow-x: auto; }
        code { font-family: 'Consolas', 'Monaco', monospace; }
        .example-label { font-weight: bold; margin-top: 15px; display: block; }
    </style>
"#,
    );
    html.push_str("</head>\n<body>\n");

    // Navigation
    html.push_str("<nav><a href=\"index.html\">Home</a></nav>\n");

    html.push_str("<h1>Built-in Macros</h1>\n");
    html.push_str("<p>Featherflow provides a set of built-in macros that are available in all templates.</p>\n");

    // Get all macros grouped by category
    let categories = get_macro_categories();
    let all_macros = get_builtin_macros();

    for category in &categories {
        let category_macros: Vec<&MacroMetadata> = all_macros
            .iter()
            .filter(|m| &m.category == category)
            .collect();

        if category_macros.is_empty() {
            continue;
        }

        let category_title = format_category_title(category);

        html.push_str(&format!(
            "<div class=\"macro-section\">\n<h2>{} Macros</h2>\n",
            category_title
        ));

        for macro_info in category_macros {
            html.push_str("<div class=\"macro-card\">\n");
            html.push_str(&format!(
                "<div class=\"macro-name\"><code>{}</code></div>\n",
                macro_info.name
            ));
            html.push_str(&format!("<p>{}</p>\n", html_escape(macro_info.description)));

            // Parameters
            if !macro_info.params.is_empty() {
                html.push_str("<table class=\"param-table\">\n");
                html.push_str("<thead><tr><th>Parameter</th><th>Type</th><th>Required</th><th>Description</th></tr></thead>\n<tbody>\n");
                for param in &macro_info.params {
                    let required = if param.required {
                        "<span class=\"badge badge-pass\">Yes</span>"
                    } else {
                        "No"
                    };
                    html.push_str(&format!(
                        "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                        param.name,
                        param.param_type,
                        required,
                        html_escape(param.description)
                    ));
                }
                html.push_str("</tbody></table>\n");
            }

            // Example
            html.push_str("<span class=\"example-label\">Example:</span>\n");
            html.push_str(&format!(
                "<pre><code>{}</code></pre>\n",
                html_escape(macro_info.example)
            ));
            html.push_str("<span class=\"example-label\">Output:</span>\n");
            html.push_str(&format!(
                "<pre><code>{}</code></pre>\n",
                html_escape(macro_info.example_output)
            ));

            html.push_str("</div>\n");
        }

        html.push_str("</div>\n");
    }

    html.push_str("</body>\n</html>\n");
    html
}
