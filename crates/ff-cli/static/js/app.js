/**
 * Feather-Flow Documentation Site - Main Application
 *
 * Entry point: fetches project index, initializes sidebar, search, theme, and graph.
 */

import { GraphRenderer } from './graph.js';

// -- State --
let projectData = null;
let searchIndex = null;
let graph = null;
let selectedModel = null;

// -- Init --
document.addEventListener('DOMContentLoaded', async () => {
  initTheme();
  await loadProjectData();
  initSidebar();
  initSearch();
  initGraph();
  initDetailPanel();
});

// ============================================================
// Data Loading
// ============================================================

async function loadProjectData() {
  try {
    const resp = await fetch('/api/index.json');
    projectData = await resp.json();

    document.getElementById('project-name').textContent = projectData.project_name;
    document.getElementById('stats').textContent =
      `${projectData.stats.total_models} models | ${projectData.stats.total_sources} sources | ${projectData.stats.total_columns} columns`;
    document.title = `${projectData.project_name} - Feather-Flow Docs`;
  } catch (err) {
    console.error('Failed to load project data:', err);
  }
}

async function loadSearchIndex() {
  if (searchIndex) return searchIndex;
  try {
    const resp = await fetch('/api/search-index.json');
    searchIndex = await resp.json();
  } catch (err) {
    console.error('Failed to load search index:', err);
    searchIndex = [];
  }
  return searchIndex;
}

async function loadModelDetail(name) {
  try {
    const resp = await fetch(`/api/models/${encodeURIComponent(name)}`);
    if (!resp.ok) return null;
    return await resp.json();
  } catch (err) {
    console.error(`Failed to load model ${name}:`, err);
    return null;
  }
}

// ============================================================
// Theme
// ============================================================

function initTheme() {
  const saved = localStorage.getItem('ff-theme');
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  const theme = saved || (prefersDark ? 'dark' : 'light');
  document.documentElement.setAttribute('data-theme', theme);

  document.getElementById('theme-toggle').addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('ff-theme', next);
    if (graph) graph.render();
  });
}

// ============================================================
// Sidebar
// ============================================================

function initSidebar() {
  if (!projectData) return;

  const list = document.getElementById('model-list');
  list.innerHTML = '';

  // Group models by prefix
  const groups = {};
  for (const model of projectData.models) {
    const prefix = getModelPrefix(model.name);
    if (!groups[prefix]) groups[prefix] = [];
    groups[prefix].push(model);
  }

  // Sort groups: stg_ first, then int_, then dim_, fct_, then others
  const order = ['stg', 'int', 'dim', 'fct'];
  const sortedKeys = Object.keys(groups).sort((a, b) => {
    const ai = order.indexOf(a);
    const bi = order.indexOf(b);
    if (ai >= 0 && bi >= 0) return ai - bi;
    if (ai >= 0) return -1;
    if (bi >= 0) return 1;
    return a.localeCompare(b);
  });

  for (const prefix of sortedKeys) {
    const label = document.createElement('div');
    label.className = 'model-group-label';
    label.textContent = prefix === '_other' ? 'Other' : prefix + '_*';
    list.appendChild(label);

    for (const model of groups[prefix].sort((a, b) => a.name.localeCompare(b.name))) {
      const item = document.createElement('div');
      item.className = 'model-item';
      item.dataset.name = model.name;

      const dot = document.createElement('span');
      dot.className = 'dot';
      dot.style.background = getNodeColor(model.name);

      const name = document.createElement('span');
      name.className = 'name';
      name.textContent = model.name;

      item.appendChild(dot);
      item.appendChild(name);

      item.addEventListener('click', () => selectModel(model.name));
      list.appendChild(item);
    }
  }

  // Sources
  if (projectData.sources.length > 0) {
    const section = document.getElementById('source-section');
    section.classList.remove('hidden');
    const sourceList = document.getElementById('source-list');
    sourceList.innerHTML = '';

    for (const source of projectData.sources) {
      const item = document.createElement('div');
      item.className = 'model-item';
      const dot = document.createElement('span');
      dot.className = 'dot';
      dot.style.background = 'var(--node-source)';
      const name = document.createElement('span');
      name.className = 'name';
      name.textContent = `${source.name} (${source.table_count} tables)`;
      item.appendChild(dot);
      item.appendChild(name);
      sourceList.appendChild(item);
    }
  }
}

function getModelPrefix(name) {
  const idx = name.indexOf('_');
  if (idx > 0 && idx < 5) return name.substring(0, idx);
  return '_other';
}

function getNodeColor(name) {
  const prefix = getModelPrefix(name);
  switch (prefix) {
    case 'stg': return 'var(--node-staging)';
    case 'int': return 'var(--node-intermediate)';
    case 'dim':
    case 'fct': return 'var(--node-model)';
    default: return 'var(--node-model)';
  }
}

// ============================================================
// Search
// ============================================================

function initSearch() {
  const input = document.getElementById('search-input');
  const results = document.getElementById('search-results');

  input.addEventListener('input', async () => {
    const query = input.value.trim().toLowerCase();
    if (query.length < 2) {
      results.classList.add('hidden');
      return;
    }

    const index = await loadSearchIndex();
    const matches = index.filter(entry => {
      if (entry.name.toLowerCase().includes(query)) return true;
      if (entry.description && entry.description.toLowerCase().includes(query)) return true;
      if (entry.columns.some(c => c.toLowerCase().includes(query))) return true;
      if (entry.tags.some(t => t.toLowerCase().includes(query))) return true;
      return false;
    }).slice(0, 10);

    results.innerHTML = '';
    if (matches.length === 0) {
      results.classList.add('hidden');
      return;
    }

    for (const match of matches) {
      const item = document.createElement('div');
      item.className = 'search-result-item';

      const type = document.createElement('span');
      type.className = 'search-result-type';
      type.textContent = match.resource_type;

      const name = document.createElement('span');
      name.className = 'search-result-name';
      name.textContent = match.name;

      item.appendChild(type);
      item.appendChild(name);

      if (match.description) {
        const desc = document.createElement('span');
        desc.className = 'search-result-desc';
        desc.textContent = match.description.substring(0, 60);
        item.appendChild(desc);
      }

      item.addEventListener('click', () => {
        if (match.resource_type === 'model') {
          selectModel(match.name);
        }
        results.classList.add('hidden');
        input.value = '';
      });

      results.appendChild(item);
    }
    results.classList.remove('hidden');
  });

  // Close on click outside
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-container')) {
      results.classList.add('hidden');
    }
  });

  // Keyboard shortcut: / to focus search
  document.addEventListener('keydown', (e) => {
    if (e.key === '/' && document.activeElement !== input) {
      e.preventDefault();
      input.focus();
    }
    if (e.key === 'Escape') {
      results.classList.add('hidden');
      input.blur();
    }
  });
}

// ============================================================
// Graph
// ============================================================

function initGraph() {
  if (!projectData) return;

  const container = document.getElementById('graph-container');
  graph = new GraphRenderer(container, projectData, (name) => {
    if (name) {
      selectModel(name);
    } else {
      closeDetailPanel();
    }
  });

  // Controls
  document.getElementById('zoom-in').addEventListener('click', () => graph.zoomIn());
  document.getElementById('zoom-out').addEventListener('click', () => graph.zoomOut());
  document.getElementById('zoom-fit').addEventListener('click', () => graph.fitToView());
}

// ============================================================
// Detail Panel
// ============================================================

function initDetailPanel() {
  document.getElementById('detail-close').addEventListener('click', () => {
    closeDetailPanel();
  });
}

async function selectModel(name) {
  selectedModel = name;

  // Highlight in sidebar
  document.querySelectorAll('.model-item').forEach(el => {
    el.classList.toggle('active', el.dataset.name === name);
  });

  // Highlight in graph
  if (graph) graph.selectNode(name);

  // Load detail
  const detail = await loadModelDetail(name);
  if (detail) showDetailPanel(detail);
}

function showDetailPanel(doc) {
  const panel = document.getElementById('detail-panel');
  const title = document.getElementById('detail-title');
  const content = document.getElementById('detail-content');

  title.textContent = doc.name;
  content.innerHTML = buildDetailHTML(doc);
  panel.classList.remove('hidden');
}

function closeDetailPanel() {
  document.getElementById('detail-panel').classList.add('hidden');
  selectedModel = null;
  document.querySelectorAll('.model-item.active').forEach(el => el.classList.remove('active'));
  if (graph) graph.selectNode(null);
}

function buildDetailHTML(doc) {
  let html = '';

  // Description
  if (doc.description) {
    html += `<p class="detail-description">${escapeHtml(doc.description)}</p>`;
  }

  // Metadata
  html += '<div class="detail-section"><h3>Metadata</h3><dl class="detail-meta">';
  if (doc.owner) html += `<dt>Owner</dt><dd>${escapeHtml(doc.owner)}</dd>`;
  if (doc.team) html += `<dt>Team</dt><dd>${escapeHtml(doc.team)}</dd>`;
  if (doc.materialized) html += `<dt>Materialized</dt><dd><span class="badge">${doc.materialized}</span></dd>`;
  if (doc.schema) html += `<dt>Schema</dt><dd>${escapeHtml(doc.schema)}</dd>`;
  if (doc.tags && doc.tags.length) html += `<dt>Tags</dt><dd>${doc.tags.map(t => `<span class="badge badge-blue">${escapeHtml(t)}</span>`).join(' ')}</dd>`;
  html += '</dl></div>';

  // Dependencies
  if ((doc.depends_on && doc.depends_on.length) || (doc.external_deps && doc.external_deps.length)) {
    html += '<div class="detail-section"><h3>Dependencies</h3><ul class="dep-list">';
    for (const dep of doc.depends_on || []) {
      html += `<li><a href="#" data-model="${escapeHtml(dep)}" class="dep-link">${escapeHtml(dep)}</a></li>`;
    }
    for (const dep of doc.external_deps || []) {
      html += `<li>${escapeHtml(dep)} <span class="badge">external</span></li>`;
    }
    html += '</ul></div>';
  }

  // Columns
  if (doc.columns && doc.columns.length) {
    html += '<div class="detail-section"><h3>Columns</h3>';
    html += '<table class="columns-table"><thead><tr><th>Name</th><th>Type</th><th>Tests</th></tr></thead><tbody>';
    for (const col of doc.columns) {
      const tests = col.tests.length
        ? col.tests.map(t => `<span class="badge badge-green">${escapeHtml(t)}</span>`).join(' ')
        : '<span class="badge">none</span>';
      html += `<tr>
        <td><code>${escapeHtml(col.name)}</code>${col.primary_key ? ' <span class="badge badge-amber">PK</span>' : ''}</td>
        <td>${col.data_type ? escapeHtml(col.data_type) : '-'}</td>
        <td class="test-list">${tests}</td>
      </tr>`;
    }
    html += '</tbody></table></div>';
  }

  // Column Lineage
  if (doc.column_lineage && doc.column_lineage.length) {
    html += '<div class="detail-section"><h3>Column Lineage</h3>';
    html += '<table class="columns-table"><thead><tr><th>Output</th><th>Sources</th><th>Type</th></tr></thead><tbody>';
    for (const cl of doc.column_lineage) {
      const sources = cl.source_columns.length ? cl.source_columns.map(s => `<code>${escapeHtml(s)}</code>`).join(', ') : '-';
      html += `<tr><td><code>${escapeHtml(cl.output_column)}</code></td><td>${sources}</td><td>${escapeHtml(cl.expr_type)}</td></tr>`;
    }
    html += '</tbody></table></div>';
  }

  // Test Suggestions
  if (doc.test_suggestions && doc.test_suggestions.length) {
    html += '<div class="detail-section"><h3>Suggested Tests</h3>';
    html += '<table class="columns-table"><thead><tr><th>Column</th><th>Test</th><th>Reason</th></tr></thead><tbody>';
    for (const ts of doc.test_suggestions) {
      html += `<tr><td><code>${escapeHtml(ts.column)}</code></td><td><span class="badge badge-purple">${escapeHtml(ts.test_type)}</span></td><td>${escapeHtml(ts.reason)}</td></tr>`;
    }
    html += '</tbody></table></div>';
  }

  // SQL
  if (doc.raw_sql) {
    html += '<div class="detail-section"><h3>SQL</h3>';
    html += `<pre class="sql-block">${highlightSQL(escapeHtml(doc.raw_sql))}</pre>`;
    html += '</div>';
  }

  // Attach click handlers for dependency links after rendering
  setTimeout(() => {
    document.querySelectorAll('.dep-link').forEach(link => {
      link.addEventListener('click', (e) => {
        e.preventDefault();
        selectModel(link.dataset.model);
      });
    });
  }, 0);

  return html;
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function highlightSQL(html) {
  // Keywords
  const keywords = /\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|ON|AND|OR|NOT|IN|AS|GROUP|BY|ORDER|HAVING|LIMIT|OFFSET|UNION|ALL|INSERT|INTO|UPDATE|SET|DELETE|CREATE|TABLE|VIEW|DROP|ALTER|INDEX|WITH|CASE|WHEN|THEN|ELSE|END|IS|NULL|BETWEEN|LIKE|EXISTS|DISTINCT|COUNT|SUM|AVG|MIN|MAX|CAST|COALESCE|NULLIF|TRUE|FALSE|ASC|DESC|IF|OVER|PARTITION|ROWS|RANGE|PRECEDING|FOLLOWING|UNBOUNDED|CURRENT|ROW)\b/gi;
  html = html.replace(keywords, '<span class="sql-keyword">$1</span>');

  // Strings (already escaped so &quot; and &#39;)
  html = html.replace(/(&quot;.*?&quot;)/g, '<span class="sql-string">$1</span>');

  // Numbers
  html = html.replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="sql-number">$1</span>');

  // Comments
  html = html.replace(/(--.*?)(?=\n|$)/g, '<span class="sql-comment">$1</span>');

  return html;
}

// Export for graph module
export { selectModel };
