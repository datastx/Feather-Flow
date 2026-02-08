/**
 * Feather-Flow DAG Graph Renderer
 *
 * Uses HTML elements for nodes (crisp text, CSS styling) and SVG for edges
 * (smooth bezier curves with arrow markers). Pan/zoom via CSS transform.
 *
 * Layout: layered DAG, left-to-right (sources -> staging -> intermediate -> marts).
 */

export class GraphRenderer {
  constructor(container, projectData, onNodeClick) {
    this.container = container;
    this.viewport = container.querySelector('#graph-viewport');
    this.edgesSvg = container.querySelector('#graph-edges');
    this.nodesContainer = container.querySelector('#graph-nodes');
    this.onNodeClick = onNodeClick;

    this.nodes = new Map();   // name -> { x, y, w, h, layer, ... }
    this.edges = [];          // [{ from, to }]
    this.nodeElements = new Map(); // name -> DOM element
    this.edgeElements = [];   // [{ path, from, to }]
    this.selectedNode = null;

    // Camera (pan/zoom applied via CSS transform on viewport)
    this.pan = { x: 0, y: 0 };
    this.zoom = 1;
    this.isDragging = false;
    this.dragStart = { x: 0, y: 0 };
    this.panStart = { x: 0, y: 0 };

    // Layout constants
    this.nodeW = 200;
    this.nodeH = 52;
    this.layerGap = 280;
    this.nodeGap = 24;

    // Build and render
    this.buildGraph(projectData);
    this.layoutDAG();
    this.renderNodes();
    this.renderEdges();
    this.setupEvents();

    // Fit after layout settles — retry until container has dimensions
    this.schedulefit();
  }

  schedulefit(attempt = 0) {
    requestAnimationFrame(() => {
      const cw = this.container.clientWidth;
      const ch = this.container.clientHeight;
      if ((cw === 0 || ch === 0) && attempt < 10) {
        // Container not laid out yet, retry
        setTimeout(() => this.schedulefit(attempt + 1), 50);
        return;
      }
      this.fitToView();
    });
  }

  // ============================================================
  // Graph Building
  // ============================================================

  buildGraph(data) {
    // Model nodes
    for (const model of data.models) {
      this.nodes.set(model.name, {
        name: model.name,
        type: 'model',
        prefix: this.getPrefix(model.name),
        description: model.description || '',
        columnCount: model.column_count || 0,
        testCount: model.test_count || 0,
        x: 0, y: 0, w: this.nodeW, h: this.nodeH, layer: 0,
      });
    }

    // Infer source nodes from edges
    const modelNames = new Set(data.models.map(m => m.name));
    for (const edge of data.edges) {
      if (!modelNames.has(edge.from) && !this.nodes.has(edge.from)) {
        // Shorten "analytics.raw_foo" to "raw_foo" for display
        const shortName = edge.from.includes('.')
          ? edge.from.split('.').pop()
          : edge.from;
        this.nodes.set(edge.from, {
          name: edge.from,
          displayName: shortName,
          type: 'source',
          prefix: 'src',
          description: 'External source table',
          columnCount: 0,
          testCount: 0,
          x: 0, y: 0, w: this.nodeW, h: this.nodeH, layer: 0,
        });
      }
    }

    this.edges = data.edges.map(e => ({ from: e.from, to: e.to }));
  }

  getPrefix(name) {
    const idx = name.indexOf('_');
    if (idx > 0 && idx < 5) return name.substring(0, idx);
    return 'other';
  }

  // ============================================================
  // Layered DAG Layout
  // ============================================================

  layoutDAG() {
    // Build adjacency lists
    const children = new Map();
    const parents = new Map();
    for (const name of this.nodes.keys()) {
      children.set(name, []);
      parents.set(name, []);
    }
    for (const edge of this.edges) {
      if (children.has(edge.from) && parents.has(edge.to)) {
        children.get(edge.from).push(edge.to);
        parents.get(edge.to).push(edge.from);
      }
    }

    // Assign layers via longest path from roots
    const layers = new Map();
    const visited = new Set();

    const assignLayer = (name) => {
      if (visited.has(name)) return layers.get(name) || 0;
      visited.add(name);
      const pars = parents.get(name) || [];
      if (pars.length === 0) {
        layers.set(name, 0);
        return 0;
      }
      let maxParent = 0;
      for (const p of pars) {
        maxParent = Math.max(maxParent, assignLayer(p));
      }
      const layer = maxParent + 1;
      layers.set(name, layer);
      return layer;
    };

    for (const name of this.nodes.keys()) assignLayer(name);

    // Group by layer
    const layerGroups = new Map();
    let maxLayer = 0;
    for (const [name, layer] of layers) {
      if (!layerGroups.has(layer)) layerGroups.set(layer, []);
      layerGroups.get(layer).push(name);
      maxLayer = Math.max(maxLayer, layer);
    }

    // Initial Y positioning
    for (let layer = 0; layer <= maxLayer; layer++) {
      const group = layerGroups.get(layer) || [];
      for (let i = 0; i < group.length; i++) {
        const node = this.nodes.get(group[i]);
        if (node) {
          node.layer = layer;
          node.x = layer * this.layerGap;
          node.y = i * (this.nodeH + this.nodeGap);
        }
      }
    }

    // Barycenter ordering passes to minimize edge crossings
    for (let pass = 0; pass < 8; pass++) {
      // Forward pass
      for (let layer = 1; layer <= maxLayer; layer++) {
        this.reorderLayer(layerGroups.get(layer) || [], parents);
      }
      // Backward pass
      for (let layer = maxLayer - 1; layer >= 0; layer--) {
        this.reorderLayer(layerGroups.get(layer) || [], children);
      }
    }
  }

  reorderLayer(group, adjacency) {
    // Sort by average position of connected nodes
    group.sort((a, b) => {
      const aAdj = adjacency.get(a) || [];
      const bAdj = adjacency.get(b) || [];
      const aCenter = aAdj.length > 0
        ? aAdj.reduce((sum, p) => sum + (this.nodes.get(p)?.y || 0), 0) / aAdj.length
        : (this.nodes.get(a)?.y || 0);
      const bCenter = bAdj.length > 0
        ? bAdj.reduce((sum, p) => sum + (this.nodes.get(p)?.y || 0), 0) / bAdj.length
        : (this.nodes.get(b)?.y || 0);
      return aCenter - bCenter;
    });
    // Re-assign Y positions
    for (let i = 0; i < group.length; i++) {
      const node = this.nodes.get(group[i]);
      if (node) node.y = i * (this.nodeH + this.nodeGap);
    }
  }

  // ============================================================
  // DOM Rendering — Nodes
  // ============================================================

  renderNodes() {
    this.nodesContainer.innerHTML = '';
    this.nodeElements.clear();

    for (const node of this.nodes.values()) {
      const el = document.createElement('div');
      el.className = `graph-node graph-node--${node.prefix}`;
      if (node.type === 'source') el.classList.add('graph-node--source');
      el.dataset.name = node.name;

      el.style.left = node.x + 'px';
      el.style.top = node.y + 'px';
      el.style.width = node.w + 'px';
      el.style.height = node.h + 'px';

      const label = node.displayName || node.name;
      const subtitle = node.type === 'source'
        ? 'source table'
        : `${node.columnCount} cols \u00b7 ${node.testCount} tests`;

      el.innerHTML = `
        <div class="graph-node__color"></div>
        <div class="graph-node__content">
          <div class="graph-node__name">${this.escapeHtml(label)}</div>
          <div class="graph-node__sub">${subtitle}</div>
        </div>
      `;

      el.addEventListener('click', (e) => {
        e.stopPropagation();
        if (node.type === 'model') {
          this.onNodeClick(node.name);
        }
      });

      this.nodesContainer.appendChild(el);
      this.nodeElements.set(node.name, el);
    }
  }

  // ============================================================
  // SVG Rendering — Edges
  // ============================================================

  renderEdges() {
    // Clear existing paths (keep defs)
    const defs = this.edgesSvg.querySelector('defs');
    this.edgesSvg.innerHTML = '';
    this.edgesSvg.appendChild(defs);
    this.edgeElements = [];

    // Compute SVG viewBox to cover all nodes
    const bounds = this.getGraphBounds();
    const pad = 60;
    this.edgesSvg.setAttribute('viewBox',
      `${bounds.minX - pad} ${bounds.minY - pad} ${bounds.width + pad * 2} ${bounds.height + pad * 2}`);
    this.edgesSvg.style.width = (bounds.width + pad * 2) + 'px';
    this.edgesSvg.style.height = (bounds.height + pad * 2) + 'px';
    this.edgesSvg.style.left = (bounds.minX - pad) + 'px';
    this.edgesSvg.style.top = (bounds.minY - pad) + 'px';

    for (const edge of this.edges) {
      const fromNode = this.nodes.get(edge.from);
      const toNode = this.nodes.get(edge.to);
      if (!fromNode || !toNode) continue;

      // Right center of source -> left center of target
      const x1 = fromNode.x + fromNode.w;
      const y1 = fromNode.y + fromNode.h / 2;
      const x2 = toNode.x;
      const y2 = toNode.y + toNode.h / 2;

      // Bezier control points — horizontal offset proportional to distance
      const dx = Math.abs(x2 - x1);
      const cp = Math.max(60, dx * 0.4);

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', `M ${x1} ${y1} C ${x1 + cp} ${y1}, ${x2 - cp} ${y2}, ${x2} ${y2}`);
      path.classList.add('graph-edge');
      path.setAttribute('marker-end', 'url(#arrow)');

      this.edgesSvg.appendChild(path);
      this.edgeElements.push({ path, from: edge.from, to: edge.to });
    }
  }

  // ============================================================
  // Selection & Highlighting
  // ============================================================

  selectNode(name) {
    this.selectedNode = name;
    this.updateHighlights();
  }

  updateHighlights() {
    const sel = this.selectedNode;

    // Reset all node states
    for (const [name, el] of this.nodeElements) {
      el.classList.remove('graph-node--selected', 'graph-node--dimmed', 'graph-node--connected');
    }

    // Reset all edge states
    for (const { path } of this.edgeElements) {
      path.classList.remove('graph-edge--highlight', 'graph-edge--dimmed');
      path.setAttribute('marker-end', 'url(#arrow)');
    }

    if (!sel) return;

    // Find connected set (upstream + downstream)
    const connected = this.getConnected(sel);

    // Apply node states
    for (const [name, el] of this.nodeElements) {
      if (name === sel) {
        el.classList.add('graph-node--selected');
      } else if (connected.has(name)) {
        el.classList.add('graph-node--connected');
      } else {
        el.classList.add('graph-node--dimmed');
      }
    }

    // Apply edge states
    for (const { path, from, to } of this.edgeElements) {
      if (connected.has(from) && connected.has(to)) {
        path.classList.add('graph-edge--highlight');
        path.setAttribute('marker-end', 'url(#arrow-highlight)');
      } else {
        path.classList.add('graph-edge--dimmed');
        path.setAttribute('marker-end', 'url(#arrow-dimmed)');
      }
    }
  }

  getConnected(name) {
    const connected = new Set();
    const childMap = new Map();
    const parentMap = new Map();

    for (const n of this.nodes.keys()) {
      childMap.set(n, []);
      parentMap.set(n, []);
    }
    for (const edge of this.edges) {
      if (childMap.has(edge.from)) childMap.get(edge.from).push(edge.to);
      if (parentMap.has(edge.to)) parentMap.get(edge.to).push(edge.from);
    }

    // BFS upstream
    const queue = [name];
    while (queue.length) {
      const cur = queue.pop();
      if (connected.has(cur)) continue;
      connected.add(cur);
      for (const p of parentMap.get(cur) || []) queue.push(p);
    }

    // BFS downstream
    const queue2 = [name];
    while (queue2.length) {
      const cur = queue2.pop();
      if (connected.has(cur)) continue;
      connected.add(cur);
      for (const c of childMap.get(cur) || []) queue2.push(c);
    }

    return connected;
  }

  // ============================================================
  // Pan / Zoom
  // ============================================================

  setupEvents() {
    // Pan: drag on container background
    this.container.addEventListener('mousedown', (e) => {
      // Only start drag if clicking on the container background (not a node)
      if (e.target.closest('.graph-node') || e.target.closest('.graph-controls')) return;
      this.isDragging = true;
      this.dragStart = { x: e.clientX, y: e.clientY };
      this.panStart = { ...this.pan };
      this.container.style.cursor = 'grabbing';
      e.preventDefault();
    });

    window.addEventListener('mousemove', (e) => {
      if (!this.isDragging) return;
      this.pan.x = this.panStart.x + (e.clientX - this.dragStart.x);
      this.pan.y = this.panStart.y + (e.clientY - this.dragStart.y);
      this.applyTransform();
    });

    window.addEventListener('mouseup', () => {
      if (this.isDragging) {
        this.isDragging = false;
        this.container.style.cursor = '';
      }
    });

    // Zoom: scroll wheel
    this.container.addEventListener('wheel', (e) => {
      e.preventDefault();
      const rect = this.container.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;

      const oldZoom = this.zoom;
      const factor = e.deltaY > 0 ? 0.92 : 1.08;
      this.zoom = Math.max(0.15, Math.min(4, this.zoom * factor));

      // Zoom toward mouse position
      this.pan.x = mx - (mx - this.pan.x) * (this.zoom / oldZoom);
      this.pan.y = my - (my - this.pan.y) * (this.zoom / oldZoom);

      this.applyTransform();
    }, { passive: false });

    // Click on background to deselect
    this.container.addEventListener('click', (e) => {
      if (!e.target.closest('.graph-node') && !e.target.closest('.graph-controls')) {
        this.selectNode(null);
        this.onNodeClick(null);
      }
    });

    // Resize observer
    const ro = new ResizeObserver(() => {
      // Refit if it's the first render
    });
    ro.observe(this.container);
  }

  applyTransform() {
    this.viewport.style.transform =
      `translate(${this.pan.x}px, ${this.pan.y}px) scale(${this.zoom})`;
  }

  // ============================================================
  // View Controls
  // ============================================================

  fitToView() {
    const bounds = this.getGraphBounds();
    if (bounds.width === 0 && bounds.height === 0) return;

    const cw = this.container.clientWidth;
    const ch = this.container.clientHeight;
    const pad = 80;

    this.zoom = Math.min(
      (cw - pad * 2) / Math.max(bounds.width, 1),
      (ch - pad * 2) / Math.max(bounds.height, 1),
      1.5
    );

    // Center the graph in the container
    this.pan.x = (cw - bounds.width * this.zoom) / 2 - bounds.minX * this.zoom;
    this.pan.y = (ch - bounds.height * this.zoom) / 2 - bounds.minY * this.zoom;

    this.applyTransform();
  }

  zoomIn() {
    const cx = this.container.clientWidth / 2;
    const cy = this.container.clientHeight / 2;
    const oldZoom = this.zoom;
    this.zoom = Math.min(4, this.zoom * 1.25);
    this.pan.x = cx - (cx - this.pan.x) * (this.zoom / oldZoom);
    this.pan.y = cy - (cy - this.pan.y) * (this.zoom / oldZoom);
    this.applyTransform();
  }

  zoomOut() {
    const cx = this.container.clientWidth / 2;
    const cy = this.container.clientHeight / 2;
    const oldZoom = this.zoom;
    this.zoom = Math.max(0.15, this.zoom / 1.25);
    this.pan.x = cx - (cx - this.pan.x) * (this.zoom / oldZoom);
    this.pan.y = cy - (cy - this.pan.y) * (this.zoom / oldZoom);
    this.applyTransform();
  }

  // ============================================================
  // Helpers
  // ============================================================

  getGraphBounds() {
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const node of this.nodes.values()) {
      minX = Math.min(minX, node.x);
      minY = Math.min(minY, node.y);
      maxX = Math.max(maxX, node.x + node.w);
      maxY = Math.max(maxY, node.y + node.h);
    }
    if (!isFinite(minX)) return { minX: 0, minY: 0, width: 0, height: 0 };
    return { minX, minY, width: maxX - minX, height: maxY - minY };
  }

  escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
}
