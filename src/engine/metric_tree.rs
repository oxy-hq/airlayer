use crate::engine::member_sql::MemberSqlResolver;
use crate::schema::models::{
    DriverConfidence, DriverDirection, DriverStrength, MeasureType, SemanticLayer,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};

/// A node in the metric tree.
#[derive(Debug, Clone, Serialize)]
pub struct MetricNode {
    /// Fully qualified name (e.g., "orders.total_revenue").
    pub id: String,
    /// View name.
    pub view: String,
    /// Measure name.
    pub measure: String,
    /// Human-readable label (from measure description or name).
    pub label: String,
    /// Description from the measure definition.
    pub description: Option<String>,
    /// Measure type (sum, count, number, etc.).
    pub measure_type: String,
    /// Whether this is an atomic measure (has a direct aggregation) or composite (type: number).
    pub is_composite: bool,
}

/// The type of edge in the metric tree.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EdgeKind {
    /// Mathematical identity: parent's expression references this child.
    Component,
    /// Explicit driver relationship (correlative/causal).
    Driver,
}

impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EdgeKind::Component => write!(f, "component"),
            EdgeKind::Driver => write!(f, "driver"),
        }
    }
}

/// An edge in the metric tree.
#[derive(Debug, Clone, Serialize)]
pub struct MetricEdge {
    /// Source (driver/component) measure ID.
    pub from: String,
    /// Target (driven) measure ID.
    pub to: String,
    /// Type of relationship.
    pub kind: EdgeKind,
    /// Direction (for driver edges).
    pub direction: DriverDirection,
    /// Strength (for driver edges).
    pub strength: DriverStrength,
    /// Confidence (for driver edges).
    pub confidence: DriverConfidence,
    /// Description.
    pub description: Option<String>,
    /// Supporting references.
    pub refs: Option<Vec<String>>,
}

/// The full metric tree graph.
#[derive(Debug, Clone, Serialize)]
pub struct MetricTree {
    pub nodes: Vec<MetricNode>,
    pub edges: Vec<MetricEdge>,
    /// The root measure ID (if specified).
    pub root: Option<String>,
}

/// Extract `{{view.measure}}` references from an expression.
/// Delegates to `MemberSqlResolver::extract_entity_refs` to avoid duplicating regex logic.
fn extract_measure_refs(expr: &str) -> Vec<String> {
    MemberSqlResolver::extract_entity_refs(expr)
        .into_iter()
        .map(|(entity, field)| format!("{}.{}", entity, field))
        .collect()
}

impl MetricTree {
    /// Build the metric tree from a semantic layer.
    ///
    /// The graph is constructed by:
    /// 1. Creating a node for every measure in every view.
    /// 2. Parsing `type: number` expressions for `{{view.measure}}` references → component edges.
    /// 3. Reading explicit `drivers` annotations → driver edges.
    pub fn build(layer: &SemanticLayer) -> Self {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut node_ids: HashSet<String> = HashSet::new();

        // Pass 1: collect all measure nodes
        for view in &layer.views {
            for measure in view.measures_list() {
                let id = format!("{}.{}", view.name, measure.name);
                nodes.push(MetricNode {
                    id: id.clone(),
                    view: view.name.clone(),
                    measure: measure.name.clone(),
                    label: measure
                        .description
                        .as_deref()
                        .unwrap_or(&measure.name)
                        .to_string(),
                    description: measure.description.clone(),
                    measure_type: measure.measure_type.to_string(),
                    is_composite: measure.measure_type == MeasureType::Number
                        || measure.measure_type == MeasureType::Custom,
                });
                node_ids.insert(id);
            }
        }

        // Pass 2: extract component edges from type: number expressions
        for view in &layer.views {
            for measure in view.measures_list() {
                if !measure.measure_type.is_passthrough() {
                    continue;
                }
                if let Some(ref expr) = measure.expr {
                    let target_id = format!("{}.{}", view.name, measure.name);
                    let refs = extract_measure_refs(expr);
                    for ref_id in refs {
                        if node_ids.contains(&ref_id) && ref_id != target_id {
                            edges.push(MetricEdge {
                                from: ref_id,
                                to: target_id.clone(),
                                kind: EdgeKind::Component,
                                direction: DriverDirection::default(),
                                strength: DriverStrength::Strong,
                                confidence: DriverConfidence::High,
                                description: None,
                                refs: None,
                            });
                        }
                    }
                }
            }
        }

        // Pass 3: extract driver edges from explicit annotations
        for view in &layer.views {
            for measure in view.measures_list() {
                if let Some(ref drivers) = measure.drivers {
                    let target_id = format!("{}.{}", view.name, measure.name);
                    for driver in drivers {
                        let from_id = &driver.measure;
                        // Validate the driver reference exists
                        if node_ids.contains(from_id) && *from_id != target_id {
                            edges.push(MetricEdge {
                                from: from_id.clone(),
                                to: target_id.clone(),
                                kind: EdgeKind::Driver,
                                direction: driver.direction.clone(),
                                strength: driver.strength.clone(),
                                confidence: driver.confidence.clone(),
                                description: driver.description.clone(),
                                refs: driver.refs.clone(),
                            });
                        }
                    }
                }
            }
        }

        MetricTree {
            nodes,
            edges,
            root: None,
        }
    }

    /// Build a subtree rooted at the given measure ID.
    /// Traverses both component and driver edges downward (from target to sources).
    pub fn subtree(&self, root_id: &str) -> Option<MetricTree> {
        if !self.nodes.iter().any(|n| n.id == root_id) {
            return None;
        }

        // Build reverse adjacency map: target -> [edge indices]
        let mut rev_adj: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, edge) in self.edges.iter().enumerate() {
            rev_adj.entry(edge.to.as_str()).or_default().push(i);
        }

        // BFS from root, following edges where `to == current` to find sources
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        queue.push_back(root_id.to_string());
        visited.insert(root_id.to_string());

        let mut subtree_edges = Vec::new();

        while let Some(current) = queue.pop_front() {
            if let Some(indices) = rev_adj.get(current.as_str()) {
                for &i in indices {
                    let edge = &self.edges[i];
                    if !visited.contains(&edge.from) {
                        visited.insert(edge.from.clone());
                        queue.push_back(edge.from.clone());
                        subtree_edges.push(edge.clone());
                    }
                }
            }
        }

        let subtree_nodes: Vec<MetricNode> = self
            .nodes
            .iter()
            .filter(|n| visited.contains(&n.id))
            .cloned()
            .collect();

        Some(MetricTree {
            nodes: subtree_nodes,
            edges: subtree_edges,
            root: Some(root_id.to_string()),
        })
    }

    /// Return all root measures (measures that are not a source for any other measure).
    /// These are candidate "North Star" metrics.
    pub fn roots(&self) -> Vec<&MetricNode> {
        let sources: HashSet<&str> = self.edges.iter().map(|e| e.from.as_str()).collect();
        let targets: HashSet<&str> = self.edges.iter().map(|e| e.to.as_str()).collect();

        // A root is a node that appears as a target (has inputs) but is not itself a source,
        // OR a node that has no edges at all but is composite.
        // More practically: nodes that are targets but not sources.
        self.nodes
            .iter()
            .filter(|n| targets.contains(n.id.as_str()) && !sources.contains(n.id.as_str()))
            .collect()
    }

    /// Return all leaf measures (measures that have no inputs — purely atomic).
    pub fn leaves(&self) -> Vec<&MetricNode> {
        let sources: HashSet<&str> = self.edges.iter().map(|e| e.from.as_str()).collect();
        let targets: HashSet<&str> = self.edges.iter().map(|e| e.to.as_str()).collect();

        self.nodes
            .iter()
            .filter(|n| sources.contains(n.id.as_str()) && !targets.contains(n.id.as_str()))
            .collect()
    }

    /// Get the direct inputs (children) for a given measure.
    pub fn inputs_of(&self, measure_id: &str) -> Vec<(&MetricNode, &MetricEdge)> {
        self.edges
            .iter()
            .filter(|e| e.to == measure_id)
            .filter_map(|e| self.nodes.iter().find(|n| n.id == e.from).map(|n| (n, e)))
            .collect()
    }

    /// Get the direct outputs (parents) for a given measure — what does this measure drive?
    pub fn outputs_of(&self, measure_id: &str) -> Vec<(&MetricNode, &MetricEdge)> {
        self.edges
            .iter()
            .filter(|e| e.from == measure_id)
            .filter_map(|e| self.nodes.iter().find(|n| n.id == e.to).map(|n| (n, e)))
            .collect()
    }

    /// Generate a standalone HTML visualization of the metric tree.
    pub fn to_html(&self) -> String {
        let tree_json =
            serde_json::to_string(self).expect("MetricTree should be serializable to JSON");
        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Metric Tree — airlayer</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0a0a0f; color: #e0e0e0; overflow: hidden; }}
  svg {{ display: block; }}
  .node-group {{ cursor: pointer; }}
  .node-rect {{ rx: 8; ry: 8; stroke-width: 1.5; }}
  .node-rect.composite {{ fill: #1a1a2e; stroke: #4a9eff; }}
  .node-rect.atomic {{ fill: #1a1a2e; stroke: #555; }}
  .node-rect.root {{ fill: #1a1a2e; stroke: #ffd700; stroke-width: 2.5; }}
  .node-rect:hover {{ filter: brightness(1.3); }}
  .node-label {{ fill: #e0e0e0; font-size: 12px; font-weight: 600; text-anchor: middle; pointer-events: none; }}
  .node-sublabel {{ fill: #888; font-size: 10px; text-anchor: middle; pointer-events: none; }}
  .edge-line {{ fill: none; stroke-width: 1.5; }}
  .edge-line.component {{ stroke: #4a9eff; }}
  .edge-line.driver {{ stroke: #888; }}
  .edge-line.strong {{ stroke-width: 2.5; }}
  .edge-line.moderate {{ stroke-width: 1.5; }}
  .edge-line.weak {{ stroke-width: 1; opacity: 0.6; }}
  .edge-line.confidence-high {{ stroke-dasharray: none; }}
  .edge-line.confidence-medium {{ stroke-dasharray: 6,3; }}
  .edge-line.confidence-low {{ stroke-dasharray: 3,3; }}
  .edge-line.positive {{ stroke: #4caf50; }}
  .edge-line.negative {{ stroke: #ef5350; }}

  /* Detail panel */
  #detail-panel {{
    position: fixed; top: 16px; right: 16px; width: 340px;
    background: #141420; border: 1px solid #333; border-radius: 12px;
    padding: 20px; display: none; z-index: 10;
    max-height: calc(100vh - 32px); overflow-y: auto;
  }}
  #detail-panel.visible {{ display: block; }}
  #detail-panel h2 {{ font-size: 16px; color: #fff; margin-bottom: 4px; }}
  #detail-panel .description {{ color: #aaa; font-size: 13px; margin-bottom: 12px; }}
  #detail-panel .meta-row {{ display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #222; font-size: 13px; }}
  #detail-panel .meta-label {{ color: #888; }}
  #detail-panel .meta-value {{ color: #e0e0e0; }}
  #detail-panel h3 {{ font-size: 13px; color: #888; margin: 12px 0 6px; text-transform: uppercase; letter-spacing: 0.5px; }}
  #detail-panel .rel-item {{ font-size: 13px; padding: 4px 0; }}
  #detail-panel .rel-item a {{ color: #4a9eff; text-decoration: none; }}
  #detail-panel .rel-item a:hover {{ text-decoration: underline; }}
  .badge {{ display: inline-block; padding: 1px 6px; border-radius: 4px; font-size: 11px; font-weight: 500; }}
  .badge.strong {{ background: #1b5e20; color: #a5d6a7; }}
  .badge.moderate {{ background: #4a3800; color: #ffd54f; }}
  .badge.weak {{ background: #4a1a1a; color: #ef9a9a; }}
  .badge.component {{ background: #0d47a1; color: #90caf9; }}
  .ref-link {{ display: block; color: #4a9eff; font-size: 12px; margin: 2px 0; text-decoration: none; word-break: break-all; }}
  .ref-link:hover {{ text-decoration: underline; }}

  /* Legend */
  #legend {{
    position: fixed; bottom: 16px; left: 16px; background: #141420;
    border: 1px solid #333; border-radius: 8px; padding: 12px 16px;
    font-size: 12px; z-index: 10;
  }}
  #legend .legend-title {{ font-weight: 600; margin-bottom: 6px; }}
  #legend .legend-row {{ display: flex; align-items: center; gap: 8px; margin: 3px 0; }}
  .legend-line {{ width: 30px; height: 0; border-top-width: 2px; border-top-style: solid; display: inline-block; }}
  .legend-swatch {{ width: 14px; height: 14px; border-radius: 3px; border: 1.5px solid; display: inline-block; }}

  #close-btn {{ position: absolute; top: 8px; right: 12px; background: none; border: none; color: #888; font-size: 18px; cursor: pointer; }}
  #close-btn:hover {{ color: #fff; }}
</style>
</head>
<body>
<svg id="canvas"></svg>

<div id="detail-panel">
  <button id="close-btn">&times;</button>
  <div id="detail-content"></div>
</div>

<div id="legend">
  <div class="legend-title">Legend</div>
  <div class="legend-row"><span class="legend-swatch" style="border-color: #ffd700; background: #1a1a2e;"></span> Root / North Star</div>
  <div class="legend-row"><span class="legend-swatch" style="border-color: #4a9eff; background: #1a1a2e;"></span> Composite metric</div>
  <div class="legend-row"><span class="legend-swatch" style="border-color: #555; background: #1a1a2e;"></span> Atomic metric</div>
  <div class="legend-row"><span class="legend-line" style="border-color: #4a9eff;"></span> Component (math identity)</div>
  <div class="legend-row"><span class="legend-line" style="border-color: #4caf50;"></span> Driver (positive)</div>
  <div class="legend-row"><span class="legend-line" style="border-color: #ef5350;"></span> Driver (negative)</div>
  <div class="legend-row"><span class="legend-line" style="border-color: #4caf50; border-top-style: dashed;"></span> Low confidence</div>
</div>

<script>
const DATA = {tree_json};

// ── Layout: top-down tree using BFS levels ──
const W = window.innerWidth, H = window.innerHeight;
const NODE_W = 160, NODE_H = 50, PAD_X = 40, PAD_Y = 80;

const svg = document.getElementById('canvas');
svg.setAttribute('width', W);
svg.setAttribute('height', H);

// Build adjacency: target -> [sources]
const adj = {{}};
DATA.edges.forEach(e => {{
  if (!adj[e.to]) adj[e.to] = [];
  adj[e.to].push(e.from);
}});

// Find roots (nodes that are targets but not sources in any edge)
const sources = new Set(DATA.edges.map(e => e.from));
const targets = new Set(DATA.edges.map(e => e.to));
let roots = DATA.nodes.filter(n => targets.has(n.id) && !sources.has(n.id)).map(n => n.id);
if (roots.length === 0) {{
  // Fallback: if DATA.root is set, use it; otherwise pick composites with most inputs
  if (DATA.root) roots = [DATA.root];
  else {{
    const inputCounts = {{}};
    DATA.edges.forEach(e => {{ inputCounts[e.to] = (inputCounts[e.to] || 0) + 1; }});
    const sorted = Object.entries(inputCounts).sort((a, b) => b[1] - a[1]);
    roots = sorted.length > 0 ? [sorted[0][0]] : (DATA.nodes.length > 0 ? [DATA.nodes[0].id] : []);
  }}
}}

// BFS to assign levels
const level = {{}};
const visited = new Set();
const queue = [];
roots.forEach(r => {{ level[r] = 0; visited.add(r); queue.push(r); }});
while (queue.length > 0) {{
  const cur = queue.shift();
  const children = adj[cur] || [];
  children.forEach(c => {{
    if (!visited.has(c)) {{
      visited.add(c);
      level[c] = (level[cur] || 0) + 1;
      queue.push(c);
    }}
  }});
}}
// Assign unvisited nodes to level 0
DATA.nodes.forEach(n => {{ if (level[n.id] === undefined) level[n.id] = 0; }});

// Group by level
const levels = {{}};
DATA.nodes.forEach(n => {{
  const l = level[n.id];
  if (!levels[l]) levels[l] = [];
  levels[l].push(n);
}});

const maxLevel = Math.max(...Object.keys(levels).map(Number), 0);

// Position nodes
const pos = {{}};
Object.entries(levels).forEach(([l, nodes]) => {{
  const lNum = Number(l);
  const totalW = nodes.length * (NODE_W + PAD_X) - PAD_X;
  const startX = (W - totalW) / 2;
  nodes.forEach((n, i) => {{
    pos[n.id] = {{
      x: startX + i * (NODE_W + PAD_X) + NODE_W / 2,
      y: 60 + lNum * (NODE_H + PAD_Y) + NODE_H / 2,
    }};
  }});
}});

const rootSet = new Set(roots);
const nodeById = {{}};
DATA.nodes.forEach(n => {{ nodeById[n.id] = n; }});

// ── Draw edges ──
const edgeG = document.createElementNS('http://www.w3.org/2000/svg', 'g');
svg.appendChild(edgeG);
DATA.edges.forEach(e => {{
  if (!pos[e.from] || !pos[e.to]) return;
  const p1 = pos[e.from], p2 = pos[e.to];
  const line = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  // Curved path
  const midY = (p1.y + p2.y) / 2;
  const d = `M${{p1.x}},${{p1.y - NODE_H/2}} C${{p1.x}},${{midY}} ${{p2.x}},${{midY}} ${{p2.x}},${{p2.y + NODE_H/2}}`;
  line.setAttribute('d', d);
  let cls = 'edge-line ' + e.kind;
  cls += ' ' + e.strength;
  cls += ' confidence-' + e.confidence;
  if (e.kind === 'driver' && e.direction !== 'unknown') cls += ' ' + e.direction;
  line.setAttribute('class', cls);
  // Arrow marker
  line.setAttribute('marker-end', 'url(#arrow)');
  edgeG.appendChild(line);
}});

// Arrow marker
const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
const marker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
marker.setAttribute('id', 'arrow');
marker.setAttribute('viewBox', '0 0 10 10');
marker.setAttribute('refX', '10');
marker.setAttribute('refY', '5');
marker.setAttribute('markerWidth', '8');
marker.setAttribute('markerHeight', '8');
marker.setAttribute('orient', 'auto-start-reverse');
const arrowPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
arrowPath.setAttribute('d', 'M 0 0 L 10 5 L 0 10 z');
arrowPath.setAttribute('fill', '#666');
marker.appendChild(arrowPath);
defs.appendChild(marker);
svg.insertBefore(defs, svg.firstChild);

// ── Draw nodes ──
const nodeG = document.createElementNS('http://www.w3.org/2000/svg', 'g');
svg.appendChild(nodeG);
DATA.nodes.forEach(n => {{
  if (!pos[n.id]) return;
  const p = pos[n.id];
  const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  g.setAttribute('class', 'node-group');
  g.setAttribute('transform', `translate(${{p.x}},${{p.y}})`);

  const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
  rect.setAttribute('x', -NODE_W/2);
  rect.setAttribute('y', -NODE_H/2);
  rect.setAttribute('width', NODE_W);
  rect.setAttribute('height', NODE_H);
  let cls = 'node-rect';
  if (rootSet.has(n.id)) cls += ' root';
  else if (n.is_composite) cls += ' composite';
  else cls += ' atomic';
  rect.setAttribute('class', cls);
  g.appendChild(rect);

  const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
  label.setAttribute('class', 'node-label');
  label.setAttribute('y', -4);
  label.textContent = n.measure.length > 18 ? n.measure.substring(0, 16) + '…' : n.measure;
  g.appendChild(label);

  const sub = document.createElementNS('http://www.w3.org/2000/svg', 'text');
  sub.setAttribute('class', 'node-sublabel');
  sub.setAttribute('y', 14);
  sub.textContent = n.view;
  g.appendChild(sub);

  g.addEventListener('click', () => showDetail(n));
  nodeG.appendChild(g);
}});

// ── Detail panel ──
const panel = document.getElementById('detail-panel');
const detailContent = document.getElementById('detail-content');
document.getElementById('close-btn').addEventListener('click', () => panel.classList.remove('visible'));

function showDetail(node) {{
  const inputs = DATA.edges.filter(e => e.to === node.id);
  const outputs = DATA.edges.filter(e => e.from === node.id);

  let html = `<h2>${{node.measure}}</h2>`;
  html += `<div class="description">${{node.description || node.id}}</div>`;
  html += `<div class="meta-row"><span class="meta-label">View</span><span class="meta-value">${{node.view}}</span></div>`;
  html += `<div class="meta-row"><span class="meta-label">Type</span><span class="meta-value">${{node.measure_type}}</span></div>`;
  html += `<div class="meta-row"><span class="meta-label">ID</span><span class="meta-value">${{node.id}}</span></div>`;

  if (inputs.length > 0) {{
    html += `<h3>Inputs (${{inputs.length}})</h3>`;
    inputs.forEach(e => {{
      const src = nodeById[e.from];
      const badge = e.kind === 'component'
        ? '<span class="badge component">component</span>'
        : `<span class="badge ${{e.strength}}">${{e.strength}}</span>`;
      html += `<div class="rel-item">${{badge}} ${{src ? src.id : e.from}}`;
      if (e.kind === 'driver' && e.direction !== 'unknown') html += ` <span style="color:#888">(${{e.direction}})</span>`;
      if (e.description) html += `<br><span style="color:#888;font-size:12px">${{e.description}}</span>`;
      if (e.refs && e.refs.length > 0) {{
        e.refs.forEach(r => {{
          html += `<a class="ref-link" href="${{r}}" target="_blank">${{r}}</a>`;
        }});
      }}
      html += `</div>`;
    }});
  }}

  if (outputs.length > 0) {{
    html += `<h3>Drives (${{outputs.length}})</h3>`;
    outputs.forEach(e => {{
      const tgt = nodeById[e.to];
      html += `<div class="rel-item">${{tgt ? tgt.id : e.to}}</div>`;
    }});
  }}

  detailContent.innerHTML = html;
  panel.classList.add('visible');
}}

// ── Pan & zoom ──
let viewBox = {{ x: 0, y: 0, w: W, h: H }};
let isPanning = false, startPan = {{ x: 0, y: 0 }};

function updateViewBox() {{
  svg.setAttribute('viewBox', `${{viewBox.x}} ${{viewBox.y}} ${{viewBox.w}} ${{viewBox.h}}`);
}}
updateViewBox();

svg.addEventListener('mousedown', e => {{
  if (e.target === svg || e.target.tagName === 'path') {{
    isPanning = true;
    startPan = {{ x: e.clientX, y: e.clientY }};
  }}
}});
window.addEventListener('mousemove', e => {{
  if (!isPanning) return;
  const dx = (e.clientX - startPan.x) * (viewBox.w / W);
  const dy = (e.clientY - startPan.y) * (viewBox.h / H);
  viewBox.x -= dx;
  viewBox.y -= dy;
  startPan = {{ x: e.clientX, y: e.clientY }};
  updateViewBox();
}});
window.addEventListener('mouseup', () => {{ isPanning = false; }});
svg.addEventListener('wheel', e => {{
  e.preventDefault();
  const scale = e.deltaY > 0 ? 1.1 : 0.9;
  const mx = viewBox.x + (e.clientX / W) * viewBox.w;
  const my = viewBox.y + (e.clientY / H) * viewBox.h;
  viewBox.w *= scale;
  viewBox.h *= scale;
  viewBox.x = mx - (e.clientX / W) * viewBox.w;
  viewBox.y = my - (e.clientY / H) * viewBox.h;
  updateViewBox();
}});
</script>
</body>
</html>"#,
            tree_json = tree_json,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::*;

    fn make_view(name: &str, measures: Vec<Measure>) -> View {
        View {
            name: name.to_string(),
            description: format!("{} view", name),
            label: None,
            datasource: None,
            dialect: None,
            table: Some(format!("public.{}", name)),
            sql: None,
            entities: vec![],
            dimensions: vec![],
            measures: Some(measures),
            segments: vec![],
            meta: None,
        }
    }

    fn atomic_measure(name: &str, mtype: MeasureType) -> Measure {
        Measure {
            name: name.to_string(),
            measure_type: mtype,
            description: None,
            expr: Some(name.to_string()),
            original_expr: None,
            filters: None,
            samples: None,
            synonyms: None,
            rolling_window: None,
            inherits_from: None,
            drivers: None,
            meta: None,
        }
    }

    #[test]
    fn test_implicit_component_edges() {
        let layer = SemanticLayer {
            views: vec![make_view(
                "orders",
                vec![
                    atomic_measure("total_revenue", MeasureType::Sum),
                    atomic_measure("total_orders", MeasureType::Count),
                    Measure {
                        name: "avg_order_value".to_string(),
                        measure_type: MeasureType::Number,
                        expr: Some(
                            "{{orders.total_revenue}} / NULLIF({{orders.total_orders}}, 0)"
                                .to_string(),
                        ),
                        description: None,
                        original_expr: None,
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        drivers: None,
                        meta: None,
                    },
                ],
            )],
            topics: None,
            motifs: None,
            saved_queries: None,
            metadata: None,
        };

        let tree = MetricTree::build(&layer);
        assert_eq!(tree.nodes.len(), 3);
        assert_eq!(tree.edges.len(), 2);
        assert!(tree.edges.iter().all(|e| e.kind == EdgeKind::Component));
        assert!(tree.edges.iter().all(|e| e.to == "orders.avg_order_value"));
    }

    #[test]
    fn test_explicit_driver_edges() {
        let layer = SemanticLayer {
            views: vec![
                make_view(
                    "marketing",
                    vec![atomic_measure("ad_spend", MeasureType::Sum)],
                ),
                make_view(
                    "leads",
                    vec![Measure {
                        name: "total_leads".to_string(),
                        measure_type: MeasureType::Count,
                        expr: None,
                        description: None,
                        original_expr: None,
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        drivers: Some(vec![Driver {
                            measure: "marketing.ad_spend".to_string(),
                            direction: DriverDirection::Positive,
                            strength: DriverStrength::Strong,
                            confidence: DriverConfidence::Medium,
                            description: Some("More spend → more leads".to_string()),
                            refs: Some(vec!["https://notion.so/ad-spend-analysis".to_string()]),
                        }]),
                        meta: None,
                    }],
                ),
            ],
            topics: None,
            motifs: None,
            saved_queries: None,
            metadata: None,
        };

        let tree = MetricTree::build(&layer);
        assert_eq!(tree.edges.len(), 1);
        let edge = &tree.edges[0];
        assert_eq!(edge.kind, EdgeKind::Driver);
        assert_eq!(edge.from, "marketing.ad_spend");
        assert_eq!(edge.to, "leads.total_leads");
        assert_eq!(edge.direction, DriverDirection::Positive);
        assert_eq!(edge.strength, DriverStrength::Strong);
        assert!(edge.refs.as_ref().unwrap().len() == 1);
    }

    #[test]
    fn test_subtree() {
        let layer = SemanticLayer {
            views: vec![
                make_view(
                    "orders",
                    vec![
                        atomic_measure("revenue", MeasureType::Sum),
                        atomic_measure("count", MeasureType::Count),
                        Measure {
                            name: "aov".to_string(),
                            measure_type: MeasureType::Number,
                            expr: Some(
                                "{{orders.revenue}} / NULLIF({{orders.count}}, 0)".to_string(),
                            ),
                            description: None,
                            original_expr: None,
                            filters: None,
                            samples: None,
                            synonyms: None,
                            rolling_window: None,
                            inherits_from: None,
                            drivers: None,
                            meta: None,
                        },
                    ],
                ),
                make_view("other", vec![atomic_measure("unrelated", MeasureType::Sum)]),
            ],
            topics: None,
            motifs: None,
            saved_queries: None,
            metadata: None,
        };

        let tree = MetricTree::build(&layer);
        let sub = tree.subtree("orders.aov").unwrap();
        assert_eq!(sub.nodes.len(), 3); // aov + revenue + count
        assert_eq!(sub.edges.len(), 2);
        // "unrelated" should not be in subtree
        assert!(!sub.nodes.iter().any(|n| n.id == "other.unrelated"));
    }

    #[test]
    fn test_roots_and_leaves() {
        let layer = SemanticLayer {
            views: vec![make_view(
                "orders",
                vec![
                    atomic_measure("revenue", MeasureType::Sum),
                    atomic_measure("count", MeasureType::Count),
                    Measure {
                        name: "aov".to_string(),
                        measure_type: MeasureType::Number,
                        expr: Some("{{orders.revenue}} / NULLIF({{orders.count}}, 0)".to_string()),
                        description: None,
                        original_expr: None,
                        filters: None,
                        samples: None,
                        synonyms: None,
                        rolling_window: None,
                        inherits_from: None,
                        drivers: None,
                        meta: None,
                    },
                ],
            )],
            topics: None,
            motifs: None,
            saved_queries: None,
            metadata: None,
        };

        let tree = MetricTree::build(&layer);
        let roots = tree.roots();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, "orders.aov");

        let leaves = tree.leaves();
        assert_eq!(leaves.len(), 2);
        let leaf_ids: Vec<&str> = leaves.iter().map(|l| l.id.as_str()).collect();
        assert!(leaf_ids.contains(&"orders.revenue"));
        assert!(leaf_ids.contains(&"orders.count"));
    }
}
