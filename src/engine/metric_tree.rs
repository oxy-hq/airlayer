use crate::engine::member_sql::MemberSqlResolver;
use crate::schema::models::{
    DriverConfidence, DriverDirection, DriverForm, DriverStrength, MeasureType, SemanticLayer,
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
    /// The SQL expression (for composite measures, shows the derivation formula).
    pub expr: Option<String>,
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
    // -- Qualitative fields --
    /// Direction (for driver edges).
    pub direction: DriverDirection,
    /// Strength (for driver edges).
    pub strength: DriverStrength,
    /// Confidence (for driver edges).
    pub confidence: DriverConfidence,
    // -- Quantitative fields --
    /// Marginal effect coefficient.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coefficient: Option<f64>,
    /// Functional form of the relationship.
    #[serde(skip_serializing_if = "is_default_form")]
    pub form: DriverForm,
    /// Intercept term.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intercept: Option<f64>,
    /// Lag in days.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
    // -- Common fields --
    /// Description.
    pub description: Option<String>,
    /// Supporting references.
    pub refs: Option<Vec<String>>,
}

fn is_default_form(form: &DriverForm) -> bool {
    *form == DriverForm::Linear
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
                    expr: measure.expr.clone(),
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
                                coefficient: None,
                                form: DriverForm::default(),
                                intercept: None,
                                lag: None,
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
                                coefficient: driver.coefficient,
                                form: driver.form.clone(),
                                intercept: driver.intercept,
                                lag: driver.lag,
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
                    // Always collect the edge (even if the source node was already visited,
                    // e.g., in diamond graphs where A->C and B->C both exist).
                    subtree_edges.push(edge.clone());
                    // Only enqueue the source node for further BFS if not yet visited.
                    if !visited.contains(&edge.from) {
                        visited.insert(edge.from.clone());
                        queue.push_back(edge.from.clone());
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
    /// Uses a force-directed graph layout with click-to-focus behavior.
    /// Only available in CLI builds — excluded from WASM/library to keep binary small.
    #[cfg(feature = "cli")]
    pub fn to_html(&self) -> String {
        let tree_json =
            serde_json::to_string(self).expect("MetricTree should be serializable to JSON");
        format!(
            r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Metric Tree — airlayer</title>
<style>
:root {{
  --bg: #0d1117; --surface: #161b22; --border: #30363d;
  --text: #e6edf3; --text-muted: #8b949e; --text-faint: #6e7681;
  --blue: #58a6ff; --green: #3fb950; --red: #f85149; --gold: #d29922;
  --purple: #bc8cff;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); overflow: hidden; }}
canvas {{ display: block; position: absolute; top: 0; left: 0; }}

/* Detail panel */
#detail-panel {{
  position: fixed; top: 0; right: -420px; width: 400px; height: 100vh;
  background: var(--surface); border-left: 1px solid var(--border);
  padding: 24px; z-index: 20; overflow-y: auto;
  transition: right 0.25s ease;
}}
#detail-panel.open {{ right: 0; }}
#detail-panel h2 {{ font-size: 18px; color: var(--text); margin-bottom: 2px; font-weight: 600; }}
.detail-id {{ color: var(--text-muted); font-size: 13px; font-family: monospace; margin-bottom: 16px; }}
.detail-desc {{ color: var(--text-muted); font-size: 14px; margin-bottom: 16px; line-height: 1.5; }}
.meta-grid {{ display: grid; grid-template-columns: auto 1fr; gap: 6px 16px; margin-bottom: 20px; font-size: 13px; }}
.meta-grid .label {{ color: var(--text-faint); }}
.meta-grid .value {{ color: var(--text); }}
.expr-block {{
  background: #0d1117; border: 1px solid var(--border); border-radius: 6px;
  padding: 12px; font-family: monospace; font-size: 12px; color: var(--blue);
  margin-bottom: 20px; white-space: pre-wrap; word-break: break-all; line-height: 1.6;
}}
.section-title {{ font-size: 11px; color: var(--text-faint); text-transform: uppercase; letter-spacing: 1px; margin: 20px 0 8px; font-weight: 600; }}
.rel-card {{
  background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
  padding: 10px 12px; margin-bottom: 8px; cursor: pointer; transition: border-color 0.15s;
}}
.rel-card:hover {{ border-color: var(--blue); }}
.rel-card .rel-name {{ font-size: 13px; font-weight: 500; }}
.rel-card .rel-meta {{ font-size: 12px; color: var(--text-muted); margin-top: 2px; }}
.rel-card .rel-desc {{ font-size: 12px; color: var(--text-faint); margin-top: 4px; }}
.badge {{
  display: inline-block; padding: 2px 8px; border-radius: 12px;
  font-size: 11px; font-weight: 500; margin-right: 4px;
}}
.badge-component {{ background: rgba(88,166,255,0.15); color: var(--blue); }}
.badge-strong {{ background: rgba(63,185,80,0.15); color: var(--green); }}
.badge-moderate {{ background: rgba(210,153,34,0.15); color: var(--gold); }}
.badge-weak {{ background: rgba(248,81,73,0.15); color: var(--red); }}
.badge-positive {{ background: rgba(63,185,80,0.1); color: var(--green); }}
.badge-negative {{ background: rgba(248,81,73,0.1); color: var(--red); }}
.ref-link {{
  display: block; color: var(--blue); font-size: 12px; margin: 4px 0;
  text-decoration: none; word-break: break-all;
}}
.ref-link:hover {{ text-decoration: underline; }}
#close-btn {{
  position: absolute; top: 16px; right: 16px; background: none; border: none;
  color: var(--text-muted); font-size: 20px; cursor: pointer; width: 32px; height: 32px;
  border-radius: 6px; display: flex; align-items: center; justify-content: center;
}}
#close-btn:hover {{ background: var(--border); color: var(--text); }}

/* Toolbar */
#toolbar {{
  position: fixed; top: 16px; left: 16px; display: flex; gap: 8px; z-index: 10;
}}
#toolbar button {{
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  color: var(--text-muted); padding: 8px 14px; font-size: 13px; cursor: pointer;
  transition: all 0.15s;
}}
#toolbar button:hover {{ border-color: var(--blue); color: var(--text); }}
#toolbar button.active {{ border-color: var(--blue); color: var(--blue); background: rgba(88,166,255,0.1); }}

/* Legend */
#legend {{
  position: fixed; bottom: 16px; left: 16px; background: var(--surface);
  border: 1px solid var(--border); border-radius: 10px; padding: 14px 18px;
  font-size: 12px; z-index: 10;
}}
#legend .legend-title {{ font-weight: 600; margin-bottom: 8px; color: var(--text); }}
.legend-row {{ display: flex; align-items: center; gap: 10px; margin: 4px 0; color: var(--text-muted); }}
.legend-circle {{ width: 16px; height: 10px; border-radius: 3px; border: 2px solid; display: inline-block; }}
.legend-line {{ width: 24px; height: 0; display: inline-block; }}

/* Focus mode banner */
#focus-banner {{
  position: fixed; top: 16px; left: 50%; transform: translateX(-50%);
  background: var(--surface); border: 1px solid var(--blue); border-radius: 8px;
  padding: 8px 16px; font-size: 13px; z-index: 10; display: none;
  align-items: center; gap: 12px;
}}
#focus-banner.visible {{ display: flex; }}
#focus-banner button {{
  background: none; border: 1px solid var(--border); border-radius: 6px;
  color: var(--text-muted); padding: 4px 10px; font-size: 12px; cursor: pointer;
}}
#focus-banner button:hover {{ border-color: var(--text-muted); color: var(--text); }}
</style>
</head>
<body>
<canvas id="canvas"></canvas>

<div id="toolbar">
  <button id="btn-reset" title="Reset view">Reset</button>
  <button id="btn-fit" title="Fit all nodes">Fit</button>
</div>

<div id="focus-banner">
  <span id="focus-label">Focused on: <strong id="focus-name"></strong></span>
  <button id="btn-unfocus">Show all</button>
</div>

<div id="detail-panel">
  <button id="close-btn">&times;</button>
  <div id="detail-content"></div>
</div>

<div id="legend">
  <div class="legend-title">Metric Tree</div>
  <div class="legend-row"><span class="legend-circle" style="border-color: var(--gold); background: rgba(210,153,34,0.2);"></span> Root / North Star</div>
  <div class="legend-row"><span class="legend-circle" style="border-color: var(--blue); background: rgba(88,166,255,0.2);"></span> Composite metric</div>
  <div class="legend-row"><span class="legend-circle" style="border-color: var(--text-faint); background: rgba(110,118,129,0.15);"></span> Atomic metric</div>
  <div class="legend-row"><span class="legend-line" style="border-top: 2px solid var(--blue);"></span> Component (math)</div>
  <div class="legend-row"><span class="legend-line" style="border-top: 2px solid var(--green);"></span> Driver (positive)</div>
  <div class="legend-row"><span class="legend-line" style="border-top: 2px solid var(--red);"></span> Driver (negative)</div>
  <div class="legend-row"><span class="legend-line" style="border-top: 2px dashed var(--text-faint);"></span> Low confidence</div>
</div>

<script>
const DATA = {tree_json};

// ── State ──
const nodeById = {{}};
DATA.nodes.forEach(n => {{ nodeById[n.id] = n; }});

// Identify roots
const srcSet = new Set(DATA.edges.map(e => e.from));
const tgtSet = new Set(DATA.edges.map(e => e.to));
const rootIds = new Set(
  DATA.nodes.filter(n => tgtSet.has(n.id) && !srcSet.has(n.id)).map(n => n.id)
);

// Build adjacency for focus mode
const inputsOf = {{}};  // target -> [edge]
const outputsOf = {{}}; // source -> [edge]
DATA.edges.forEach(e => {{
  (inputsOf[e.to] = inputsOf[e.to] || []).push(e);
  (outputsOf[e.from] = outputsOf[e.from] || []).push(e);
}});

// Pre-build neighbor sets for each node (for highlight checks)
const neighborOf = {{}};
DATA.edges.forEach(e => {{
  (neighborOf[e.from] = neighborOf[e.from] || new Set()).add(e.to);
  (neighborOf[e.to] = neighborOf[e.to] || new Set()).add(e.from);
}});

// ── Measure text for pill sizing ──
const _measureCanvas = document.createElement('canvas');
const _measureCtx = _measureCanvas.getContext('2d');
function measureText(text, font) {{
  _measureCtx.font = font;
  return _measureCtx.measureText(text).width;
}}

// ── Simulation state — pills (rounded rects) instead of circles ──
const FONT_MAIN = '600 12px -apple-system, BlinkMacSystemFont, sans-serif';
const FONT_SUB = '400 10px -apple-system, BlinkMacSystemFont, sans-serif';
const PILL_H = 40;   // total height
const PILL_PAD = 16;  // horizontal padding each side
const PILL_R = 10;    // corner radius

const SIM_NODES = DATA.nodes.map(n => {{
  const nameW = measureText(n.measure, FONT_MAIN);
  const viewW = measureText(n.view, FONT_SUB);
  const textW = Math.max(nameW, viewW);
  const w = textW + PILL_PAD * 2;
  return {{
    id: n.id, x: 0, y: 0, vx: 0, vy: 0,
    w: Math.max(w, 80),  // minimum width
    h: PILL_H,
    pinned: false,
  }};
}});
const nodeSimById = {{}};
SIM_NODES.forEach(n => {{ nodeSimById[n.id] = n; }});

const SIM_EDGES = DATA.edges.map(e => ({{
  source: nodeSimById[e.from],
  target: nodeSimById[e.to],
  data: e,
}}));

// Initial positions: centered at origin with hierarchy-aware placement
(function initPositions() {{
  const level = {{}};
  const queue = [...rootIds];
  queue.forEach(r => {{ level[r] = 0; }});
  const visited = new Set(queue);
  while (queue.length) {{
    const cur = queue.shift();
    (inputsOf[cur] || []).forEach(e => {{
      if (!visited.has(e.from)) {{
        visited.add(e.from);
        level[e.from] = (level[cur] || 0) + 1;
        queue.push(e.from);
      }}
    }});
  }}
  SIM_NODES.forEach(n => {{ if (level[n.id] === undefined) level[n.id] = 0; }});
  const levels = {{}};
  SIM_NODES.forEach(n => {{
    const l = level[n.id];
    (levels[l] = levels[l] || []).push(n);
  }});
  const maxLevel = Math.max(...Object.keys(levels).map(Number), 0);
  const totalHeight = maxLevel * 120;
  Object.entries(levels).forEach(([l, nodes]) => {{
    const lNum = Number(l);
    const spacing = Math.min(180, 800 / (nodes.length + 1));
    const startX = -(nodes.length - 1) * spacing / 2;
    nodes.forEach((n, i) => {{
      n.x = startX + i * spacing + (Math.random() - 0.5) * 20;
      n.y = -totalHeight / 2 + lNum * 120 + (Math.random() - 0.5) * 20;
    }});
  }});
}})();

// ── Force simulation ──
let simAlpha = 1.0;
let simRunning = true;
const ALPHA_DECAY = 0.02;
const ALPHA_MIN = 0.001;
const VELOCITY_DECAY = 0.3;

function simTick() {{
  // Repulsion
  for (let i = 0; i < SIM_NODES.length; i++) {{
    for (let j = i + 1; j < SIM_NODES.length; j++) {{
      const a = SIM_NODES[i], b = SIM_NODES[j];
      let dx = b.x - a.x, dy = b.y - a.y;
      let dist = Math.sqrt(dx * dx + dy * dy) || 1;
      const minDist = (a.w + b.w) / 2 + 30;
      const strength = -1200 * simAlpha;
      const force = strength / (dist * dist);
      const fx = (dx / dist) * force, fy = (dy / dist) * force;
      if (!a.pinned) {{ a.vx -= fx; a.vy -= fy; }}
      if (!b.pinned) {{ b.vx += fx; b.vy += fy; }}
      // Collision: axis-aligned overlap
      const overlapX = (a.w + b.w) / 2 + 20 - Math.abs(dx);
      const overlapY = (a.h + b.h) / 2 + 12 - Math.abs(dy);
      if (overlapX > 0 && overlapY > 0) {{
        const pushX = (dx > 0 ? 1 : -1) * overlapX * 0.3;
        const pushY = (dy > 0 ? 1 : -1) * overlapY * 0.3;
        if (!a.pinned) {{ a.x -= pushX; a.y -= pushY; }}
        if (!b.pinned) {{ b.x += pushX; b.y += pushY; }}
      }}
    }}
  }}
  // Link spring
  SIM_EDGES.forEach(e => {{
    const s = e.source, t = e.target;
    let dx = t.x - s.x, dy = t.y - s.y;
    let dist = Math.sqrt(dx * dx + dy * dy) || 1;
    const idealDist = 180;
    const strength = 0.12 * simAlpha;
    const displacement = (dist - idealDist) * strength;
    const fx = (dx / dist) * displacement, fy = (dy / dist) * displacement;
    if (!s.pinned) {{ s.vx += fx; s.vy += fy; }}
    if (!t.pinned) {{ t.vx -= fx; t.vy -= fy; }}
  }});
  // Vertical hierarchy: source (child) below target (parent)
  SIM_EDGES.forEach(e => {{
    const s = e.source, t = e.target;
    const verticalBias = 0.05 * simAlpha;
    if (s.y < t.y) {{
      const pull = (t.y - s.y + 60) * verticalBias;
      if (!s.pinned) s.vy += pull;
      if (!t.pinned) t.vy -= pull;
    }}
  }});
  // Center gravity
  const cx = window.innerWidth / 2, cy = window.innerHeight / 2;
  SIM_NODES.forEach(n => {{
    if (n.pinned) return;
    n.vx += (cx - n.x) * 0.003 * simAlpha;
    n.vy += (cy - n.y) * 0.003 * simAlpha;
  }});
  // Integrate
  SIM_NODES.forEach(n => {{
    if (n.pinned) return;
    n.vx *= VELOCITY_DECAY;
    n.vy *= VELOCITY_DECAY;
    n.x += n.vx;
    n.y += n.vy;
  }});
  simAlpha = Math.max(simAlpha - ALPHA_DECAY, 0);
  if (simAlpha <= ALPHA_MIN) simRunning = false;
}}

// ── Canvas rendering ──
const canvas = document.getElementById('canvas');
const ctx = canvas.getContext('2d');
let dpr = window.devicePixelRatio || 1;

let camera = {{ x: 0, y: 0, zoom: 1 }};
let focusedNode = null;
let visibleNodes = new Set(SIM_NODES.map(n => n.id));
let visibleEdges = new Set(SIM_EDGES.map((_, i) => i));
let hoveredNode = null;
let selectedNode = null;
let dragNode = null, dragStart = null, isDragging = false;
let isPanning = false, panStart = {{ x: 0, y: 0 }};

function resize() {{
  const w = window.innerWidth, h = window.innerHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.width = w + 'px';
  canvas.style.height = h + 'px';
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}}
resize();
window.addEventListener('resize', resize);

function screenToWorld(sx, sy) {{
  return {{
    x: (sx - window.innerWidth / 2) / camera.zoom + camera.x,
    y: (sy - window.innerHeight / 2) / camera.zoom + camera.y,
  }};
}}

function nodeAt(wx, wy) {{
  for (let i = SIM_NODES.length - 1; i >= 0; i--) {{
    const n = SIM_NODES[i];
    if (!visibleNodes.has(n.id)) continue;
    if (wx >= n.x - n.w/2 && wx <= n.x + n.w/2 && wy >= n.y - n.h/2 && wy <= n.y + n.h/2) return n;
  }}
  return null;
}}

function edgeColor(e) {{
  if (e.data.kind === 'component') return '#58a6ff';
  if (e.data.direction === 'positive') return '#3fb950';
  if (e.data.direction === 'negative') return '#f85149';
  return '#8b949e';
}}

function edgeWidth(e) {{
  if (e.data.strength === 'strong') return 2.5;
  if (e.data.strength === 'moderate') return 1.5;
  return 1;
}}

// Find intersection of line from center to edge of rounded rect
function rectEdgePoint(cx, cy, w, h, targetX, targetY) {{
  const dx = targetX - cx, dy = targetY - cy;
  if (dx === 0 && dy === 0) return {{ x: cx, y: cy }};
  const hw = w / 2 + 2, hh = h / 2 + 2;
  const absDx = Math.abs(dx), absDy = Math.abs(dy);
  let scale;
  if (absDx / hw > absDy / hh) {{
    scale = hw / absDx;
  }} else {{
    scale = hh / absDy;
  }}
  return {{ x: cx + dx * scale, y: cy + dy * scale }};
}}

function drawEdge(e, alpha) {{
  const s = e.source, t = e.target;
  const color = edgeColor(e);
  const width = edgeWidth(e);
  const dashed = e.data.confidence === 'low';

  const sp = rectEdgePoint(s.x, s.y, s.w, s.h, t.x, t.y);
  const tp = rectEdgePoint(t.x, t.y, t.w, t.h, s.x, s.y);

  ctx.save();
  ctx.globalAlpha = alpha * (dashed ? 0.7 : 1.0);
  ctx.strokeStyle = color;
  ctx.lineWidth = width / camera.zoom;
  if (dashed) ctx.setLineDash([6 / camera.zoom, 4 / camera.zoom]);
  else ctx.setLineDash([]);

  // Curved line
  const midX = (sp.x + tp.x) / 2, midY = (sp.y + tp.y) / 2;
  const perpX = -(tp.y - sp.y) * 0.12, perpY = (tp.x - sp.x) * 0.12;
  ctx.beginPath();
  ctx.moveTo(sp.x, sp.y);
  ctx.quadraticCurveTo(midX + perpX, midY + perpY, tp.x, tp.y);
  ctx.stroke();

  // Arrowhead
  const headLen = 7 / camera.zoom;
  const tt = 0.97;
  const cX = midX + perpX, cY = midY + perpY;
  const tdx = 2*(1-tt)*(cX - sp.x) + 2*tt*(tp.x - cX);
  const tdy = 2*(1-tt)*(cY - sp.y) + 2*tt*(tp.y - cY);
  const endAngle = Math.atan2(tdy, tdx);
  ctx.setLineDash([]);
  ctx.globalAlpha = alpha;
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.moveTo(tp.x, tp.y);
  ctx.lineTo(tp.x - headLen * Math.cos(endAngle - 0.4), tp.y - headLen * Math.sin(endAngle - 0.4));
  ctx.lineTo(tp.x - headLen * Math.cos(endAngle + 0.4), tp.y - headLen * Math.sin(endAngle + 0.4));
  ctx.closePath();
  ctx.fill();
  ctx.restore();
}}

function roundRect(x, y, w, h, r) {{
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + r);
  ctx.lineTo(x + w, y + h - r);
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
  ctx.lineTo(x + r, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}}

function draw() {{
  const w = window.innerWidth, h = window.innerHeight;
  ctx.clearRect(0, 0, w, h);
  ctx.save();
  ctx.translate(w / 2, h / 2);
  ctx.scale(camera.zoom, camera.zoom);
  ctx.translate(-camera.x, -camera.y);

  const selNeighbors = selectedNode ? (neighborOf[selectedNode.id] || new Set()) : null;

  // Draw edges
  SIM_EDGES.forEach((e, i) => {{
    if (!visibleEdges.has(i)) return;
    // In focus mode, only visible edges are in the set — draw them fully.
    // When a node is selected (single-click), dim unrelated edges.
    let alpha = 1;
    if (selectedNode) {{
      const connected = e.source.id === selectedNode.id || e.target.id === selectedNode.id;
      alpha = connected ? 1 : 0.08;
    }}
    drawEdge(e, alpha);
  }});

  // Draw nodes
  SIM_NODES.forEach(n => {{
    if (!visibleNodes.has(n.id)) return;
    const data = nodeById[n.id];
    const isRoot = rootIds.has(n.id);
    const isHovered = hoveredNode === n;
    const isSelected = selectedNode === n;

    let alpha = 1;
    if (selectedNode && !isSelected) {{
      alpha = (selNeighbors && selNeighbors.has(n.id)) ? 1 : 0.12;
    }}
    ctx.globalAlpha = alpha;

    const rx = n.x - n.w / 2, ry = n.y - n.h / 2;

    // Glow
    if (isSelected) {{
      ctx.save();
      ctx.shadowColor = isRoot ? '#d29922' : '#58a6ff';
      ctx.shadowBlur = 16;
      roundRect(rx - 1, ry - 1, n.w + 2, n.h + 2, PILL_R);
      ctx.fillStyle = 'transparent';
      ctx.fill();
      ctx.restore();
    }}

    // Pill background
    roundRect(rx, ry, n.w, n.h, PILL_R);
    ctx.fillStyle = isRoot ? 'rgba(210,153,34,0.12)' : (data.is_composite ? 'rgba(88,166,255,0.08)' : 'rgba(110,118,129,0.06)');
    ctx.fill();
    const strokeColor = isRoot ? '#d29922' : (data.is_composite ? '#58a6ff' : '#30363d');
    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = (isSelected || isHovered ? 2 : 1.2) / camera.zoom;
    ctx.stroke();

    // Main label
    ctx.font = FONT_MAIN;
    ctx.fillStyle = '#e6edf3';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(data.measure, n.x, n.y - 5);

    // Sub label (view)
    ctx.font = FONT_SUB;
    ctx.fillStyle = '#8b949e';
    ctx.fillText(data.view, n.x, n.y + 9);

    ctx.globalAlpha = 1;
  }});

  ctx.restore();
}}

// ── Pre-compute stable layout before first render ──
for (let i = 0; i < 120; i++) simTick();
simAlpha = Math.max(simAlpha, 0.001);
fitNodes(SIM_NODES);

// ── Animation loop ──
function frame() {{
  if (simRunning) simTick();
  draw();
  requestAnimationFrame(frame);
}}
requestAnimationFrame(frame);

// ── Interaction ──
canvas.addEventListener('mousedown', e => {{
  const w = screenToWorld(e.clientX, e.clientY);
  const hit = nodeAt(w.x, w.y);
  if (hit) {{
    dragNode = hit;
    dragStart = {{ x: e.clientX, y: e.clientY }};
    isDragging = false;
    hit.pinned = true;
  }} else {{
    isPanning = true;
    panStart = {{ x: e.clientX, y: e.clientY }};
  }}
}});

canvas.addEventListener('mousemove', e => {{
  const w = screenToWorld(e.clientX, e.clientY);
  if (dragNode) {{
    const dx = e.clientX - dragStart.x, dy = e.clientY - dragStart.y;
    if (Math.abs(dx) + Math.abs(dy) > 4) isDragging = true;
    if (isDragging) {{
      dragNode.x = w.x;
      dragNode.y = w.y;
      dragNode.vx = 0;
      dragNode.vy = 0;
      reheat(0.3);
    }}
  }} else if (isPanning) {{
    const dx = (e.clientX - panStart.x) / camera.zoom;
    const dy = (e.clientY - panStart.y) / camera.zoom;
    camera.x -= dx;
    camera.y -= dy;
    panStart = {{ x: e.clientX, y: e.clientY }};
  }} else {{
    const hit = nodeAt(w.x, w.y);
    hoveredNode = hit;
    canvas.style.cursor = hit ? 'pointer' : 'grab';
  }}
}});

canvas.addEventListener('mouseup', e => {{
  if (dragNode && !isDragging) {{
    // Toggle: clicking the already-selected node deselects and unfocuses
    if (selectedNode && selectedNode.id === dragNode.id) {{
      selectedNode = null;
      panel.classList.remove('open');
      if (focusedNode) unfocus();
    }} else {{
      selectNode(dragNode);
    }}
  }}
  if (dragNode) dragNode.pinned = false;
  dragNode = null;
  isDragging = false;
  isPanning = false;
}});

canvas.addEventListener('wheel', e => {{
  e.preventDefault();
  const factor = e.deltaY > 0 ? 0.92 : 1.08;
  const w = screenToWorld(e.clientX, e.clientY);
  camera.zoom *= factor;
  camera.zoom = Math.max(0.1, Math.min(5, camera.zoom));
  const w2 = screenToWorld(e.clientX, e.clientY);
  camera.x -= (w2.x - w.x);
  camera.y -= (w2.y - w.y);
}}, {{ passive: false }});

// Double-click to focus
canvas.addEventListener('dblclick', e => {{
  const w = screenToWorld(e.clientX, e.clientY);
  const hit = nodeAt(w.x, w.y);
  if (hit) focusOn(hit);
}});

// Click on empty space to deselect
canvas.addEventListener('click', e => {{
  if (!dragNode && !isDragging) {{
    const w = screenToWorld(e.clientX, e.clientY);
    const hit = nodeAt(w.x, w.y);
    if (!hit && selectedNode) {{
      selectedNode = null;
      panel.classList.remove('open');
    }}
  }}
}});

function reheat(alpha) {{
  simAlpha = Math.max(simAlpha, alpha || 0.5);
  simRunning = true;
}}

// ── Selection & detail panel ──
const panel = document.getElementById('detail-panel');
const detailContent = document.getElementById('detail-content');
document.getElementById('close-btn').addEventListener('click', () => {{
  panel.classList.remove('open');
  selectedNode = null;
}});

function selectNode(simNode) {{
  const node = nodeById[simNode.id];
  selectedNode = simNode;

  const inputs = (inputsOf[node.id] || []);
  const outputs = (outputsOf[node.id] || []);

  let html = `<h2>${{esc(node.measure)}}</h2>`;
  html += `<div class="detail-id">${{esc(node.id)}}</div>`;
  if (node.description) html += `<div class="detail-desc">${{esc(node.description)}}</div>`;

  html += `<div class="meta-grid">`;
  html += `<span class="label">View</span><span class="value">${{esc(node.view)}}</span>`;
  html += `<span class="label">Type</span><span class="value">${{esc(node.measure_type)}}</span>`;
  html += `<span class="label">Role</span><span class="value">${{rootIds.has(node.id) ? 'Root / North Star' : (node.is_composite ? 'Composite' : 'Atomic')}}</span>`;
  html += `</div>`;

  if (node.expr) {{
    html += `<div class="section-title">Derivation</div>`;
    html += `<div class="expr-block">${{esc(node.expr)}}</div>`;
  }}

  if (inputs.length > 0) {{
    html += `<div class="section-title">Inputs (${{inputs.length}})</div>`;
    inputs.forEach(e => {{
      const src = nodeById[e.from];
      html += `<div class="rel-card" onclick="focusOnId('${{e.from}}')">`;
      html += `<div class="rel-name">`;
      if (e.kind === 'component') html += `<span class="badge badge-component">component</span>`;
      else {{
        html += `<span class="badge badge-${{e.strength}}">${{e.strength}}</span>`;
        if (e.direction !== 'unknown') html += `<span class="badge badge-${{e.direction}}">${{e.direction}}</span>`;
      }}
      html += ` ${{esc(src ? src.id : e.from)}}</div>`;
      if (e.description) html += `<div class="rel-desc">${{esc(e.description)}}</div>`;
      if (e.refs && e.refs.length > 0) {{
        e.refs.forEach(r => {{
          html += `<a class="ref-link" href="${{esc(r)}}" target="_blank" onclick="event.stopPropagation()">${{esc(r)}}</a>`;
        }});
      }}
      html += `</div>`;
    }});
  }}

  if (outputs.length > 0) {{
    html += `<div class="section-title">Drives (${{outputs.length}})</div>`;
    outputs.forEach(e => {{
      const tgt = nodeById[e.to];
      html += `<div class="rel-card" onclick="focusOnId('${{e.to}}')">`;
      html += `<div class="rel-name">${{esc(tgt ? tgt.id : e.to)}}</div>`;
      html += `</div>`;
    }});
  }}

  detailContent.innerHTML = html;
  panel.classList.add('open');
}}

function esc(s) {{
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}}

// ── Focus mode ──
const focusBanner = document.getElementById('focus-banner');
const focusName = document.getElementById('focus-name');

function focusOn(simNode) {{
  focusedNode = simNode;
  const related = new Set();
  const queue = [simNode.id];
  related.add(simNode.id);
  while (queue.length) {{
    const cur = queue.shift();
    (inputsOf[cur] || []).forEach(e => {{
      if (!related.has(e.from)) {{ related.add(e.from); queue.push(e.from); }}
    }});
    (outputsOf[cur] || []).forEach(e => {{
      if (!related.has(e.to)) {{ related.add(e.to); queue.push(e.to); }}
    }});
  }}
  visibleNodes = related;
  visibleEdges = new Set();
  SIM_EDGES.forEach((e, i) => {{
    if (related.has(e.source.id) && related.has(e.target.id)) visibleEdges.add(i);
  }});

  focusName.textContent = nodeById[simNode.id].measure;
  focusBanner.classList.add('visible');
  selectNode(simNode);
  fitNodes([...related].map(id => nodeSimById[id]));
}}

function focusOnId(id) {{
  const sim = nodeSimById[id];
  if (sim) focusOn(sim);
}}

function unfocus() {{
  focusedNode = null;
  visibleNodes = new Set(SIM_NODES.map(n => n.id));
  visibleEdges = new Set(SIM_EDGES.map((_, i) => i));
  focusBanner.classList.remove('visible');
}}

document.getElementById('btn-unfocus').addEventListener('click', unfocus);

// ── Toolbar ──
document.getElementById('btn-reset').addEventListener('click', () => {{
  unfocus();
  selectedNode = null;
  panel.classList.remove('open');
  camera = {{ x: 0, y: 0, zoom: 1 }};
  SIM_NODES.forEach(n => {{
    n.vx = (Math.random() - 0.5) * 10;
    n.vy = (Math.random() - 0.5) * 10;
    n.pinned = false;
  }});
  reheat(1.0);
}});

document.getElementById('btn-fit').addEventListener('click', () => {{
  const nodes = focusedNode
    ? [...visibleNodes].map(id => nodeSimById[id])
    : SIM_NODES;
  fitNodes(nodes);
}});

function fitNodes(nodes) {{
  if (!nodes.length) return;
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  nodes.forEach(n => {{
    minX = Math.min(minX, n.x - n.w / 2);
    maxX = Math.max(maxX, n.x + n.w / 2);
    minY = Math.min(minY, n.y - n.h / 2);
    maxY = Math.max(maxY, n.y + n.h / 2);
  }});
  const pad = 100;
  const bw = maxX - minX + pad * 2, bh = maxY - minY + pad * 2;
  const ww = window.innerWidth, wh = window.innerHeight;
  camera.zoom = Math.min(ww / bw, wh / bh, 2);
  camera.x = (minX + maxX) / 2;
  camera.y = (minY + maxY) / 2;
}}

// Layout was pre-computed and fit applied before first render — no delayed fit needed.
</script>
</body>
</html>"##,
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
                            coefficient: None,
                            form: DriverForm::default(),
                            intercept: None,
                            lag: None,
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

    #[test]
    fn test_subtree_diamond_graph() {
        // Diamond: top depends on A and B, both depend on shared leaf C.
        // Subtree of top should include all 4 nodes and all 4 edges.
        let layer = SemanticLayer {
            views: vec![make_view(
                "v",
                vec![
                    atomic_measure("c", MeasureType::Sum),
                    Measure {
                        name: "a".to_string(),
                        measure_type: MeasureType::Number,
                        expr: Some("{{v.c}} + 1".to_string()),
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
                    Measure {
                        name: "b".to_string(),
                        measure_type: MeasureType::Number,
                        expr: Some("{{v.c}} * 2".to_string()),
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
                    Measure {
                        name: "top".to_string(),
                        measure_type: MeasureType::Number,
                        expr: Some("{{v.a}} + {{v.b}}".to_string()),
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
        let sub = tree.subtree("v.top").unwrap();
        // All 4 nodes: top, a, b, c
        assert_eq!(sub.nodes.len(), 4);
        // All 4 edges: c->a, c->b, a->top, b->top
        assert_eq!(sub.edges.len(), 4, "Diamond graph should preserve all edges");
    }
}
