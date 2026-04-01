use crate::schema::globals::GlobalSemantics;
use crate::schema::models::*;
use std::path::Path;

/// Parses .view.yml and .topic.yml files into a SemanticLayer.
pub struct SchemaParser {
    globals: Option<GlobalSemantics>,
}

impl SchemaParser {
    pub fn new() -> Self {
        Self { globals: None }
    }

    pub fn with_globals(globals: GlobalSemantics) -> Self {
        Self {
            globals: Some(globals),
        }
    }

    /// Parse a directory tree containing .view.yml and optionally .topic.yml files.
    #[cfg(feature = "cli")]
    pub fn parse_directory(
        &self,
        views_dir: &Path,
        topics_dir: Option<&Path>,
    ) -> Result<SemanticLayer, String> {
        self.parse_directory_full(views_dir, topics_dir, None, None)
    }

    /// Parse a directory tree with optional motifs and sequences directories.
    #[cfg(feature = "cli")]
    pub fn parse_directory_full(
        &self,
        views_dir: &Path,
        topics_dir: Option<&Path>,
        motifs_dir: Option<&Path>,
        sequences_dir: Option<&Path>,
    ) -> Result<SemanticLayer, String> {
        let views = self.parse_views(views_dir)?;
        let topics = if let Some(td) = topics_dir {
            Some(self.parse_topics(td)?)
        } else {
            None
        };
        let motifs = if let Some(md) = motifs_dir {
            let m = self.parse_motifs(md)?;
            if m.is_empty() { None } else { Some(m) }
        } else {
            None
        };
        let sequences = if let Some(sd) = sequences_dir {
            let s = self.parse_sequences(sd)?;
            if s.is_empty() { None } else { Some(s) }
        } else {
            None
        };
        Ok(SemanticLayer::with_motifs_and_sequences(views, topics, motifs, sequences))
    }

    /// Parse all .view.yml files in a directory (recursively).
    #[cfg(feature = "cli")]
    pub fn parse_views(&self, dir: &Path) -> Result<Vec<View>, String> {
        let mut views = Vec::new();
        let pattern = dir.join("**/*.view.yml");
        let pattern_str = pattern
            .to_str()
            .ok_or("Invalid path encoding")?;

        for entry in glob::glob(pattern_str).map_err(|e| format!("Glob error: {}", e))? {
            let path = entry.map_err(|e| format!("Path error: {}", e))?;
            let view = self.parse_view_file(&path)?;
            views.push(view);
        }

        // Also try .yml files directly named (not just *.view.yml) if nothing found
        if views.is_empty() {
            let pattern2 = dir.join("*.view.yml");
            let pattern_str2 = pattern2.to_str().ok_or("Invalid path encoding")?;
            for entry in glob::glob(pattern_str2).map_err(|e| format!("Glob error: {}", e))? {
                let path = entry.map_err(|e| format!("Path error: {}", e))?;
                let view = self.parse_view_file(&path)?;
                views.push(view);
            }
        }

        Ok(views)
    }

    /// Parse a single .view.yml file.
    #[cfg(feature = "cli")]
    pub fn parse_view_file(&self, path: &Path) -> Result<View, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        self.parse_view_str(&content, path.to_str().unwrap_or("<unknown>"))
    }

    /// Parse a view from a YAML string.
    pub fn parse_view_str(&self, yaml: &str, source: &str) -> Result<View, String> {
        let raw: RawView = serde_yaml::from_str(yaml)
            .map_err(|e| format!("Failed to parse view YAML from {}: {}", source, e))?;
        self.resolve_raw_view(raw)
    }

    /// Resolve inheritance in a RawView to produce a View.
    fn resolve_raw_view(&self, raw: RawView) -> Result<View, String> {
        let entities = self.resolve_entities(raw.entities)?;
        let dimensions = self.resolve_dimensions(raw.dimensions)?;
        let measures = if let Some(items) = raw.measures {
            Some(self.resolve_measures(items)?)
        } else {
            None
        };

        Ok(View {
            name: raw.name,
            description: raw.description,
            label: raw.label,
            datasource: raw.datasource,
            dialect: raw.dialect,
            table: raw.table,
            sql: raw.sql,
            entities,
            dimensions,
            measures,
            segments: raw.segments,
        })
    }

    fn resolve_entities(&self, items: Vec<EntityItem>) -> Result<Vec<Entity>, String> {
        let mut result = Vec::new();
        for item in items {
            match item {
                EntityItem::Inline(mut entity) => {
                    // If the entity has inherits_from, merge with globals
                    if let Some(ref path) = entity.inherits_from {
                        if let Ok(global) = self.resolve_entity_inheritance(path) {
                            // Use global values as defaults for missing fields
                            entity.entity_type = global.entity_type;
                            if entity.key.is_none() && entity.keys.is_none() {
                                entity.key = global.key;
                                entity.keys = global.keys;
                            }
                            if entity.description.is_none() {
                                entity.description = global.description;
                            }
                        }
                    }
                    result.push(entity);
                }
                EntityItem::Inherit { inherits_from } => {
                    let entity = self.resolve_entity_inheritance(&inherits_from)?;
                    result.push(entity);
                }
            }
        }
        Ok(result)
    }

    fn resolve_dimensions(&self, items: Vec<DimensionItem>) -> Result<Vec<Dimension>, String> {
        let mut result = Vec::new();
        for item in items {
            match item {
                DimensionItem::Inline(dim) => result.push(dim),
                DimensionItem::Inherit { inherits_from } => {
                    let dim = self.resolve_dimension_inheritance(&inherits_from)?;
                    result.push(dim);
                }
            }
        }
        Ok(result)
    }

    fn resolve_measures(&self, items: Vec<MeasureItem>) -> Result<Vec<Measure>, String> {
        let mut result = Vec::new();
        for item in items {
            match item {
                MeasureItem::Inline(measure) => result.push(measure),
                MeasureItem::Inherit { inherits_from } => {
                    let measure = self.resolve_measure_inheritance(&inherits_from)?;
                    result.push(measure);
                }
            }
        }
        Ok(result)
    }

    fn resolve_entity_inheritance(&self, path: &str) -> Result<Entity, String> {
        // Format: globals.semantics.entities.<name>
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() != 4 || parts[0] != "globals" || parts[1] != "semantics" || parts[2] != "entities" {
            return Err(format!(
                "Invalid entity inheritance path: '{}'. Expected: globals.semantics.entities.<name>",
                path
            ));
        }
        let name = parts[3];
        let globals = self.globals.as_ref().ok_or_else(|| {
            format!("No globals loaded, but entity '{}' references globals", name)
        })?;
        let global = globals.entities.get(name).ok_or_else(|| {
            format!("Global entity '{}' not found", name)
        })?;

        let entity_type = match global.entity_type.as_str() {
            "primary" => EntityType::Primary,
            "foreign" => EntityType::Foreign,
            other => return Err(format!("Invalid entity type '{}' in global entity '{}'", other, name)),
        };

        Ok(Entity {
            name: global.name.clone(),
            entity_type,
            description: global.description.clone(),
            key: global.key.clone(),
            keys: global.keys.clone(),
            inherits_from: Some(path.to_string()),
        })
    }

    fn resolve_dimension_inheritance(&self, path: &str) -> Result<Dimension, String> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() != 4 || parts[0] != "globals" || parts[1] != "semantics" || parts[2] != "dimensions" {
            return Err(format!(
                "Invalid dimension inheritance path: '{}'. Expected: globals.semantics.dimensions.<name>",
                path
            ));
        }
        let name = parts[3];
        let globals = self.globals.as_ref().ok_or_else(|| {
            format!("No globals loaded, but dimension '{}' references globals", name)
        })?;
        let global = globals.dimensions.get(name).ok_or_else(|| {
            format!("Global dimension '{}' not found", name)
        })?;

        let dimension_type = parse_dimension_type(&global.dimension_type)?;

        Ok(Dimension {
            name: name.to_string(),
            dimension_type,
            description: global.description.clone(),
            expr: global.expr.clone(),
            original_expr: None,
            samples: global.samples.clone(),
            synonyms: global.synonyms.clone(),
            primary_key: None,
            sub_query: None,
            inherits_from: Some(path.to_string()),
        })
    }

    fn resolve_measure_inheritance(&self, path: &str) -> Result<Measure, String> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() != 4 || parts[0] != "globals" || parts[1] != "semantics" || parts[2] != "measures" {
            return Err(format!(
                "Invalid measure inheritance path: '{}'. Expected: globals.semantics.measures.<name>",
                path
            ));
        }
        let name = parts[3];
        let globals = self.globals.as_ref().ok_or_else(|| {
            format!("No globals loaded, but measure '{}' references globals", name)
        })?;
        let global = globals.measures.get(name).ok_or_else(|| {
            format!("Global measure '{}' not found", name)
        })?;

        let measure_type = parse_measure_type(&global.measure_type)?;

        let filters = global.filters.as_ref().map(|fs| {
            fs.iter()
                .map(|f| MeasureFilter {
                    expr: f.expr.clone(),
                    original_expr: None,
                    description: f.description.clone(),
                })
                .collect()
        });

        Ok(Measure {
            name: name.to_string(),
            measure_type,
            description: global.description.clone(),
            expr: global.expr.clone(),
            original_expr: None,
            filters,
            samples: global.samples.clone(),
            synonyms: global.synonyms.clone(),
            rolling_window: None,
            inherits_from: Some(path.to_string()),
        })
    }

    /// Parse .motif.yml files from a directory.
    #[cfg(feature = "cli")]
    pub fn parse_motifs(&self, dir: &Path) -> Result<Vec<Motif>, String> {
        let mut motifs = Vec::new();
        let pattern = dir.join("**/*.motif.yml");
        let pattern_str = pattern.to_str().ok_or("Invalid path encoding")?;

        for entry in glob::glob(pattern_str).map_err(|e| format!("Glob error: {}", e))? {
            let path = entry.map_err(|e| format!("Path error: {}", e))?;
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
            let motif: Motif = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse motif {}: {}", path.display(), e))?;
            motifs.push(motif);
        }

        Ok(motifs)
    }

    /// Parse .sequence.yml files from a directory.
    #[cfg(feature = "cli")]
    pub fn parse_sequences(&self, dir: &Path) -> Result<Vec<Sequence>, String> {
        let mut sequences = Vec::new();
        let pattern = dir.join("**/*.sequence.yml");
        let pattern_str = pattern.to_str().ok_or("Invalid path encoding")?;

        for entry in glob::glob(pattern_str).map_err(|e| format!("Glob error: {}", e))? {
            let path = entry.map_err(|e| format!("Path error: {}", e))?;
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
            let sequence: Sequence = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse sequence {}: {}", path.display(), e))?;
            sequences.push(sequence);
        }

        Ok(sequences)
    }

    /// Parse .topic.yml files from a directory.
    #[cfg(feature = "cli")]
    pub fn parse_topics(&self, dir: &Path) -> Result<Vec<Topic>, String> {
        let mut topics = Vec::new();
        let pattern = dir.join("**/*.topic.yml");
        let pattern_str = pattern.to_str().ok_or("Invalid path encoding")?;

        for entry in glob::glob(pattern_str).map_err(|e| format!("Glob error: {}", e))? {
            let path = entry.map_err(|e| format!("Path error: {}", e))?;
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
            let topic: Topic = serde_yaml::from_str(&content)
                .map_err(|e| format!("Failed to parse topic {}: {}", path.display(), e))?;
            topics.push(topic);
        }

        Ok(topics)
    }
}

fn parse_dimension_type(s: &str) -> Result<DimensionType, String> {
    match s.to_lowercase().as_str() {
        "string" => Ok(DimensionType::String),
        "number" => Ok(DimensionType::Number),
        "date" => Ok(DimensionType::Date),
        "datetime" => Ok(DimensionType::Datetime),
        "boolean" => Ok(DimensionType::Boolean),
        other => Err(format!("Unknown dimension type: '{}'", other)),
    }
}

fn parse_measure_type(s: &str) -> Result<MeasureType, String> {
    match s.to_lowercase().as_str() {
        "count" => Ok(MeasureType::Count),
        "sum" => Ok(MeasureType::Sum),
        "average" | "avg" => Ok(MeasureType::Average),
        "min" => Ok(MeasureType::Min),
        "max" => Ok(MeasureType::Max),
        "count_distinct" => Ok(MeasureType::CountDistinct),
        "median" => Ok(MeasureType::Median),
        "custom" => Ok(MeasureType::Custom),
        other => Err(format!("Unknown measure type: '{}'", other)),
    }
}

impl Default for SchemaParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_view() {
        let yaml = r#"
name: orders
description: Customer orders
table: orders
entities:
  - name: order
    type: primary
    description: Primary order entity
    key: order_id
dimensions:
  - name: order_id
    type: number
    description: Unique order identifier
    expr: id
  - name: status
    type: string
    description: Order status
    expr: status
measures:
  - name: total_orders
    type: count
    description: Total number of orders
  - name: total_revenue
    type: sum
    description: Total revenue
    expr: amount
"#;

        let parser = SchemaParser::new();
        let view = parser.parse_view_str(yaml, "test").unwrap();

        assert_eq!(view.name, "orders");
        assert_eq!(view.entities.len(), 1);
        assert_eq!(view.entities[0].name, "order");
        assert_eq!(view.entities[0].entity_type, EntityType::Primary);
        assert_eq!(view.dimensions.len(), 2);
        assert_eq!(view.dimensions[0].name, "order_id");
        assert_eq!(view.dimensions[0].dimension_type, DimensionType::Number);
        assert_eq!(view.measures_list().len(), 2);
        assert_eq!(view.measures_list()[0].measure_type, MeasureType::Count);
        assert_eq!(view.measures_list()[1].measure_type, MeasureType::Sum);
    }

    #[test]
    fn test_parse_view_with_sql() {
        let yaml = r#"
name: active_orders
description: Active orders only
sql: "SELECT * FROM orders WHERE status = 'active'"
entities: []
dimensions:
  - name: id
    type: number
    description: Order id
    expr: id
"#;

        let parser = SchemaParser::new();
        let view = parser.parse_view_str(yaml, "test").unwrap();

        assert!(view.table.is_none());
        assert!(view.sql.is_some());
        assert!(view.source_sql().contains("SELECT"));
    }

    #[test]
    fn test_parse_composite_keys() {
        let yaml = r#"
name: order_items
description: Order line items
table: order_items
entities:
  - name: order_item
    type: primary
    description: Line item
    keys:
      - order_id
      - line_item_id
dimensions:
  - name: order_id
    type: number
    description: Order ID
    expr: order_id
  - name: line_item_id
    type: number
    description: Line item ID
    expr: line_item_id
"#;

        let parser = SchemaParser::new();
        let view = parser.parse_view_str(yaml, "test").unwrap();

        assert!(view.entities[0].is_composite());
        assert_eq!(view.entities[0].get_keys().len(), 2);
    }

    #[test]
    fn test_parse_measure_with_filters() {
        let yaml = r#"
name: customers
description: Customers
table: customers
entities: []
dimensions:
  - name: id
    type: number
    expr: id
    description: ID
  - name: gender
    type: string
    expr: gender
    description: Gender
measures:
  - name: male_count
    type: count
    description: Count of male customers
    expr: id
    filters:
      - expr: "gender = 'M'"
        description: Males only
"#;

        let parser = SchemaParser::new();
        let view = parser.parse_view_str(yaml, "test").unwrap();
        let measure = &view.measures_list()[0];
        assert!(measure.filters.is_some());
        assert_eq!(measure.filters.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_parse_geo_dimension_type() {
        let yaml = r#"
name: locations
description: Store locations
table: locations
entities: []
dimensions:
  - name: id
    type: number
    description: Location id
    expr: id
  - name: coordinates
    type: geo
    description: GPS coordinates
    expr: coordinates
  - name: city
    type: string
    description: City name
    expr: city
"#;

        let parser = SchemaParser::new();
        let view = parser.parse_view_str(yaml, "test").unwrap();
        assert_eq!(view.dimensions.len(), 3);
        assert_eq!(view.dimensions[1].name, "coordinates");
        assert_eq!(view.dimensions[1].dimension_type, DimensionType::Geo);
        assert_eq!(view.dimensions[1].dimension_type.to_string(), "geo");
        // Geo and String are distinct types
        assert_ne!(view.dimensions[1].dimension_type, DimensionType::String);
        assert_eq!(view.dimensions[2].dimension_type, DimensionType::String);
    }

    #[test]
    fn test_parse_view_with_dialect() {
        let yaml = r#"
name: orders
description: Customer orders
dialect: bigquery
table: orders
dimensions:
  - name: id
    type: number
    description: Order ID
    expr: id
"#;

        let parser = SchemaParser::new();
        let view = parser.parse_view_str(yaml, "test").unwrap();
        assert_eq!(view.dialect, Some("bigquery".to_string()));
    }

    #[test]
    fn test_parse_view_without_dialect() {
        let yaml = r#"
name: orders
description: Customer orders
table: orders
dimensions:
  - name: id
    type: number
    description: Order ID
    expr: id
"#;

        let parser = SchemaParser::new();
        let view = parser.parse_view_str(yaml, "test").unwrap();
        assert_eq!(view.dialect, None);
    }

    #[test]
    fn test_parse_builtin_motif() {
        let yaml = r#"
name: yoy
description: Year-over-year comparison
type: builtin
params:
  measure:
    type: measure
    constraints: [numeric]
  time:
    type: dimension
    constraints: [temporal]
"#;
        let motif: Motif = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(motif.name, "yoy");
        assert_eq!(motif.motif_kind, MotifKind::Builtin);
        assert!(motif.params.contains_key("measure"));
        assert!(motif.params.contains_key("time"));
    }

    #[test]
    fn test_parse_custom_motif() {
        let yaml = r#"
name: custom_ratio
description: Custom ratio computation
type: custom
params:
  measure:
    type: measure
outputs:
  - name: doubled
    expr: "{{ measure }} * 2"
  - name: halved
    expr: "{{ measure }} / 2.0"
"#;
        let motif: Motif = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(motif.name, "custom_ratio");
        assert_eq!(motif.motif_kind, MotifKind::Custom);
        assert_eq!(motif.outputs.len(), 2);
        assert_eq!(motif.outputs[0].name, "doubled");
    }

    #[test]
    fn test_parse_sequence() {
        let yaml = r#"
name: revenue_analysis
description: Multi-step revenue analysis
params:
  period:
    type: enum
    values: [month, quarter, year]
    default: month
steps:
  - name: overall_trend
    query:
      measures: ["orders.total_revenue"]
      motif: trend
  - name: anomaly_check
    query:
      measures: ["orders.total_revenue"]
      motif: anomaly
"#;
        let seq: Sequence = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(seq.name, "revenue_analysis");
        assert_eq!(seq.steps.len(), 2);
        assert_eq!(seq.steps[0].name, "overall_trend");
        assert_eq!(seq.steps[1].name, "anomaly_check");
    }
}
