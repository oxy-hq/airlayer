//! Promotion closure (entity-hierarchy walk).
//!
//! A *promotion* is the pushforward of a measure from a fine-grained view to
//! a coarser-grained view. The hierarchy that drives promotion lives on the
//! **entity**, not on the view: each Primary entity declaration may carry
//! `parent: <other_entity>`, asserting that this entity rolls up to that one.
//! Any measure declared on any view at the child entity's grain is then
//! induced on every ancestor's grain.
//!
//! Why on the entity: the relationship "store rolls up to company" is a
//! property of the store_id entity. Sales, returns, and inventory all share
//! that entity; declaring the parent on the entity (in one place) means every
//! fact view that uses store_id participates in the hierarchy for free.
//!
//! Direction is unambiguous: `parent:` points at the parent. Even when both
//! views over-declare Foreign/Primary symmetrically (the `customers/orders`
//! case), only the entity that names a parent participates — declaration
//! beats convention.
//!
//! Outputs:
//! - `Promotions::all_induced` — every (target_view, measure_name) pair with
//!   its source view, the entity path taken, and an additivity class read
//!   from `MeasureType::additivity_class()`.
//! - `Promotions::collisions` — induced names dropped because the target view
//!   already declares a measure of that name (explicit wins).
//! - `Promotions::ambiguities` — same induced name reachable from multiple
//!   source views (planner resolves via `through`).

use crate::engine::EngineError;
use crate::schema::models::{AdditivityClass, EntityType, View};
use std::collections::{HashMap, HashSet, VecDeque};

/// An induced (promoted) measure: a measure declared on `source_view` that
/// becomes queryable as `target_view.<source_measure>` because of the entity
/// hierarchy between them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InducedMeasure {
    pub target_view: String,
    pub source_view: String,
    pub source_measure: String,
    /// Ordered list of entity names walked from source's grain entity to the
    /// target view's primary entity (length == number of hops).
    pub path: Vec<String>,
    pub additivity: AdditivityClass,
}

/// An induced measure dropped because the target view already declares a
/// measure with the same name. Explicit beats induced; the drop is recorded
/// so the validator/inspect can warn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionCollision {
    pub target_view: String,
    pub measure_name: String,
    pub dropped_sources: Vec<(String, Vec<String>)>,
}

/// The same induced name is reachable from multiple distinct source views.
/// All candidates stay in the closure so the planner can pick one via
/// `through`; this report exists so tooling can surface the ambiguity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionAmbiguity {
    pub target_view: String,
    pub measure_name: String,
    pub candidates: Vec<(String, Vec<String>)>,
}

/// Promotion closure for a `SemanticLayer`.
#[derive(Debug, Default, Clone)]
pub struct Promotions {
    induced: HashMap<(String, String), Vec<InducedMeasure>>,
    collisions: Vec<PromotionCollision>,
    ambiguities: Vec<PromotionAmbiguity>,
    /// entity_name → the view that owns it as Primary. Public access via
    /// `primary_owner` so callers (inspect, RCA) can navigate the hierarchy.
    primary_owner: HashMap<String, String>,
    /// entity_name → parent entity (read off the Primary declaration).
    /// Cycles are rejected at build time.
    entity_parent: HashMap<String, String>,
    /// Reverse adjacency: entity_name → child entity names (those that name
    /// it as their parent). Powers the hierarchy-aware RCA splitter.
    entity_children: HashMap<String, Vec<String>>,
}

impl Promotions {
    /// Build the closure from a list of views.
    pub fn build(views: &[View]) -> Result<Self, EngineError> {
        // 1. Index views.
        let view_by_name: HashMap<&str, &View> =
            views.iter().map(|v| (v.name.as_str(), v)).collect();

        // 2. Build entity_parent and primary_owner from Primary declarations.
        //    A `parent:` on a Foreign declaration is silently ignored here
        //    (the validator emits an error before we get this far when the
        //    layer is run through validation).
        let mut primary_owner: HashMap<String, String> = HashMap::new();
        let mut entity_parent: HashMap<String, String> = HashMap::new();
        for v in views {
            for e in &v.entities {
                if e.entity_type != EntityType::Primary {
                    continue;
                }
                primary_owner
                    .entry(e.name.clone())
                    .or_insert_with(|| v.name.clone());
                if let Some(p) = &e.parent {
                    entity_parent.insert(e.name.clone(), p.clone());
                }
            }
        }

        // 3. Cycle detection in the entity-parent graph.
        Self::detect_cycle(&entity_parent)?;

        // 4. Reverse adjacency: child entities of each entity. Used by the
        //    hierarchy-aware RCA splitter.
        let mut entity_children: HashMap<String, Vec<String>> = HashMap::new();
        for (child, parent) in &entity_parent {
            entity_children
                .entry(parent.clone())
                .or_default()
                .push(child.clone());
        }
        for v in entity_children.values_mut() {
            v.sort();
        }

        // 5. For each source view, BFS the entity hierarchy:
        //    - first hop: source's Foreign entity → its Primary owner view
        //    - subsequent hops: at the current grain-entity, follow `parent:`
        //      to the next entity → its Primary owner view
        let mut induced: HashMap<(String, String), Vec<InducedMeasure>> = HashMap::new();
        for source in views {
            if source.measures_list().is_empty() {
                continue;
            }

            // Seed the BFS with each Foreign entity on the source view that
            // has a known Primary owner.
            let mut queue: VecDeque<(String, String, Vec<String>)> = VecDeque::new();
            for e in &source.entities {
                if e.entity_type != EntityType::Foreign {
                    continue;
                }
                let Some(owner) = primary_owner.get(&e.name) else {
                    continue;
                };
                if *owner == source.name {
                    continue; // self
                }
                queue.push_back((e.name.clone(), owner.clone(), vec![e.name.clone()]));
            }

            let mut visited: HashSet<String> = HashSet::new();
            visited.insert(source.name.clone());

            while let Some((entity, target, path)) = queue.pop_front() {
                if !visited.insert(target.clone()) {
                    continue;
                }
                for m in source.measures_list() {
                    if m.shift.is_some() {
                        continue;
                    }
                    induced
                        .entry((target.clone(), m.name.clone()))
                        .or_default()
                        .push(InducedMeasure {
                            target_view: target.clone(),
                            source_view: source.name.clone(),
                            source_measure: m.name.clone(),
                            path: path.clone(),
                            additivity: m.measure_type.additivity_class(),
                        });
                }
                // Walk one hop up the hierarchy via `parent:` on the current
                // entity (read off its Primary declaration above).
                if let Some(parent) = entity_parent.get(&entity) {
                    if let Some(owner) = primary_owner.get(parent) {
                        let mut next_path = path.clone();
                        next_path.push(parent.clone());
                        queue.push_back((parent.clone(), owner.clone(), next_path));
                    }
                }
            }
        }

        // 6. Drop induced entries that collide with an explicit measure on
        //    the target view.
        let mut collisions: Vec<PromotionCollision> = Vec::new();
        induced.retain(|(target, name), entries| {
            let explicit = view_by_name
                .get(target.as_str())
                .map(|v| v.measures_list().iter().any(|m| &m.name == name))
                .unwrap_or(false);
            if explicit {
                let mut dropped: Vec<(String, Vec<String>)> = entries
                    .iter()
                    .map(|im| (im.source_view.clone(), im.path.clone()))
                    .collect();
                dropped.sort();
                collisions.push(PromotionCollision {
                    target_view: target.clone(),
                    measure_name: name.clone(),
                    dropped_sources: dropped,
                });
                false
            } else {
                true
            }
        });

        // 7. Ambiguity: same induced name from more than one source view.
        let mut ambiguities: Vec<PromotionAmbiguity> = Vec::new();
        for ((target, name), entries) in &induced {
            let distinct_sources: HashSet<&str> =
                entries.iter().map(|im| im.source_view.as_str()).collect();
            if distinct_sources.len() > 1 {
                let mut candidates: Vec<(String, Vec<String>)> = entries
                    .iter()
                    .map(|im| (im.source_view.clone(), im.path.clone()))
                    .collect();
                candidates.sort();
                ambiguities.push(PromotionAmbiguity {
                    target_view: target.clone(),
                    measure_name: name.clone(),
                    candidates,
                });
            }
        }
        collisions.sort_by(|a, b| {
            (&a.target_view, &a.measure_name).cmp(&(&b.target_view, &b.measure_name))
        });
        ambiguities.sort_by(|a, b| {
            (&a.target_view, &a.measure_name).cmp(&(&b.target_view, &b.measure_name))
        });

        Ok(Self {
            induced,
            collisions,
            ambiguities,
            primary_owner,
            entity_parent,
            entity_children,
        })
    }

    /// DFS-based cycle detection in the entity-parent graph. Errors with a
    /// readable trace if a cycle is found. A self-loop (`a parent: a`) counts.
    fn detect_cycle(entity_parent: &HashMap<String, String>) -> Result<(), EngineError> {
        #[derive(Clone, Copy, PartialEq)]
        enum Mark {
            White,
            Gray,
            Black,
        }
        let mut mark: HashMap<&str, Mark> = entity_parent
            .keys()
            .map(|k| (k.as_str(), Mark::White))
            .collect();

        for start in entity_parent.keys() {
            if mark.get(start.as_str()).copied() != Some(Mark::White) {
                continue;
            }
            let mut stack: Vec<&str> = vec![start.as_str()];
            let mut path: Vec<&str> = Vec::new();
            while let Some(node) = stack.last().copied() {
                match mark.get(node).copied() {
                    Some(Mark::White) => {
                        mark.insert(node, Mark::Gray);
                        path.push(node);
                        if let Some(parent) = entity_parent.get(node) {
                            match mark.get(parent.as_str()).copied() {
                                Some(Mark::Gray) => {
                                    let cycle_start =
                                        path.iter().position(|n| *n == parent.as_str()).unwrap();
                                    let trace = path[cycle_start..].to_vec();
                                    return Err(EngineError::SchemaError(format!(
                                        "entity parent hierarchy has a cycle: {} → {}",
                                        trace.join(" → "),
                                        parent
                                    )));
                                }
                                Some(Mark::White) => {
                                    stack.push(parent.as_str());
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Mark::Gray) => {
                        mark.insert(node, Mark::Black);
                        path.pop();
                        stack.pop();
                    }
                    _ => {
                        stack.pop();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn all_induced(&self) -> impl Iterator<Item = &InducedMeasure> {
        self.induced.values().flat_map(|v| v.iter())
    }

    pub fn induced_for_view(&self, target_view: &str) -> Vec<&InducedMeasure> {
        let mut out: Vec<&InducedMeasure> = self
            .induced
            .iter()
            .filter(|((tv, _), _)| tv == target_view)
            .flat_map(|(_, v)| v.iter())
            .collect();
        out.sort_by(|a, b| {
            (&a.source_measure, &a.source_view).cmp(&(&b.source_measure, &b.source_view))
        });
        out
    }

    pub fn candidates(&self, target_view: &str, measure_name: &str) -> &[InducedMeasure] {
        self.induced
            .get(&(target_view.to_string(), measure_name.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn collisions(&self) -> &[PromotionCollision] {
        &self.collisions
    }

    pub fn ambiguities(&self) -> &[PromotionAmbiguity] {
        &self.ambiguities
    }

    /// Parent of `entity` in the hierarchy, if any. The Primary declaration
    /// of `entity` named this as its parent.
    pub fn parent_of(&self, entity: &str) -> Option<&str> {
        self.entity_parent.get(entity).map(|s| s.as_str())
    }

    /// Direct children of `entity` — entities whose Primary declaration names
    /// `entity` as their parent. Powers hierarchy-aware drill-down (RCA).
    pub fn children_of(&self, entity: &str) -> &[String] {
        self.entity_children
            .get(entity)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// View that owns this entity as Primary, if any.
    pub fn primary_owner(&self, entity: &str) -> Option<&str> {
        self.primary_owner.get(entity).map(|s| s.as_str())
    }

    /// Walk from `entity` up through `parent:` until the chain terminates.
    /// Includes `entity` as the first element.
    pub fn ancestry(&self, entity: &str) -> Vec<String> {
        let mut out = vec![entity.to_string()];
        let mut current = entity.to_string();
        let mut seen: HashSet<String> = HashSet::from([entity.to_string()]);
        while let Some(parent) = self.entity_parent.get(&current) {
            if !seen.insert(parent.clone()) {
                break; // cycle guard (already caught at build, double-check)
            }
            out.push(parent.clone());
            current = parent.clone();
        }
        out
    }

    /// All descendants of `entity` (transitive children). Excludes `entity`.
    pub fn descendants(&self, entity: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        for c in self.children_of(entity) {
            queue.push_back(c.clone());
        }
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(c) = queue.pop_front() {
            if !seen.insert(c.clone()) {
                continue;
            }
            for cc in self.children_of(&c) {
                queue.push_back(cc.clone());
            }
            out.push(c);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::*;

    fn dim(name: &str) -> Dimension {
        Dimension {
            name: name.to_string(),
            dimension_type: DimensionType::String,
            description: None,
            expr: name.to_string(),
            original_expr: None,
            samples: None,
            synonyms: None,
            primary_key: None,
            sub_query: None,
            inherits_from: None,
            meta: None,
        }
    }

    fn measure(name: &str, mt: MeasureType, expr: Option<&str>) -> Measure {
        Measure {
            name: name.to_string(),
            measure_type: mt,
            description: None,
            expr: expr.map(|s| s.to_string()),
            original_expr: None,
            filters: None,
            samples: None,
            synonyms: None,
            rolling_window: None,
            inherits_from: None,
            drivers: None,
            shift: None,
            meta: None,
        }
    }

    fn ent(name: &str, ty: EntityType, key: &str, parent: Option<&str>) -> Entity {
        Entity {
            name: name.to_string(),
            entity_type: ty,
            description: None,
            key: Some(key.to_string()),
            keys: None,
            lifespan: None,
            inherits_from: None,
            meta: None,
            parent: parent.map(|s| s.to_string()),
        }
    }

    fn view(name: &str, entities: Vec<Entity>, measures: Vec<Measure>) -> View {
        let mut dims: Vec<Dimension> = Vec::new();
        for e in &entities {
            for k in e.get_keys() {
                if !dims.iter().any(|d| d.name == k) {
                    dims.push(dim(&k));
                }
            }
        }
        View {
            name: name.to_string(),
            description: None,
            label: None,
            datasource: None,
            dialect: None,
            table: Some(name.to_string()),
            sql: None,
            entities,
            dimensions: dims,
            measures: if measures.is_empty() {
                None
            } else {
                Some(measures)
            },
            segments: vec![],
            pre_aggregations: None,
            refresh_key: None,
            meta: None,
        }
    }

    /// Foreign + matching Primary → measures appear on the immediate parent.
    /// No `parent:` on the entity is needed for a single-hop edge.
    #[test]
    fn single_hop_induces_on_parent_view() {
        let sales = view(
            "sales",
            vec![ent("store_id", EntityType::Foreign, "store_id", None)],
            vec![measure("net_sales", MeasureType::Sum, Some("amount"))],
        );
        let stores = view(
            "stores",
            vec![ent("store_id", EntityType::Primary, "store_id", None)],
            vec![],
        );
        let p = Promotions::build(&[sales, stores]).unwrap();
        let cands = p.candidates("stores", "net_sales");
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].path, vec!["store_id"]);
        assert_eq!(cands[0].additivity, AdditivityClass::Additive);
    }

    /// A 3-level hierarchy declared via `parent:` on each entity. sales →
    /// stores → companies → markets, all driven from the entity declarations.
    #[test]
    fn transitive_hierarchy_via_parent_chains() {
        let sales = view(
            "sales",
            vec![ent("store_id", EntityType::Foreign, "store_id", None)],
            vec![measure("net_sales", MeasureType::Sum, Some("amount"))],
        );
        let stores = view(
            "stores",
            vec![
                ent(
                    "store_id",
                    EntityType::Primary,
                    "store_id",
                    Some("company_id"),
                ),
                ent("company_id", EntityType::Foreign, "company_id", None),
            ],
            vec![],
        );
        let companies = view(
            "companies",
            vec![
                ent(
                    "company_id",
                    EntityType::Primary,
                    "company_id",
                    Some("market_id"),
                ),
                ent("market_id", EntityType::Foreign, "market_id", None),
            ],
            vec![],
        );
        let markets = view(
            "markets",
            vec![ent("market_id", EntityType::Primary, "market_id", None)],
            vec![],
        );
        let p = Promotions::build(&[sales, stores, companies, markets]).unwrap();
        assert_eq!(
            p.candidates("stores", "net_sales")[0].path,
            vec!["store_id"]
        );
        assert_eq!(
            p.candidates("companies", "net_sales")[0].path,
            vec!["store_id", "company_id"]
        );
        assert_eq!(
            p.candidates("markets", "net_sales")[0].path,
            vec!["store_id", "company_id", "market_id"]
        );
    }

    /// The customers/orders over-declaration case. Only the entity with a
    /// `parent:` participates in the hierarchy; nothing rolls up the wrong
    /// direction.
    #[test]
    fn over_declared_foreign_is_harmless_without_parent() {
        let orders = view(
            "orders",
            vec![
                ent(
                    "order_id",
                    EntityType::Primary,
                    "order_id",
                    Some("customer_id"),
                ),
                ent("customer_id", EntityType::Foreign, "customer_id", None),
            ],
            vec![measure("revenue", MeasureType::Sum, Some("amount"))],
        );
        // customers redundantly declares Foreign order_id. With no `parent:`
        // on customer_id, customers' nonexistent measures don't go anywhere.
        let customers = view(
            "customers",
            vec![
                ent("customer_id", EntityType::Primary, "customer_id", None),
                ent("order_id", EntityType::Foreign, "order_id", None),
            ],
            vec![],
        );
        let p = Promotions::build(&[orders, customers]).unwrap();
        // orders.revenue rolls up to customers (correct direction).
        assert_eq!(
            p.candidates("customers", "revenue")[0].source_view,
            "orders"
        );
        // Nothing rolls "down" to orders.
        assert!(p.candidates("orders", "any_measure").is_empty());
    }

    /// Two fact views share the same parent grain via the same entity →
    /// ambiguity is recorded but each side stays available for the planner
    /// to resolve via `through`.
    #[test]
    fn shared_parent_with_two_facts_is_ambiguous_but_kept() {
        let sales = view(
            "sales",
            vec![ent("store_id", EntityType::Foreign, "store_id", None)],
            vec![measure("net_amount", MeasureType::Sum, Some("amount"))],
        );
        let returns = view(
            "returns",
            vec![ent("store_id", EntityType::Foreign, "store_id", None)],
            vec![measure("net_amount", MeasureType::Sum, Some("refund"))],
        );
        let stores = view(
            "stores",
            vec![ent("store_id", EntityType::Primary, "store_id", None)],
            vec![],
        );
        let p = Promotions::build(&[sales, returns, stores]).unwrap();
        assert_eq!(p.candidates("stores", "net_amount").len(), 2);
        assert_eq!(p.ambiguities().len(), 1);
    }

    /// Explicit measure on the parent shadows the induced one (warning case).
    #[test]
    fn explicit_measure_on_parent_shadows_induced() {
        let sales = view(
            "sales",
            vec![ent("store_id", EntityType::Foreign, "store_id", None)],
            vec![measure("net_sales", MeasureType::Sum, Some("amount"))],
        );
        let stores = view(
            "stores",
            vec![ent("store_id", EntityType::Primary, "store_id", None)],
            vec![measure("net_sales", MeasureType::Sum, Some("override"))],
        );
        let p = Promotions::build(&[sales, stores]).unwrap();
        assert!(p.candidates("stores", "net_sales").is_empty());
        assert_eq!(p.collisions().len(), 1);
    }

    /// Cycles in the parent hierarchy are rejected at build time.
    #[test]
    fn parent_cycle_is_rejected() {
        let a = view(
            "a",
            vec![ent("ea", EntityType::Primary, "ea", Some("eb"))],
            vec![],
        );
        let b = view(
            "b",
            vec![ent("eb", EntityType::Primary, "eb", Some("ea"))],
            vec![],
        );
        let err = Promotions::build(&[a, b]).expect_err("cycle must error");
        let msg = format!("{err:?}");
        assert!(msg.contains("cycle"), "expected a cycle error, got: {msg}");
    }

    /// Additivity is read from MeasureType per spec.
    #[test]
    fn additivity_classes_match_measure_types() {
        let sales = view(
            "sales",
            vec![ent("store_id", EntityType::Foreign, "store_id", None)],
            vec![
                measure("revenue", MeasureType::Sum, Some("a")),
                measure("orders", MeasureType::Count, None),
                measure("avg_t", MeasureType::Average, Some("a")),
                measure("ratio", MeasureType::Number, Some("{{sales.revenue}} / 2")),
            ],
        );
        let stores = view(
            "stores",
            vec![ent("store_id", EntityType::Primary, "store_id", None)],
            vec![],
        );
        let p = Promotions::build(&[sales, stores]).unwrap();
        let cls = |n: &str| p.candidates("stores", n)[0].additivity;
        assert_eq!(cls("revenue"), AdditivityClass::Additive);
        assert_eq!(cls("orders"), AdditivityClass::Additive);
        assert_eq!(cls("avg_t"), AdditivityClass::NonAdditive);
        assert_eq!(cls("ratio"), AdditivityClass::Passthrough);
    }

    /// Hierarchy navigation: ancestry / descendants / children_of.
    #[test]
    fn hierarchy_navigation_helpers() {
        // store_id → company_id → market_id
        let stores = view(
            "stores",
            vec![ent(
                "store_id",
                EntityType::Primary,
                "store_id",
                Some("company_id"),
            )],
            vec![],
        );
        let companies = view(
            "companies",
            vec![ent(
                "company_id",
                EntityType::Primary,
                "company_id",
                Some("market_id"),
            )],
            vec![],
        );
        let markets = view(
            "markets",
            vec![ent("market_id", EntityType::Primary, "market_id", None)],
            vec![],
        );
        let p = Promotions::build(&[stores, companies, markets]).unwrap();
        assert_eq!(p.parent_of("store_id"), Some("company_id"));
        assert_eq!(p.parent_of("market_id"), None);
        assert_eq!(p.children_of("market_id"), &["company_id".to_string()]);
        assert_eq!(
            p.ancestry("store_id"),
            vec!["store_id", "company_id", "market_id"]
        );
        assert_eq!(p.descendants("market_id"), vec!["company_id", "store_id"]);
    }
}
