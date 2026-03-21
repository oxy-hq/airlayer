use crate::engine::EngineError;
use crate::schema::models::{EntityType, View};
use petgraph::algo::dijkstra;
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use std::collections::HashMap;

/// A join condition between two views.
#[derive(Debug, Clone)]
pub struct JoinEdge {
    /// The source view name.
    pub from_view: String,
    /// The target view name.
    pub to_view: String,
    /// The shared entity name that connects them.
    pub entity_name: String,
    /// SQL join conditions (one per key in the entity).
    pub conditions: Vec<JoinCondition>,
    /// Relationship type based on entity analysis.
    pub relationship: JoinRelationship,
}

#[derive(Debug, Clone)]
pub struct JoinCondition {
    /// Column in the source (foreign) view.
    pub from_column: String,
    /// Column in the target (primary) view.
    pub to_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinRelationship {
    ManyToOne,
    OneToMany,
    OneToOne,
}

impl JoinRelationship {
    pub fn as_str(&self) -> &str {
        match self {
            JoinRelationship::ManyToOne => "many_to_one",
            JoinRelationship::OneToMany => "one_to_many",
            JoinRelationship::OneToOne => "one_to_one",
        }
    }
}

/// The join graph: nodes are views, edges are join relationships derived from entities.
pub struct JoinGraph {
    graph: Graph<String, JoinEdge>,
    node_map: HashMap<String, NodeIndex>,
    edges: Vec<JoinEdge>,
}

impl JoinGraph {
    /// Build the join graph from a list of views by analyzing entity relationships.
    pub fn build(views: &[View]) -> Result<Self, EngineError> {
        let mut graph = Graph::new();
        let mut node_map = HashMap::new();
        let mut edges = Vec::new();

        // Add nodes
        for view in views {
            let idx = graph.add_node(view.name.clone());
            node_map.insert(view.name.clone(), idx);
        }

        // Build entity maps
        // primary_entities: entity_name -> (view_name, keys)
        let mut primary_entities: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();
        // foreign_entities: (view_name, entity_name, keys)
        let mut foreign_entities: Vec<(String, String, Vec<String>)> = Vec::new();

        for view in views {
            for entity in &view.entities {
                let keys = entity.get_keys();
                match entity.entity_type {
                    EntityType::Primary => {
                        primary_entities
                            .entry(entity.name.clone())
                            .or_default()
                            .push((view.name.clone(), keys));
                    }
                    EntityType::Foreign => {
                        foreign_entities.push((view.name.clone(), entity.name.clone(), keys));
                    }
                }
            }
        }

        // Generate edges: for each foreign entity, find matching primary entity
        for (foreign_view, entity_name, foreign_keys) in &foreign_entities {
            if let Some(primaries) = primary_entities.get(entity_name) {
                for (primary_view, primary_keys) in primaries {
                    if foreign_view == primary_view {
                        continue; // Skip self-joins
                    }

                    // Build join conditions
                    let conditions: Vec<JoinCondition> = foreign_keys
                        .iter()
                        .zip(primary_keys.iter())
                        .map(|(fk, pk)| JoinCondition {
                            from_column: fk.clone(),
                            to_column: pk.clone(),
                        })
                        .collect();

                    let edge = JoinEdge {
                        from_view: foreign_view.clone(),
                        to_view: primary_view.clone(),
                        entity_name: entity_name.clone(),
                        conditions,
                        relationship: JoinRelationship::ManyToOne,
                    };

                    let from_idx = node_map[foreign_view];
                    let to_idx = node_map[primary_view];
                    graph.add_edge(from_idx, to_idx, edge.clone());

                    // Also add reverse edge
                    let reverse = JoinEdge {
                        from_view: primary_view.clone(),
                        to_view: foreign_view.clone(),
                        entity_name: entity_name.clone(),
                        conditions: edge
                            .conditions
                            .iter()
                            .map(|c| JoinCondition {
                                from_column: c.to_column.clone(),
                                to_column: c.from_column.clone(),
                            })
                            .collect(),
                        relationship: JoinRelationship::OneToMany,
                    };
                    graph.add_edge(to_idx, from_idx, reverse.clone());

                    edges.push(edge);
                    edges.push(reverse);
                }
            }
        }

        Ok(JoinGraph {
            graph,
            node_map,
            edges,
        })
    }

    /// Find the shortest join path between two views.
    pub fn find_join_path(&self, from: &str, to: &str) -> Result<Vec<JoinEdge>, EngineError> {
        let from_idx = self
            .node_map
            .get(from)
            .ok_or_else(|| EngineError::JoinError(format!("View '{}' not found in join graph", from)))?;
        let to_idx = self
            .node_map
            .get(to)
            .ok_or_else(|| EngineError::JoinError(format!("View '{}' not found in join graph", to)))?;

        if from_idx == to_idx {
            return Ok(vec![]);
        }

        // Use Dijkstra to find shortest path
        let distances = dijkstra(&self.graph, *from_idx, Some(*to_idx), |_| 1u32);

        if !distances.contains_key(to_idx) {
            return Err(EngineError::JoinError(format!(
                "No join path found between '{}' and '{}'",
                from, to
            )));
        }

        // Reconstruct path via BFS
        self.reconstruct_path(*from_idx, *to_idx)
    }

    /// Find the shortest join path between two views, preferring paths that go
    /// through the specified entity names. When hints are provided, the BFS
    /// favours edges whose entity_name appears in the hints list.
    pub fn find_join_path_with_hints(
        &self,
        from: &str,
        to: &str,
        through: &[String],
    ) -> Result<Vec<JoinEdge>, EngineError> {
        if through.is_empty() {
            return self.find_join_path(from, to);
        }

        let from_idx = self
            .node_map
            .get(from)
            .ok_or_else(|| EngineError::JoinError(format!("View '{}' not found in join graph", from)))?;
        let to_idx = self
            .node_map
            .get(to)
            .ok_or_else(|| EngineError::JoinError(format!("View '{}' not found in join graph", to)))?;

        if from_idx == to_idx {
            return Ok(vec![]);
        }

        // Weighted BFS (Dijkstra): edges through hinted entities cost 0,
        // all others cost 2. This makes the algorithm strongly prefer paths
        // that traverse the requested entities while still finding any
        // reachable path.
        let through_set: std::collections::HashSet<&str> =
            through.iter().map(|s| s.as_str()).collect();

        let distances = dijkstra(&self.graph, *from_idx, Some(*to_idx), |edge_ref| {
            let edge = edge_ref.weight();
            if through_set.contains(edge.entity_name.as_str()) {
                0u32
            } else {
                2u32
            }
        });

        if !distances.contains_key(to_idx) {
            return Err(EngineError::JoinError(format!(
                "No join path found between '{}' and '{}'",
                from, to
            )));
        }

        // Reconstruct using weighted BFS that respects through hints
        self.reconstruct_path_with_hints(*from_idx, *to_idx, &through_set)
    }

    /// Reconstruct a path using weighted BFS that prefers hinted entities.
    fn reconstruct_path_with_hints(
        &self,
        from: NodeIndex,
        to: NodeIndex,
        through: &std::collections::HashSet<&str>,
    ) -> Result<Vec<JoinEdge>, EngineError> {
        // Use a priority queue (min-heap) for Dijkstra-style reconstruction
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        let mut dist: HashMap<NodeIndex, u32> = HashMap::new();
        let mut prev: HashMap<NodeIndex, Option<petgraph::graph::EdgeIndex>> = HashMap::new();
        let mut heap = BinaryHeap::new();

        dist.insert(from, 0);
        prev.insert(from, None);
        heap.push(Reverse((0u32, from)));

        while let Some(Reverse((cost, current))) = heap.pop() {
            if current == to {
                break;
            }
            if cost > *dist.get(&current).unwrap_or(&u32::MAX) {
                continue;
            }
            for edge in self.graph.edges(current) {
                let neighbor = edge.target();
                let w = if through.contains(edge.weight().entity_name.as_str()) {
                    0u32
                } else {
                    2u32
                };
                let new_cost = cost + w;
                if new_cost < *dist.get(&neighbor).unwrap_or(&u32::MAX) {
                    dist.insert(neighbor, new_cost);
                    prev.insert(neighbor, Some(edge.id()));
                    heap.push(Reverse((new_cost, neighbor)));
                }
            }
        }

        if !prev.contains_key(&to) {
            return Err(EngineError::JoinError("No path found".to_string()));
        }

        let mut path = Vec::new();
        let mut current = to;
        while let Some(Some(edge_id)) = prev.get(&current) {
            let edge = &self.graph[*edge_id];
            path.push(edge.clone());
            let (source, _) = self.graph.edge_endpoints(*edge_id).unwrap();
            current = source;
        }
        path.reverse();
        Ok(path)
    }

    /// Find join paths from a base view to all listed views.
    pub fn find_join_tree(
        &self,
        base_view: &str,
        target_views: &[&str],
    ) -> Result<Vec<JoinEdge>, EngineError> {
        let mut all_edges = Vec::new();
        let mut visited = std::collections::HashSet::new();
        visited.insert(base_view.to_string());

        for target in target_views {
            if *target == base_view {
                continue;
            }
            let path = self.find_join_path(base_view, target)?;
            for edge in path {
                let edge_key = format!("{}->{}", edge.from_view, edge.to_view);
                if visited.insert(edge_key) {
                    all_edges.push(edge);
                }
            }
        }

        Ok(all_edges)
    }

    /// Find join paths from a base view to all listed views, using through hints.
    pub fn find_join_tree_with_hints(
        &self,
        base_view: &str,
        target_views: &[&str],
        through: &[String],
    ) -> Result<Vec<JoinEdge>, EngineError> {
        if through.is_empty() {
            return self.find_join_tree(base_view, target_views);
        }

        let mut all_edges = Vec::new();
        let mut visited = std::collections::HashSet::new();
        visited.insert(base_view.to_string());

        for target in target_views {
            if *target == base_view {
                continue;
            }
            let path = self.find_join_path_with_hints(base_view, target, through)?;
            for edge in path {
                let edge_key = format!("{}->{}", edge.from_view, edge.to_view);
                if visited.insert(edge_key) {
                    all_edges.push(edge);
                }
            }
        }

        Ok(all_edges)
    }

    /// Get direct edges from a view.
    pub fn edges_from(&self, view: &str) -> Vec<&JoinEdge> {
        if let Some(&idx) = self.node_map.get(view) {
            self.graph
                .edges(idx)
                .map(|e| e.weight())
                .collect()
        } else {
            vec![]
        }
    }

    /// All edges in the graph.
    pub fn all_edges(&self) -> &[JoinEdge] {
        &self.edges
    }

    /// Compute the total number of join edges needed if `base` is the root.
    /// Returns None if no valid join tree exists.
    pub fn join_tree_cost(&self, base: &str, targets: &[&str]) -> Option<usize> {
        self.find_join_tree(base, targets)
            .ok()
            .map(|edges| edges.len())
    }

    /// Check if a view exists in the graph.
    pub fn has_view(&self, name: &str) -> bool {
        self.node_map.contains_key(name)
    }

    /// Reconstruct a path from BFS.
    fn reconstruct_path(
        &self,
        from: NodeIndex,
        to: NodeIndex,
    ) -> Result<Vec<JoinEdge>, EngineError> {
        use std::collections::VecDeque;

        let mut queue = VecDeque::new();
        let mut visited = HashMap::new();
        queue.push_back(from);
        visited.insert(from, None);

        while let Some(current) = queue.pop_front() {
            if current == to {
                break;
            }
            for edge in self.graph.edges(current) {
                let neighbor = edge.target();
                if !visited.contains_key(&neighbor) {
                    visited.insert(neighbor, Some(edge.id()));
                    queue.push_back(neighbor);
                }
            }
        }

        if !visited.contains_key(&to) {
            return Err(EngineError::JoinError("No path found".to_string()));
        }

        // Trace back
        let mut path = Vec::new();
        let mut current = to;
        while let Some(Some(edge_id)) = visited.get(&current) {
            let edge = &self.graph[*edge_id];
            path.push(edge.clone());
            // Find the source of this edge
            let (source, _) = self.graph.edge_endpoints(*edge_id).unwrap();
            current = source;
        }
        path.reverse();
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::models::*;

    fn make_view(name: &str, entities: Vec<Entity>) -> View {
        View {
            name: name.to_string(),
            description: "".to_string(),
            label: None,
            datasource: None,
            dialect: None,
            table: Some(name.to_string()),
            sql: None,
            entities,
            dimensions: vec![
                Dimension {
                    name: "id".to_string(),
                    dimension_type: DimensionType::Number,
                    description: None,
                    expr: "id".to_string(),
                    original_expr: None,
                    samples: None,
                    synonyms: None,
                    primary_key: None,
                    sub_query: None,
                    inherits_from: None,
                },
            ],
            measures: None,
            segments: vec![],
        }
    }

    #[test]
    fn test_simple_join() {
        let orders = make_view(
            "orders",
            vec![
                Entity {
                    name: "order".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
                Entity {
                    name: "customer".to_string(),
                    entity_type: EntityType::Foreign,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
            ],
        );
        let customers = make_view(
            "customers",
            vec![Entity {
                name: "customer".to_string(),
                entity_type: EntityType::Primary,
                description: None,
                key: Some("id".to_string()),
                keys: None,
                inherits_from: None,
            }],
        );

        let graph = JoinGraph::build(&[orders, customers]).unwrap();
        let path = graph.find_join_path("orders", "customers").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].entity_name, "customer");
    }

    #[test]
    fn test_transitive_join() {
        let orders = make_view(
            "orders",
            vec![
                Entity {
                    name: "order".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
            ],
        );
        let order_items = make_view(
            "order_items",
            vec![
                Entity {
                    name: "order_item".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
                Entity {
                    name: "order".to_string(),
                    entity_type: EntityType::Foreign,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
            ],
        );
        let products = make_view(
            "products",
            vec![Entity {
                name: "product".to_string(),
                entity_type: EntityType::Primary,
                description: None,
                key: Some("id".to_string()),
                keys: None,
                inherits_from: None,
            }],
        );
        // order_items has foreign entity "product"
        let mut oi = order_items;
        oi.entities.push(Entity {
            name: "product".to_string(),
            entity_type: EntityType::Foreign,
            description: None,
            key: Some("id".to_string()),
            keys: None,
            inherits_from: None,
        });

        let graph = JoinGraph::build(&[orders, oi, products]).unwrap();
        // orders -> order_items -> products (transitive)
        let path = graph.find_join_path("orders", "products").unwrap();
        assert_eq!(path.len(), 2);
    }

    #[test]
    fn test_through_hints_prefer_specified_entity() {
        // Build a diamond graph:
        //   orders --[warehouse_order]--> warehouses
        //   orders --[store_order]--> stores
        //   warehouses --[shipment]--> shipments
        //   stores --[shipment]--> shipments
        //
        // Without hints: either path is valid (2 hops).
        // With through=["warehouse_order"]: should go through warehouses.
        // With through=["store_order"]: should go through stores.

        let orders = make_view(
            "orders",
            vec![
                Entity {
                    name: "order".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
                Entity {
                    name: "warehouse_order".to_string(),
                    entity_type: EntityType::Foreign,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
                Entity {
                    name: "store_order".to_string(),
                    entity_type: EntityType::Foreign,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
            ],
        );
        let warehouses = make_view(
            "warehouses",
            vec![
                Entity {
                    name: "warehouse_order".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
                Entity {
                    name: "shipment".to_string(),
                    entity_type: EntityType::Foreign,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
            ],
        );
        let stores = make_view(
            "stores",
            vec![
                Entity {
                    name: "store_order".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
                Entity {
                    name: "shipment".to_string(),
                    entity_type: EntityType::Foreign,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
            ],
        );
        let shipments = make_view(
            "shipments",
            vec![Entity {
                name: "shipment".to_string(),
                entity_type: EntityType::Primary,
                description: None,
                key: Some("id".to_string()),
                keys: None,
                inherits_from: None,
            }],
        );

        let graph =
            JoinGraph::build(&[orders, warehouses, stores, shipments]).unwrap();

        // With through=["warehouse_order"], path must go through warehouses
        let path = graph
            .find_join_path_with_hints(
                "orders",
                "shipments",
                &["warehouse_order".to_string()],
            )
            .unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].to_view, "warehouses");
        assert_eq!(path[1].to_view, "shipments");

        // With through=["store_order"], path must go through stores
        let path = graph
            .find_join_path_with_hints(
                "orders",
                "shipments",
                &["store_order".to_string()],
            )
            .unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].to_view, "stores");
        assert_eq!(path[1].to_view, "shipments");
    }

    #[test]
    fn test_through_empty_hints_same_as_default() {
        // Same as test_transitive_join but via find_join_path_with_hints with empty hints
        let orders = make_view(
            "orders",
            vec![Entity {
                name: "order".to_string(),
                entity_type: EntityType::Primary,
                description: None,
                key: Some("id".to_string()),
                keys: None,
                inherits_from: None,
            }],
        );
        let order_items = make_view(
            "order_items",
            vec![
                Entity {
                    name: "order_item".to_string(),
                    entity_type: EntityType::Primary,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
                Entity {
                    name: "order".to_string(),
                    entity_type: EntityType::Foreign,
                    description: None,
                    key: Some("id".to_string()),
                    keys: None,
                    inherits_from: None,
                },
            ],
        );

        let graph = JoinGraph::build(&[orders, order_items]).unwrap();
        let path_default = graph.find_join_path("orders", "order_items").unwrap();
        let path_hints = graph
            .find_join_path_with_hints("orders", "order_items", &[])
            .unwrap();
        assert_eq!(path_default.len(), path_hints.len());
    }
}
