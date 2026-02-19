//! DAG building and topological sorting

use crate::error::{CoreError, CoreResult};
use crate::model_name::ModelName;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use std::collections::{HashMap, HashSet, VecDeque};

/// A directed acyclic graph of model dependencies
#[derive(Debug)]
pub struct ModelDag {
    /// The underlying graph
    graph: DiGraph<ModelName, ()>,

    /// Map from model name to node index
    node_map: HashMap<ModelName, NodeIndex>,
}

impl ModelDag {
    /// Create a new empty DAG
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_map: HashMap::new(),
        }
    }

    /// Add a model to the DAG
    pub fn add_model(&mut self, name: &str) -> CoreResult<NodeIndex> {
        if let Some(&idx) = self.node_map.get(name) {
            Ok(idx)
        } else {
            let model_name = ModelName::try_new(name).ok_or_else(|| CoreError::EmptyName {
                context: "model name in DAG".into(),
            })?;
            let idx = self.graph.add_node(model_name.clone());
            self.node_map.insert(model_name, idx);
            Ok(idx)
        }
    }

    /// Add a dependency edge (from depends on to)
    pub fn add_dependency(&mut self, from: &str, to: &str) -> CoreResult<()> {
        let from_idx = self.add_model(from)?;
        let to_idx = self.add_model(to)?;
        // Edge goes from dependency to dependent (to -> from)
        // This way topological sort gives us dependencies first
        self.graph.add_edge(to_idx, from_idx, ());
        Ok(())
    }

    /// Build the DAG from a map of model name -> dependencies
    pub fn build(dependencies: &HashMap<String, Vec<String>>) -> CoreResult<Self> {
        let mut dag = Self::new();

        for model in dependencies.keys() {
            dag.add_model(model)?;
        }

        for (model, deps) in dependencies {
            for dep in deps {
                // Only add edge if the dependency is also a model (not external)
                if dependencies.contains_key(dep) {
                    dag.add_dependency(model, dep)?;
                }
            }
        }

        dag.validate()?;

        Ok(dag)
    }

    /// Validate the DAG has no cycles
    pub fn validate(&self) -> CoreResult<()> {
        match toposort(&self.graph, None) {
            Ok(_) => Ok(()),
            Err(cycle) => {
                let cycle_str = self.find_cycle_path(cycle.node_id());
                Err(CoreError::CircularDependency { cycle: cycle_str })
            }
        }
    }

    /// Find a cycle path starting from a node for error reporting
    fn find_cycle_path(&self, start: NodeIndex) -> String {
        let mut path: Vec<String> = vec![self.graph[start].to_string()];
        let mut current = start;
        let mut visited = HashSet::new();
        visited.insert(current);

        while let Some(edge) = self.graph.edges(current).next() {
            let target = edge.target();
            path.push(self.graph[target].to_string());

            if target == start || visited.contains(&target) {
                break;
            }

            visited.insert(target);
            current = target;
        }

        path.join(" -> ")
    }

    /// Get models in topological order (dependencies first)
    pub fn topological_order(&self) -> CoreResult<Vec<String>> {
        self.topological_order_names()
            .map(|names| names.into_iter().map(|n| n.to_string()).collect())
    }

    /// Get models in topological order as `ModelName` values
    pub fn topological_order_names(&self) -> CoreResult<Vec<ModelName>> {
        match toposort(&self.graph, None) {
            Ok(indices) => Ok(indices
                .into_iter()
                .map(|idx| self.graph[idx].clone())
                .collect()),
            Err(cycle) => {
                let cycle_str = self.find_cycle_path(cycle.node_id());
                Err(CoreError::CircularDependency { cycle: cycle_str })
            }
        }
    }

    /// Get models in reverse topological order (dependents first)
    pub fn reverse_topological_order(&self) -> CoreResult<Vec<String>> {
        let mut order = self.topological_order()?;
        order.reverse();
        Ok(order)
    }

    /// Get direct dependencies of a model
    pub fn dependencies(&self, model: &str) -> Vec<String> {
        if let Some(&idx) = self.node_map.get(model) {
            self.graph
                .edges_directed(idx, petgraph::Direction::Incoming)
                .map(|e| self.graph[e.source()].to_string())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get direct dependents of a model
    pub fn dependents(&self, model: &str) -> Vec<String> {
        if let Some(&idx) = self.node_map.get(model) {
            self.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| self.graph[e.target()].to_string())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get all ancestors (transitive dependencies) of a model
    pub fn ancestors(&self, model: &str) -> Vec<String> {
        if let Some(&idx) = self.node_map.get(model) {
            self.collect_reachable(idx, petgraph::Direction::Incoming)
        } else {
            Vec::new()
        }
    }

    /// Get ancestors up to `max_depth` hops away using BFS
    pub fn ancestors_bounded(&self, model: &str, max_depth: usize) -> Vec<String> {
        self.traverse_bfs_bounded(model, petgraph::Direction::Incoming, max_depth)
    }

    /// Get all descendants (transitive dependents) of a model
    pub fn descendants(&self, model: &str) -> Vec<String> {
        if let Some(&idx) = self.node_map.get(model) {
            self.collect_reachable(idx, petgraph::Direction::Outgoing)
        } else {
            Vec::new()
        }
    }

    /// Get descendants up to `max_depth` hops away using BFS
    pub fn descendants_bounded(&self, model: &str, max_depth: usize) -> Vec<String> {
        self.traverse_bfs_bounded(model, petgraph::Direction::Outgoing, max_depth)
    }

    /// Collect all nodes reachable from `start` by following edges in `direction` (DFS).
    fn collect_reachable(&self, start: NodeIndex, direction: petgraph::Direction) -> Vec<String> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        self.collect_reachable_dfs(start, direction, &mut result, &mut visited);
        result
    }

    fn collect_reachable_dfs(
        &self,
        idx: NodeIndex,
        direction: petgraph::Direction,
        result: &mut Vec<String>,
        visited: &mut HashSet<NodeIndex>,
    ) {
        for edge in self.graph.edges_directed(idx, direction) {
            let neighbor = match direction {
                petgraph::Direction::Incoming => edge.source(),
                petgraph::Direction::Outgoing => edge.target(),
            };
            if visited.insert(neighbor) {
                result.push(self.graph[neighbor].to_string());
                self.collect_reachable_dfs(neighbor, direction, result, visited);
            }
        }
    }

    /// BFS traversal collecting nodes up to `max_depth` hops from `model` in `direction`.
    fn traverse_bfs_bounded(
        &self,
        model: &str,
        direction: petgraph::Direction,
        max_depth: usize,
    ) -> Vec<String> {
        let Some(&start) = self.node_map.get(model) else {
            return Vec::new();
        };

        let mut result = Vec::new();
        let mut visited = HashSet::new();
        visited.insert(start);
        let mut queue = VecDeque::new();
        queue.push_back((start, 0usize));

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            for edge in self.graph.edges_directed(current, direction) {
                let neighbor = match direction {
                    petgraph::Direction::Incoming => edge.source(),
                    petgraph::Direction::Outgoing => edge.target(),
                };
                if visited.insert(neighbor) {
                    result.push(self.graph[neighbor].to_string());
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        result
    }

    /// Get models matching a selector pattern
    /// Supports: +model (ancestors + model), model+ (model + descendants)
    ///
    /// **Deprecated**: Prefer [`Selector`](crate::selector::Selector) for richer
    /// selection syntax (tags, paths, state-based selectors).
    pub fn select(&self, selector: &str) -> CoreResult<Vec<String>> {
        let (prefix, model_name, suffix) = Self::parse_selector(selector);

        let model_name = if self.node_map.contains_key(model_name) {
            model_name.to_string()
        } else {
            return Err(CoreError::ModelNotFound {
                name: model_name.to_string(),
            });
        };

        let mut selected = vec![model_name.clone()];

        if prefix {
            selected.extend(self.ancestors(&model_name));
        }

        if suffix {
            selected.extend(self.descendants(&model_name));
        }

        let order = self.topological_order()?;
        let selected_set: HashSet<_> = selected.into_iter().collect();
        Ok(order
            .into_iter()
            .filter(|m| selected_set.contains(m))
            .collect())
    }

    /// Parse a selector string into (has_prefix, model_name, has_suffix)
    fn parse_selector(selector: &str) -> (bool, &str, bool) {
        let prefix = selector.starts_with('+');
        let suffix = selector.ends_with('+');

        let model_name = selector.trim_start_matches('+').trim_end_matches('+');

        (prefix, model_name, suffix)
    }

    /// Get all model names in the DAG
    pub fn models(&self) -> Vec<ModelName> {
        self.node_map.keys().cloned().collect()
    }

    /// Check if a model exists in the DAG
    pub fn contains(&self, model: &str) -> bool {
        self.node_map.contains_key(model)
    }
}

impl Default for ModelDag {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "dag_test.rs"]
mod tests;
