use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Ordering;
use std::hash::Hash;

use petgraph::Direction;
use petgraph::algo::Measure;
use petgraph::graph::DiGraph;
use petgraph::graph::NodeIndex;
use petgraph::graph::EdgeReference;
use petgraph::visit::{EdgeRef, IntoEdgesDirected, VisitMap, Visitable};

use super::static_sim::Node;
use super::static_sim::RoutingGraphEdge;
type GG = DiGraph<Node, RoutingGraphEdge>;


/// An implementation of Backwards dijkstra (searches from destination to all nodes, or to 
/// a user-specifiable subset).  Based on (aka mostly copied from) the implementation in the
/// petgraph library.
///
/// Compute the length of the shortest path to `goal` from every node from 
/// which goal is reachable.
///
/// The graph should be `Visitable` and implement `IntoEdges`. The function
/// `edge_cost` should return the cost for a particular edge, which is used
/// to compute path costs. Edge costs must be non-negative.
///
/// If `goal` is not `None`, then the algorithm terminates once the `goal` node's
/// cost is calculated.
///
/// Returns two `HashMap`s: first maps `NodeId` to path cost, and second maps 'NodeId' to 
/// the edge taken from that node on the shortest path.
/// ```


pub fn bkwds_dijkstra_with_paths<F, K>(
    graph: &GG,
    goal: NodeIndex,
    mut origins: Option<HashSet<NodeIndex>>,
    used_nodes: Option<HashSet<NodeIndex>>,
    mut edge_cost: F,
) -> (HashMap<NodeIndex, K>, HashMap<NodeIndex, EdgeReference<RoutingGraphEdge>>)
where
    NodeIndex: Eq + Hash,
    F: FnMut(EdgeReference<RoutingGraphEdge>) -> K,
    K: Measure + Copy,
{
    let mut visited = graph.visit_map();
    let mut scores = HashMap::new();
    let mut edges_used = HashMap::new();
    let zero_score = K::default();
    scores.insert(goal, zero_score);

    let mut visit_next = BinaryHeap::new();
    visit_next.push(MinScored(zero_score, (goal, "no_outgoing_route")));
    while let Some(MinScored(node_score, (node, outgoing_route))) = visit_next.pop() {
        if visited.is_visited(&node) {
            continue;
        }
        if let Some(origins) = &mut origins {
            if origins.contains(&node) {
                origins.remove(&node);
            }
            if origins.len() == 0 {
                break;
            }
        }
        for edge in graph.edges_directed(node, Direction::Incoming) {
            let incoming_route = &edge.weight().route_id;
            if incoming_route == outgoing_route {
                continue;
            }
            let prev = edge.source();
            if visited.is_visited(&prev) {
                continue;
            }
            match &used_nodes {
                // check if we're supposed to ignore this node
                Some(used_nodes) if ! used_nodes.contains(&prev) => continue,
                _ => (),
            }
            let prev_score = node_score + edge_cost(edge);
            match scores.entry(prev) {
                Occupied(ent) => {
                    if prev_score < *ent.get() {
                        *ent.into_mut() = prev_score;
                        visit_next.push(MinScored(prev_score, (prev, incoming_route)));
                        edges_used.insert(prev.clone(), edge);
                    }
                }
                Vacant(ent) => {
                    ent.insert(prev_score);
                    visit_next.push(MinScored(prev_score, (prev, incoming_route)));
                    edges_used.insert(prev.clone(), edge);
                }
            }
        }
        visited.visit(node);
    }
    (scores, edges_used)
}


#[derive(Copy, Clone, Debug)]
pub struct MinScored<K, T>(pub K, pub T);

impl<K: PartialOrd, T> PartialEq for MinScored<K, T> {
    #[inline]
    fn eq(&self, other: &MinScored<K, T>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<K: PartialOrd, T> Eq for MinScored<K, T> {}

impl<K: PartialOrd, T> PartialOrd for MinScored<K, T> {
    #[inline]
    fn partial_cmp(&self, other: &MinScored<K, T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: PartialOrd, T> Ord for MinScored<K, T> {
    #[inline]
    fn cmp(&self, other: &MinScored<K, T>) -> Ordering {
        let a = &self.0;
        let b = &other.0;
        if a == b {
            Ordering::Equal
        } else if a < b {
            Ordering::Greater
        } else if a > b {
            Ordering::Less
        } else if a.ne(a) && b.ne(b) {
            // these are the NaN cases
            Ordering::Equal
        } else if a.ne(a) {
            // Order NaN less, so that it is last in the MinScore order
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::Graph;
    use petgraph::prelude::*;
    

    // simple graph with two nodes
    #[test]
    fn test_simple_graph() {
        // TODO write this test
        let mut graph: GG = Graph::new();
        let aa = graph.add_node(Node{}); // node with no weight
        let bb = graph.add_node(Node{});
        graph.add_edge(aa, bb, RoutingGraphEdge::new("test", 1.));
        let (costs, path_edges) = bkwds_dijkstra_with_paths(&graph, bb, None, None, |_| 1);

        let expected_costs: HashMap<NodeIndex, usize> = [
            (aa, 1),
            (bb, 0),
        ].iter().cloned().collect();
        assert_eq!(costs, expected_costs);
        let expected_edges: HashMap<NodeIndex, NodeIndex> = [
            (aa, bb),
        ].iter().cloned().collect();
        let path_edges: HashMap<NodeIndex, NodeIndex> = path_edges.iter().map(
            |(ni, er)| (ni.clone(), er.target())).collect();
        assert_eq!(expected_edges, path_edges);
    }

    #[test]    
    fn test_origins() {
        let mut graph: GG = Graph::new();
        let aa = graph.add_node(Node{}); // node with no weight
        let bb = graph.add_node(Node{});
        let cc = graph.add_node(Node{});
        let dd = graph.add_node(Node{});
        let ee = graph.add_node(Node{});
        let ff = graph.add_node(Node{});
        let gg = graph.add_node(Node{});
        let hh = graph.add_node(Node{});
        
        // graph.extend_with_edges(&[
        graph.add_edge(aa, bb, RoutingGraphEdge::new("0", 1.));
        graph.add_edge(bb, cc, RoutingGraphEdge::new("1", 1.));
        graph.add_edge(cc, dd, RoutingGraphEdge::new("2", 1.));
        graph.add_edge(dd, aa, RoutingGraphEdge::new("3", 1.));
        graph.add_edge(ee, ff, RoutingGraphEdge::new("4", 1.));
        graph.add_edge(bb, ee, RoutingGraphEdge::new("5", 1.));
        graph.add_edge(ff, gg, RoutingGraphEdge::new("6", 1.));
        graph.add_edge(gg, hh, RoutingGraphEdge::new("7", 1.));
        graph.add_edge(hh, ee, RoutingGraphEdge::new("8", 1.));
        // a ----> b ----> e ----> f
        // ^       |       ^       |
        // |       v       |       v
        // d <---- c       h <---- g

        let origins: HashSet<NodeIndex> = [
            aa,
            dd,
        ].iter().cloned().collect();
        
        // z is not inside res because there is not path from b to z.
        let expected_costs: HashMap<NodeIndex, usize> = [
            (aa, 1),
            (bb, 0),
            (dd, 2),
        ].iter().cloned().collect();
        let expected_edges: HashMap<NodeIndex, NodeIndex> = [
            (aa, bb),
            (dd, aa),
        ].iter().cloned().collect();

        let (costs, path_edges) = bkwds_dijkstra_with_paths(&graph, bb, Some(origins), None, |_| 1);
        assert_eq!(costs, expected_costs);

        let path_edges: HashMap<NodeIndex, NodeIndex> = path_edges.iter().map(
            |(ni, er)| (ni.clone(), er.target())).collect();
        assert_eq!(path_edges, expected_edges);
    }

    #[test]
    fn test_used_nodes_twoloops() {
        let mut graph: GG = Graph::new();
        let aa = graph.add_node(Node{}); // node with no weight
        let bb = graph.add_node(Node{});
        let cc = graph.add_node(Node{});
        let dd = graph.add_node(Node{});
        let ee = graph.add_node(Node{});
        let ff = graph.add_node(Node{});
        let gg = graph.add_node(Node{});
        let hh = graph.add_node(Node{});
        
        graph.add_edge(aa, bb, RoutingGraphEdge::new("0", 1.));
        graph.add_edge(bb, cc, RoutingGraphEdge::new("1", 1.));
        graph.add_edge(cc, dd, RoutingGraphEdge::new("2", 1.));
        graph.add_edge(dd, aa, RoutingGraphEdge::new("3", 1.));
        graph.add_edge(ee, ff, RoutingGraphEdge::new("4", 1.));
        graph.add_edge(bb, ee, RoutingGraphEdge::new("5", 1.));
        graph.add_edge(ff, gg, RoutingGraphEdge::new("6", 1.));
        graph.add_edge(gg, hh, RoutingGraphEdge::new("7", 1.));
        graph.add_edge(hh, ee, RoutingGraphEdge::new("8", 1.));

        let used_nodes: HashSet<NodeIndex> = [
            aa,
            dd,
            ff,
        ].iter().cloned().collect();
        
        // z is not inside res because there is not path from b to z.
        let expected_costs: HashMap<NodeIndex, usize> = [
            (aa, 1),
            (bb, 0),
            (dd, 2),
        ].iter().cloned().collect();
        // TODO check path_edges
        let expected_edges: HashMap<NodeIndex, NodeIndex> = [
            (aa, bb),
            (dd, aa),
        ].iter().cloned().collect();

        let (costs, path_edges) = bkwds_dijkstra_with_paths(&graph, bb, None, Some(used_nodes), 
                                                            |_| 1);
        assert_eq!(costs, expected_costs);

        let path_edges: HashMap<NodeIndex, NodeIndex> = path_edges.iter().map(
            |(ni, er)| (ni.clone(), er.target())).collect();
        assert_eq!(path_edges, expected_edges);
    }

    #[test]
    fn test_used_nodes_twopaths() {
        let mut graph: GG = Graph::new();
        let aa = graph.add_node(Node{}); // node with no weight
        let bb = graph.add_node(Node{});
        let cc = graph.add_node(Node{});
        let dd = graph.add_node(Node{});
        let ee = graph.add_node(Node{});
        
        graph.add_edge(aa, bb, RoutingGraphEdge::new("0", 1.));
        graph.add_edge(bb, cc, RoutingGraphEdge::new("1", 1.));
        graph.add_edge(cc, ee, RoutingGraphEdge::new("2", 1.));
        graph.add_edge(aa, dd, RoutingGraphEdge::new("3", 1.));
        graph.add_edge(dd, ee, RoutingGraphEdge::new("4", 1.));
        // a ----> b ----> c ----> e
        // |                       ^
        // ----------> d --------- |

        let expected_costs: HashMap<NodeIndex, usize> = [
            (aa, 2),
            (bb, 2),
            (cc, 1),
            (dd, 1),
            (ee, 0),
        ].iter().cloned().collect();
        let expected_edges: HashMap<NodeIndex, NodeIndex> = [
            (aa, dd),
            (bb, cc),
            (cc, ee),
            (dd, ee),
        ].iter().cloned().collect();

        let (costs, path_edges) = bkwds_dijkstra_with_paths(&graph, ee, None, None, |_| 1);
        assert_eq!(costs, expected_costs);

        let path_edges: HashMap<NodeIndex, NodeIndex> = path_edges.iter().map(
            |(ni, er)| (ni.clone(), er.target())).collect();
        assert_eq!(path_edges, expected_edges);


        let used_nodes: HashSet<NodeIndex> = [
            aa,
            bb,
            cc,
        ].iter().cloned().collect();
        
        let expected_costs: HashMap<NodeIndex, usize> = [
            (aa, 3),
            (bb, 2),
            (cc, 1),
            (ee, 0),
        ].iter().cloned().collect();
        let expected_edges: HashMap<NodeIndex, NodeIndex> = [
            (aa, bb),
            (bb, cc),
            (cc, ee),
        ].iter().cloned().collect();
        graph.add_edge(aa, bb, RoutingGraphEdge::new("0.1", 1.));
        graph.add_edge(bb, cc, RoutingGraphEdge::new("1.1", 1.));
        graph.add_edge(cc, ee, RoutingGraphEdge::new("2.1", 1.));
        graph.add_edge(aa, dd, RoutingGraphEdge::new("3.1", 1.));
        graph.add_edge(dd, ee, RoutingGraphEdge::new("4.1", 1.));

        let (costs, path_edges) = bkwds_dijkstra_with_paths(&graph, ee, None, Some(used_nodes), 
                                                            |_| 1);
        assert_eq!(costs, expected_costs);

        let path_edges: HashMap<NodeIndex, NodeIndex> = path_edges.iter().map(
            |(ni, er)| (ni.clone(), er.target())).collect();
        assert_eq!(path_edges, expected_edges);
    }


    #[test]
    fn test_petgraph_example() {
        let mut graph: GG = Graph::new();
        let aa = graph.add_node(Node{}); // node with no weight
        let bb = graph.add_node(Node{});
        let cc = graph.add_node(Node{});
        let dd = graph.add_node(Node{});
        let ee = graph.add_node(Node{});
        let ff = graph.add_node(Node{});
        let gg = graph.add_node(Node{});
        let hh = graph.add_node(Node{});
    
        graph.add_edge(aa, bb, RoutingGraphEdge::new("0", 1.));
        graph.add_edge(bb, cc, RoutingGraphEdge::new("1", 1.));
        graph.add_edge(cc, dd, RoutingGraphEdge::new("2", 1.));
        graph.add_edge(dd, aa, RoutingGraphEdge::new("3", 1.));
        graph.add_edge(ee, ff, RoutingGraphEdge::new("4", 1.));
        graph.add_edge(bb, ee, RoutingGraphEdge::new("5", 1.));
        graph.add_edge(ff, gg, RoutingGraphEdge::new("6", 1.));
        graph.add_edge(gg, hh, RoutingGraphEdge::new("7", 1.));
        graph.add_edge(hh, ee, RoutingGraphEdge::new("8", 1.));

        // a ----> b ----> e ----> f
        // ^       |       ^       |
        // |       v       |       v
        // d <---- c       h <---- g
        
        let expected_costs: HashMap<NodeIndex, usize> = [
            (aa, 1),
            (bb, 0),
            (cc, 3),
            (dd, 2),
        ].iter().cloned().collect();
        let expected_edges: HashMap<NodeIndex, NodeIndex> = [
            (aa, bb),
            (cc, dd),
            (dd, aa),
        ].iter().cloned().collect();
        let (costs, path_edges) = bkwds_dijkstra_with_paths(&graph, bb, None, None, |_| 1);
        assert_eq!(costs, expected_costs);

        let path_edges: HashMap<NodeIndex, NodeIndex> = path_edges.iter().map(
            |(ni, er)| (ni.clone(), er.target())).collect();
        assert_eq!(path_edges, expected_edges);
    }
}