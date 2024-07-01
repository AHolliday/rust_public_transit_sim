use std::collections::HashMap;
use std::collections::HashSet;
use std::cmp::Ordering;
use std::cmp::Ord;
use std::iter::Iterator;
use std::path::PathBuf;
use itertools::Itertools;
use itertools::iproduct;

use petgraph::Direction;
use petgraph::graph::DiGraph;
use petgraph::graphmap::DiGraphMap;
use petgraph::graph::NodeIndex;
use petgraph::graph::EdgeReference;
use petgraph::visit::EdgeRef;
use ndarray::prelude::*;
use yaml_rust::Yaml;
use yaml_rust::YamlLoader;
use rand::Rng;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use rand_isaac::Isaac64Rng;
use rayon::prelude::*;
use priority_queue::PriorityQueue;

use super::geometry::Point2d;
use super::geometry::euclidean_distances;
use super::PtVehicleType;
use super::path_network::PathSegment;
use super::path_network::PathNetwork;
use super::config_utils;
use super::PassengerTrip;
use super::SimConfig;

use super::bkwds_dijkstra_with_paths;


static RAND_SEED: u64 = 100;
static WALK_KEY: &str = "walk";

#[derive(Clone)]
pub struct RoutingGraphEdge {
    pub route_id: String,
    time_s: f64,
}

impl RoutingGraphEdge {
    pub fn new(route_id: &str, time_s: f64) -> RoutingGraphEdge {
        return RoutingGraphEdge{route_id: String::from(route_id), time_s};
    }
}

struct Candidate {
    next_node_id: usize,
    frequency: f64,
    time_from_boarding: f64,
}

enum Path{
    Walkingpath(WalkingPath),
    Hyperpath(Hyperpath),
    Directpath(DirectPath)
}

impl Path {
    fn mean_journey_time(&self) -> f64 {
        match self {
            Path::Walkingpath(wp) => wp.mean_journey_time(),
            Path::Hyperpath(hp) => hp.mean_journey_time,
            Path::Directpath(dp) => dp.journey_time,
        }
    }

    fn next_nodes(&self) -> HashMap<String, usize> {
        match self {
            Path::Hyperpath(hp) => hp.next_nodes(),
            Path::Walkingpath(wp) => {
                match wp.next_node_id {
                    Some(nnid) => {
                        let key = String::from(WALK_KEY);
                        let mut nn = HashMap::new();
                        nn.insert(key.clone(), nnid);
                        nn                            
                    }
                    None => HashMap::new(),
                }
            },
            Path::Directpath(dp) => {
                match dp.next_node_id {
                    Some(nnid) => {
                        let mut nn = HashMap::new();
                        nn.insert(dp.route_id.clone(), nnid);
                        nn                            
                    }
                    None => HashMap::new(),
                }
            },
        }
    }

    fn route_probabilities(&self) -> HashMap<String, f64> {
        match self {
            Path::Hyperpath(hp) => hp.route_probabilities(),
            Path::Walkingpath(wp) => {
                let key = String::from(WALK_KEY);
                let mut pp = HashMap::new();
                pp.insert(key.clone(), 1.);
                pp                    
            },
            Path::Directpath(dp) => {
                let mut pp = HashMap::new();
                pp.insert(dp.route_id.clone(), 1.);
                pp
            }
        }
    }

}

struct WalkingPath {
    next_node_id: Option<usize>,
    walk_time: f64,
    postwalk_time: f64,
}

impl WalkingPath {
    fn new(next_node_id: Option<usize>, walk_time: Option<f64>, postwalk_time: Option<f64>) 
           -> WalkingPath {
        let walk_time = match walk_time {
            Some(val) => val,
            None => f64::INFINITY,
        };
        let postwalk_time = match postwalk_time {
            Some(val) => val,
            None => f64::INFINITY,
        };
        return WalkingPath{next_node_id, walk_time, postwalk_time};
    }

    fn mean_journey_time(&self) -> f64 {
        return self.walk_time + self.postwalk_time;
    }
}

struct Hyperpath {
    mean_journey_time: f64,
    effective_frequency: f64,
    candidates: HashMap<String, Candidate>,
}

impl Hyperpath {
    fn new(mean_journey_time: Option<f64>) -> Hyperpath {
        return Hyperpath {
            mean_journey_time: match mean_journey_time {
                Some(val) => val,
                None => f64::INFINITY,
            },
            effective_frequency: 0.,
            candidates: HashMap::new(),
        };
    }

    fn add(&mut self, route_id: &str, next_node_id: usize, frequency: f64, postwait_time: f64) {
        if let Some(candidate) = self.candidates.get(route_id) {
            if candidate.time_from_boarding <= postwait_time {
                // we already found a better disembark stop on this route
                return;
            } else {
                // the new stop is better, so remove the old one on this route before adding it
                self.remove(route_id);
            }
        }

        // add the route
        let new_candidate = Candidate{
            next_node_id,
            frequency,
            time_from_boarding: postwait_time,
        };
        self.candidates.insert(String::from(route_id), new_candidate);

        // update the mean journey time and effective frequency
        let mut weighted_time = 1.;
        if self.mean_journey_time < f64::INFINITY {
            weighted_time = self.mean_journey_time * self.effective_frequency;
        }
        let wieghted_new = frequency * postwait_time;
        self.effective_frequency += frequency;
        self.mean_journey_time = (weighted_time + wieghted_new) / self.effective_frequency;
    }

    fn remove(&mut self, route_id: &str) {
        let candidate = self.candidates.remove(route_id).
            expect("Tried to remove non-existent route id!");
        // update the mean journey time
        let weighted_time = self.mean_journey_time * self.effective_frequency;
        let weighted_rmv = candidate.frequency * candidate.time_from_boarding;
        let new_freq = self.effective_frequency - candidate.frequency;

        // new_freq might be zero here. That's ok: in that case, mean journey
        // time should be infinite.
        self.mean_journey_time = (weighted_time - weighted_rmv) / new_freq;
        self.effective_frequency = new_freq;
    }

    fn remove_dominated(&mut self) {
        let mut done = false;
        while !done {
            // set done to true here, but set it back to false if we update the hyperpath during
            // this iteration.
            done = true;
            let mut to_remove = vec![];
            for (route_id, candidate) in &self.candidates {
                if candidate.time_from_boarding >= self.mean_journey_time {
                    to_remove.push(route_id.clone());
                    done = false;
                }
            }
            for route_id in to_remove {
                self.remove(&route_id);
            }
        }
    }

    fn route_probabilities(&self) -> HashMap<String, f64> {
        return self.candidates.iter().map(|(route_id, candidate)|
            (route_id.clone(), candidate.frequency / self.effective_frequency)).collect();
    }

    fn next_nodes(&self) -> HashMap<String, usize> {
        return self.candidates.iter().map(|(route_id, candidate)|
            (route_id.clone(), candidate.next_node_id)).collect();
    }
}

struct DirectPath {
    journey_time: f64,
    route_id: String,
    next_node_id: Option<usize>,
}



#[derive(Clone, Debug)]
struct BHeapNode {
    node_id: usize,
    journey_time_s: f64,
}

impl BHeapNode {
    fn new(node_id: usize, journey_time_s: f64) -> BHeapNode {
        return BHeapNode{node_id, journey_time_s};
    }
}

impl Ord for BHeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse the ordering to make the binary heap where this is used a min-heap
        if self.journey_time_s < other.journey_time_s {
            return Ordering::Greater;
        } 
        else if self.journey_time_s > other.journey_time_s {
            return Ordering::Less;
        }
        else {
            return other.node_id.cmp(&self.node_id);
        }
    }
}

// Implementing Ord requires all of the below traits
impl PartialOrd for BHeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return Some(self.cmp(other));
    }
}

impl PartialEq for BHeapNode {
    fn eq(&self, other: &Self) -> bool {
        return self.cmp(other) == Ordering::Equal;
    }
}

impl Eq for BHeapNode{}


fn hyperpath_dijkstra(routing_graph: &DiGraph<Node, RoutingGraphEdge>, 
                      nodes_to_cover: Option<&HashSet<NodeIndex>>, 
                      route_frequencies: &HashMap<String, f64>, origins: Option<HashSet<NodeIndex>>, 
                      destination: NodeIndex, transfer_penalty: f64, 
                      heuristic_vec: Option<Array<f64, Ix1>>)
                      -> HashMap<usize, Path>
{
/* 
    graph: a routing graph.  Each route edge must be labelled with
        the ID of the corresponding route.
    route_frequencies: a dict from route IDs to frequencies.
    origin: the id of the start node; if None, hyperpaths from all nodes to 
        the destination will be found.
    destination: the id of the goal node.
    transfer_penalty: a cost (measured as time) that is added to each transfer

    Returns a dict: {origin_id: hyperpath} for each origin with any path to the
        destination if no origin is provided, or for each node on the hyperpath
        from the given origin to the destination (and some others) if an origin
        is given.
*/
    let mut hyperpaths = HashMap::new();
    hyperpaths.insert(destination, Hyperpath::new(Some(0.)));
    let mut walk_hyperpaths = HashMap::new();
    walk_hyperpaths.insert(destination, WalkingPath::new(Some(destination.index()), 
                                                         Some(0.), Some(0.)));

    let mut journey_time_pqueue = PriorityQueue::new();
    let nodes_to_cover: HashSet<NodeIndex> = match nodes_to_cover {
        Some(nodes_to_cover) => {
            let mut nodes_to_cover = nodes_to_cover.clone();
            // enforce that the destination node is always covered
            nodes_to_cover.insert(destination);
            nodes_to_cover
        },
        None => routing_graph.node_indices().collect()
    };
    for stop_id in &nodes_to_cover {
        let journey_time = match stop_id {
            val if *val == destination => 0.,
            _ => f64::INFINITY,
        };
        journey_time_pqueue.push(*stop_id, BHeapNode::new(stop_id.index(), journey_time));
    }

    let mut solved_set = HashSet::new();
    let mut origins = match origins {
        Some(origins) => origins,
        None => nodes_to_cover.clone(),
    };
    // 
    while let Some((cur_id, cur_node)) = journey_time_pqueue.pop() {
        solved_set.insert(cur_id);

        // check if we reached the origin; if so, we're done.
        origins.remove(&cur_id);
        if origins.len() == 0 {
            // we've covered all of the origin nodes, so we're done!
            break;
        }

        // for all incoming edges:
        for edge in routing_graph.edges_directed(cur_id, Direction::Incoming) {
            let prev_node_id = edge.source();
            if solved_set.contains(&prev_node_id) || ! nodes_to_cover.contains(&prev_node_id) {
                // we already dealt with this node, or we're not supposed to deal with it
                continue;
            }
            let edge_time_s = edge.weight().time_s;
            let route_id = &edge.weight().route_id;

            let mut postwait_time = cur_node.journey_time_s + edge_time_s;
            let hyperpath = hyperpaths.entry(prev_node_id).or_insert(Hyperpath::new(None));
            let mut walk_hp = walk_hyperpaths.entry(prev_node_id)
                .or_insert(WalkingPath::new(None, None, None));
            
            if !route_frequencies.contains_key(route_id) {
                // this is a walking link with an effectively infinite frequency.
                if postwait_time < walk_hp.mean_journey_time() {
                    walk_hp.next_node_id = Some(cur_id.index());
                    walk_hp.walk_time = edge_time_s;
                    walk_hp.postwalk_time = cur_node.journey_time_s;
                }
            } else {
                if cur_id != destination {
                    // this arc doesn't end at the destination...
                    let next_wp = walk_hyperpaths.entry(cur_id)
                        .or_insert(WalkingPath::new(None, None, None));
                    if ! (next_wp.next_node_id == Some(destination.index()) &&
                          cur_node.journey_time_s == next_wp.mean_journey_time()) {
                        // ...and we don't walk to the dest from the end.  So we must transfer!
                        postwait_time += transfer_penalty;
                    }
                }
                if postwait_time < hyperpath.mean_journey_time {
                    let freq = route_frequencies.get(route_id).unwrap();
                    hyperpath.add(route_id, cur_id.index(), *freq, postwait_time);
                    hyperpath.remove_dominated();
                }
            }

            // compute the priority score for this node, determining when it will next be visited
            // must avoid reusing the earlier mutable 'walk_hp' here
            let walk_hp = walk_hyperpaths.get(&prev_node_id).unwrap();
            let mut priority = hyperpath.mean_journey_time.min(walk_hp.mean_journey_time());
            if let Some(hv) = &heuristic_vec {
                priority += hv[prev_node_id.index()];
            }
            if ! origins.contains(&prev_node_id) && route_frequencies.contains_key(route_id) {
                // we're taking a transit link that doesn't start from a src node, so a transfer
                // must have happened before.
                priority += transfer_penalty;
            }

            let new_pq_node = BHeapNode::new(prev_node_id.index(), priority);
            // update the priority if it's in the queue, or insert it if not
            if let None = journey_time_pqueue.change_priority(&prev_node_id, new_pq_node.clone()) {
                journey_time_pqueue.push(prev_node_id, new_pq_node);
            }
        }
    }

    // integrate infinite-frequency hyperpaths
    let mut paths = HashMap::new();
    for (stop_id, hyperpath) in hyperpaths {
        match walk_hyperpaths.remove(&stop_id) {
            Some(walk_hp) => match walk_hp.next_node_id {
                Some(_) if walk_hp.walk_time < hyperpath.mean_journey_time => 
                    paths.insert(stop_id.index(), Path::Walkingpath(walk_hp)),
                _ => paths.insert(stop_id.index(), Path::Hyperpath(hyperpath)),
            }
            None => None,
        };
    }
    return paths;
}

#[derive(Clone)]
struct RouteLoad<'a> {
    route: &'a Vec<usize>,
    capacity: f64,
    demands: Array<f64, Ix2>,
    closest_idxs: HashMap<usize, HashMap<usize, (usize, usize)>>,
    is_loop: bool,
}

impl<'a> RouteLoad<'a> {
    fn new(route: &Vec<usize>, capacity: f64) -> RouteLoad {
        let is_loop = route[0] == *route.last().
            expect("Tried to build RouteLoad from empty route!");
        let mut load_len = route.len();
        if is_loop {
            load_len -= 1;
        }

        let demands = Array::zeros((load_len, load_len));     
        let mut closest_idxs = HashMap::new();
        for (ii, inode) in route.iter().enumerate() {
            if closest_idxs.contains_key(inode) {
                continue;
            }
            let mut i_closest_idxs = HashMap::new();
            let mut count_from_idx = ii;
            let mut jj = ii + 1;
            if is_loop {
                jj = jj % load_len;
            } 

            while (is_loop && jj != ii) || (!is_loop && jj < load_len) {
                let jnode = route[jj];
                if jnode == *inode {
                    // this is a second visit to the same node
                    count_from_idx = jj;
                }

                // compute the distance from ii to jj
                let dist = RouteLoad::num_stops_until(load_len, count_from_idx, jj);
                let insert_jnode = match i_closest_idxs.get(&jnode) {
                    Some((from, to)) => {
                        let best_dist = RouteLoad::num_stops_until(load_len, *from, *to);
                        dist < best_dist
                    }
                    None => true,
                };
                if insert_jnode {
                    i_closest_idxs.insert(jnode, (count_from_idx, jj));
                }
                jj += 1;
                if is_loop {
                    jj = jj % load_len;
                }
            }

            closest_idxs.insert(*inode, i_closest_idxs);
        }

        return RouteLoad{
            route,
            capacity,
            demands,
            closest_idxs,
            is_loop,
        };
    }

    fn add_load(&mut self, board_node: usize, dmbrk_node: usize, new_load: f64, dryrun: bool)
                -> f64 {
        /*
         We limit the load that can board by the available room at the highest-
        load point along its section of the route, not at the point where it
        gets on.  The latter would be more realistic (when you get on a bus, 
        you don't look forward in time to see how full it will be, you just
        see if it is full now, and get on if not).  But this would require
        checking whether the added load increased the route's load to above
        capacity on some downstream segment, and if it did, we'd have to reduce
        all loads that got on at that segment; this would require backtracking
        to adjust all the demands that contributed to it, which would become
        computationally very costly, and difficult to implement
        */
        let mut loads = float_cumsum(&(self.board_counts() - self.disembark_counts()));
        loads -= loads.iter().cloned().fold(f64::INFINITY, f64::min);
        let (board_idx, dmbrk_idx) = self.closest_idxs.get(&board_node).unwrap().get(&dmbrk_node).
            unwrap();
        // "roll" the loads so board load is at the start, to ease indexing
        let mut rolled_loads = Array::zeros(loads.len());
        let shift = self.route_len() - board_idx;
        let mut second_part = rolled_loads.slice_mut(s!(shift..));
        second_part.assign(&loads.slice(s!(..loads.len() - shift)));
        let mut first_part = rolled_loads.slice_mut(s!(..shift));
        first_part.assign(&loads.slice(s!(loads.len() - shift..)));

        let rolled_dmbrk_idx = RouteLoad::num_stops_until(self.route_len(), *board_idx, *dmbrk_idx);
        let post_dmbrk: Vec<f64> = rolled_loads.slice(s!(..rolled_dmbrk_idx)).iter().
            map(|ff| *ff).collect();
        let max_occupancy = post_dmbrk.iter().cloned().fold(-f64::INFINITY, f64::max);
        let room = self.capacity - max_occupancy;
        
        let mut load_to_add = new_load;
        if room < load_to_add {
            // limit the added load to our actual capacity
            load_to_add = room;
        }
        if ! dryrun {
            // add the load to the satisfied demand
            self.demands[[*board_idx, *dmbrk_idx]] += load_to_add;
        }

        return load_to_add;
    }

    fn route_len(&self) -> usize {
        if self.is_loop {
            return self.route.len() - 1;
        } else {
            return self.route.len();
        }
    }

    fn board_counts(&self) -> Array<f64, Ix1> {
        return self.demands.sum_axis(Axis(1));
    }

    fn disembark_counts(&self) -> Array<f64, Ix1> {
        return self.demands.sum_axis(Axis(0));
    }

    fn num_stops_until(route_len: usize, from_idx: usize, to_idx: usize) -> usize {
        if to_idx >= from_idx {
            return to_idx - from_idx;
        } else {
            return (route_len - from_idx) + to_idx;
        }
    }
}


#[derive(Clone)]
struct StaticSimConfig {
    basin_radius_m: f64,
    transfer_radius_m: f64,
    transfer_penalty_m: f64,
    beeline_dist_factor: f64,
    walk_speed_mps: f64,
    mean_intersection_time_s: f64,
    mean_stop_time_s: f64,
    adjacency_threshold_s: f64,
    penalize_indirection: bool,
    ignore_small_components: bool,
}

impl StaticSimConfig {
    fn from_yaml(yaml_cfg: &Yaml) -> StaticSimConfig {
        return StaticSimConfig {
            basin_radius_m: yaml_cfg["basin_radius_m"].as_i64().expect("no basin radius") as f64,
            transfer_radius_m: yaml_cfg["transfer_radius_m"].as_i64().
                expect("no transfer radius") as f64,
            transfer_penalty_m: yaml_cfg["transfer_penalty_m"].as_i64().
                expect("no transfer penalty") as f64,
            beeline_dist_factor: yaml_cfg["beeline_dist_factor"].as_f64().
                expect("no beeline dist factor") as f64,
            walk_speed_mps: yaml_cfg["walk_speed_mps"].as_f64().expect("no walk speed"),
            mean_intersection_time_s: yaml_cfg["mean_intersection_time_s"].as_i64().
                expect("no intersection time") as f64,
            mean_stop_time_s: yaml_cfg["mean_stop_time_s"].as_i64().expect("no stop time") as f64,
            adjacency_threshold_s: yaml_cfg["adjacency_threshold_s"].as_i64().
                expect("adjacency threshold") as f64,
            penalize_indirection: yaml_cfg["penalize_indirection"].as_bool().
                expect("no penalize indirection"),
            ignore_small_components: yaml_cfg["ignore_small_components"].as_bool().
                expect("no ignore small components"),
        };
    }
}

impl SimConfig for StaticSimConfig {
    fn get_basin_radius_m(&self) -> f64 {
        return self.basin_radius_m;
    }
    
    fn get_transfer_time_s(&self) -> u32 {
        return (self.transfer_penalty_m as f64 / self.walk_speed_mps) as u32;
    }

    fn get_transfer_radius_m(&self) -> f64 {
        return self.transfer_radius_m;
    }

    fn get_beeline_dist_factor(&self) -> f64 {
        return self.beeline_dist_factor;
    }

    fn get_walk_speed_mps(&self) -> f64 {
        return self.walk_speed_mps;
    }

    fn get_mean_intersection_time_s(&self) -> u32 {
        return self.mean_intersection_time_s as u32;
    }

    fn get_ignore_small_components(&self) -> bool {
        return self.ignore_small_components;
    }
}

#[derive(Clone, Debug)]
pub struct Node;


pub struct SimResults {
    pub satisfied_demand: f64,
    pub power_used_kW: f64,
    pub total_saved_time: f64,
    pub per_stop_satisfied_demand: Vec<Array<f64, Ix1>>,
    pub stop_n_boarders: Vec<Array<f64, Ix1>>,
    pub stop_n_disembarkers: Vec<Array<f64, Ix1>>,
    pub per_stop_power_used_kW: Vec<Array<f64, Ix1>>,
}

type BasinConnections = HashMap<NodeIndex, HashMap<NodeIndex, f64>>;

pub struct StaticSimulator {
    path_network: PathNetwork<'static, StaticSimConfig>,
    demand_graph: DiGraph<Point2d, f64>,
    // vehicle_type: PtVehicleType,
    // some precomputed values
    transfer_graph: DiGraph<Node, RoutingGraphEdge>,
    basin_connections: BasinConnections,
    cfg: &'static StaticSimConfig,
    rng: Isaac64Rng,
}

impl StaticSimulator {
    pub fn from_cfg(config_path_str: &str) -> StaticSimulator {
        let mut config_path = PathBuf::new();
        config_path.push(config_path_str);
        let file_contents = std::fs::read_to_string(config_path.clone()).
            expect("Failed to read simulator config file!");
        let yaml_cfg = YamlLoader::load_from_str(&file_contents).
            expect("Failed to parse sim config as yaml!");
        let yaml_cfg = &yaml_cfg[0];
        let cfg = Box::new(StaticSimConfig::from_yaml(&yaml_cfg));
        let cfg: &'static mut StaticSimConfig = Box::leak(cfg);
        let mut rng = Isaac64Rng::seed_from_u64(RAND_SEED);
        // TODO handle xml paths
        let (path_network, demand_graph) = if ! yaml_cfg["grid_city"].is_badvalue() {
            let grid_cfg = &yaml_cfg["grid_city"];
            let xnodes = grid_cfg["num_x_nodes"].as_i64().expect("no x nodes") as usize;
            let ynodes = grid_cfg["num_y_nodes"].as_i64().expect("no y nodes") as usize;
            let city_size = grid_cfg["city_size"].as_i64().expect("no city size") as f64;
            let num_travellers = grid_cfg["num_travellers"].as_i64().
                expect("no num travellers") as usize;
            let num_demand_loci = grid_cfg["num_demand_loci"].as_i64().
                expect("no num demand loci") as usize;
            let num_demand_edges = grid_cfg["num_demand_edges"].as_i64().
                expect("no num demand edges") as usize;
            let drive_speed_mps = grid_cfg["drive_speed_mps"].as_f64().
                expect("no drive speed mps");
            generate_grid_city(xnodes, ynodes, city_size, num_travellers, num_demand_loci, 
                               num_demand_edges, drive_speed_mps, cfg, &mut rng)
        } else if ! yaml_cfg["dataset"].is_badvalue() {
            let dataset_cfg = &yaml_cfg["dataset"];
            let dir = config_path.as_path().parent().unwrap();
            let path = dataset_cfg["network_path"].as_str().unwrap();
            let path = config_utils::str_to_absolute_path(path, dir);
            let network = PathNetwork::from_xml(path.as_path(), cfg).
                                                expect("Failed to parse network xml!");

            let path = dataset_cfg["od_path"].as_str().unwrap();
            let path = config_utils::str_to_absolute_path(path, dir);
            let time_range_start = match dataset_cfg["time_range_start"].is_badvalue() {
                // TODO parse this appropriately
                false => {
                    let time_str = dataset_cfg["time_range_start"].as_str().unwrap();
                    Some(config_utils::get_num_seconds_from_time_str(time_str))
                }
                true => None,
            };
            let time_range_end = match dataset_cfg["time_range_end"].is_badvalue() {
                false => {
                    let time_str = dataset_cfg["time_range_end"].as_str().unwrap();
                    Some(config_utils::get_num_seconds_from_time_str(time_str))
                }
                true => None,
            };

            let trips = PassengerTrip::all_from_csv(path.as_path(), false, time_range_start, 
                                                    time_range_end).unwrap();
            let demand_graph = od_to_demand_graph(trips);
            (network, demand_graph)
        } else {
            panic!("No valid city given in config!");
        };
        
        // build transfer graph
        let transfer_graph = build_transfer_graph(&path_network);

        // build the basin graph
        let basin_connections = build_basin_conns(&path_network, &demand_graph, &cfg);

        return StaticSimulator {
            path_network,
            demand_graph,
            transfer_graph,
            basin_connections,
            cfg,
            rng,
        };
    }


    pub fn run(&mut self, routes: &Vec<Vec<usize>>, 
               route_frequencies_Hz: &Vec<f64>,
               route_vehicles: &Vec<PtVehicleType>,
               fast_demand_assignment: bool)
               -> SimResults {
        log::debug!("routes: {:?}", routes);
        let mut routing_graph = self.transfer_graph.clone();

        // filter out nodes in routing graph that aren't on any routes
        let mut used_stops = HashSet::new();
        let mut visit_counts = vec![0; routing_graph.node_count()];
        for route in routes {
            for stop_idx in route {
                used_stops.insert(*stop_idx);
            }
            let route_set: HashSet<usize> = route.iter().map(|ss| *ss).collect();
            for stop in route_set {
                visit_counts[stop] += 1;
            }
        }

        let route_names: Vec<String> = routes.iter().enumerate().map(|(ii, _)| format!("{}", ii)).
            collect();

        // add edges for the routes to the routing graph
        let mut leg_times = vec![];
        log::debug!("build routing graph");
        for (ri, route) in routes.iter().enumerate(){
            // compute the cost of each leg on the route
            let route_leg_times = self.get_route_leg_times(&route, &route_vehicles[ri].mode);

            // compute costs from each node on the route to each other node
            let mut treated_nodes = HashSet::new();
            let mut costs = HashMap::new();
            let is_loop = route[0] == *route.last().unwrap();
            for (ii, inode) in route.iter().enumerate() {
                if treated_nodes.contains(inode) {
                    // we already dealt with this node, so skip this iteration
                    continue
                } else {
                    // we're treating the node now, so mark it treated
                    treated_nodes.insert(*inode);
                }

                // starting from a's first appearance:
                let icosts = costs.entry(inode).or_insert(HashMap::new());
                let mut jj = ii + 1;
                if is_loop {
                    // loop around at the end of the route
                    jj = jj % (route.len() - 1);
                }

                let mut cumulative_cost = 0.;
                while (is_loop && jj != ii) || (!is_loop && jj < route.len()) {
                    // j <- next node on route
                    let jnode = route[jj];
                    if jnode == *inode {
                        // this is a second visit to i, so reset the cost
                        cumulative_cost = 0.;
                    } else {
                        // add the cost of this leg
                        cumulative_cost += if jj == 0 {
                            *route_leg_times.last().unwrap()
                        } else {
                            route_leg_times[jj - 1]
                        };
                    }

                    let best_cost_ij_sofar = match icosts.get(&jnode) {
                        Some(cost) => *cost,
                        None => f64::INFINITY,
                    };
                    if cumulative_cost < best_cost_ij_sofar {
                        // this is a lower cost from i to j, so record it
                        icosts.insert(jnode, cumulative_cost);
                    }

                    jj += 1;
                    if is_loop {
                        jj = jj % (route.len() - 1);
                    }
                }
            }
            leg_times.push(route_leg_times);

            // // all costs are computed, so use them as edge weights
            for pair in treated_nodes.iter().permutations(2) {
                let ni = NodeIndex::from(*pair[0] as u32);
                let nj = NodeIndex::from(*pair[1] as u32);
                if let Some(first_elem_costs) = costs.get(pair[0]) {
                    if let Some(cost) = first_elem_costs.get(pair[1]) {
                        let edge = RoutingGraphEdge::new(&route_names[ri], *cost);
                        routing_graph.add_edge(ni, nj, edge);        
                    }
                }
            }
        }

        log::debug!("compute route powers and set up route loads");
        // compute penalties of routes and initialize route load trackers
        let mut route_loads = HashMap::new();
        let mut route_rewards = HashMap::new();
        let mut route_powers = vec![];
        for ri in 0..routes.len() {
            let freq = route_frequencies_Hz[ri];
            let veh_type = &route_vehicles[ri];
            let hourly_cpcty = veh_type.get_capacity() as f64 * 3600. * freq;
            let load = RouteLoad::new(&routes[ri], hourly_cpcty);
            route_loads.insert(route_names[ri].clone(), load.clone());
            route_rewards.insert(route_names[ri].clone(), load);
            let n_vehicles = freq * leg_times[ri].sum();
            route_powers.push(n_vehicles * veh_type.avg_power_kW);
        }
        log::debug!("build freq dict");
        // for each demand edge, try to assign it to a route.
        let route_freq_dict: HashMap<String, f64> = 
            route_frequencies_Hz.iter().enumerate()
            .map(|(ii, ff)| (route_names[ii].clone(), *ff)).collect();


        log::debug!("compute hyperpaths");
        let mut dests = HashSet::new();
        for (_, dst_conns) in &self.basin_connections {
            for (dst_stop, _) in dst_conns {
                dests.insert(*dst_stop);
            }
        }

        // iterate over destinations in parallel and compute hyperpaths!
        let dests_and_seeds: Vec<(NodeIndex, u64)> = dests.into_iter().map(
                |dst| (dst, self.rng.gen::<u64>())
            ).collect();
        let hyperpaths: HashMap<usize, HashMap<usize, Path>> = dests_and_seeds.par_iter().map(
            |(dst, seed)| {

            if fast_demand_assignment {
                // seed them from numbers generated from the top-level rng, for reproducibility.
                let mut rng = Isaac64Rng::seed_from_u64(*seed);
                let edge_cost = |edge_ref: EdgeReference<RoutingGraphEdge>| {
                    let ww = edge_ref.weight();
                    let mut cost = ww.time_s;
                    if ww.route_id != WALK_KEY && edge_ref.target() != *dst {
                        // this is a bus route, but not the last one, so add the transfer cost
                        cost += self.cfg.get_transfer_time_s() as f64;
                    }
                    cost += match route_freq_dict.get(&ww.route_id) {
                        Some(freq) => rng.gen::<f64>() / freq,
                        None => 0.,
                    };
                    return cost;
                };

                let (costs, path_edges) = bkwds_dijkstra_with_paths(&routing_graph, *dst, 
                                                                    None, None, edge_cost);
                let mut paths = HashMap::new();
                for (src, edge) in path_edges {
                    let cost = *costs.get(&src).unwrap();
                    let route_id = &edge.weight().route_id;
                    let target = Some(edge.target().index());
                    if route_freq_dict.contains_key(route_id) {
                        let path = DirectPath {
                            journey_time: cost, 
                            next_node_id: target, 
                            route_id: route_id.clone(),
                        };
                        paths.insert(src.index(), Path::Directpath(path));
                    } else {
                        let path = WalkingPath::new(target, Some(0.), Some(cost));
                        paths.insert(src.index(), Path::Walkingpath(path));
                    }
                }
                // add a dummy path for the destination node
                paths.insert(dst.index(), 
                    Path::Walkingpath(WalkingPath::new(None, None, None)
                ));
                return (dst.index(), paths);                
            } else {
                return (dst.index(),
                        hyperpath_dijkstra(&routing_graph, None, &route_freq_dict,
                                           None, *dst, self.cfg.get_transfer_time_s() as f64,
                                           None));
            }

        }).collect();
        log::debug!("allocate demand");
        // allocate demand
        let mut total_satisfied_demand = 0.;
        let mut edges: Vec<EdgeReference<f64>> = self.demand_graph.edge_references().collect();
        let mut total_saved_time = 0.;
        edges.shuffle(&mut self.rng);
        for edge_ref in edges {
            let src = edge_ref.source();
            let dst = edge_ref.target();
            let mut fit_demand = *edge_ref.weight();
            if let Some((first_stop, last_stop, transit_cost)) = 
                self.get_best_board_dmbrk(src, dst, &hyperpaths) {
                // a valid path by transit exists!  Allocate the demand to transit.
                if self.cfg.penalize_indirection {
                    // assume people choose to take transit based on how direct it is.
                    // possible optimization: pre-compute these in the constructor
                    let car_cost = self.get_best_car_time(src, dst);
                    fit_demand *= car_cost / transit_cost;
                }
                let src_pos = self.demand_graph.node_weight(src).expect("no src node!");
                let dst_pos = self.demand_graph.node_weight(dst).expect("no dst node!");
                // possible optimization: pre-compute these in the constructor
                let walk_time = src_pos.euclidean_distance(&dst_pos);
                if transit_cost < walk_time {
                    // split this demand among the relevant route segments
                    let dst_hyperpaths = hyperpaths.get(&last_stop).unwrap();
                    let allocated = allocate_demand_rewards(fit_demand, dst_hyperpaths, 
                        first_stop, &mut route_loads, &mut route_rewards);
                    total_satisfied_demand += allocated;
                    // TODO incorporate walking weight
                    total_saved_time += walk_time - transit_cost;
                }
            }
        }

        log::debug!("compute passengers served");
        let per_stop_satisfied_demand: Vec<Array<f64, Ix1>> = route_names.iter().map(|rn| {
                let rl = route_rewards.get(rn).expect("no route load!");
                return (rl.board_counts() + rl.disembark_counts()) / 2.;
            }).collect();
        let stop_n_boarders: Vec<Array<f64, Ix1>> = route_names.iter().map(|rn| {
                let rl = route_loads.get(rn).expect("no route load!");
                return rl.board_counts();
            }).collect();
        let stop_n_disembarkers: Vec<Array<f64, Ix1>> = route_names.iter().map(|rn| {
                let rl = route_loads.get(rn).expect("no route load!");
                return rl.disembark_counts();
            }).collect();

        log::debug!("compute power cost of each stop");
        let mut per_stop_powers = vec![];
        for ri in 0..routes.len() {
            /* for intermediate stops, fraction of cost is cost due to deviation from shortest path
               between surrounding stops.  For end stops, cost is half of the remaining cost. */
            let route = &routes[ri];
            let vehicle_type = &route_vehicles[ri];

            let route_len = if route[0] == *route.last().expect("Route is empty!") {
                route.len() - 1
            } else {
                route.len()
            };
            let mut stop_times = Array::zeros(route_len);
            for si in 1..route_len - 1 {
                let time_without = self.path_network.get_travel_time(route[si-1], route[si+1], 
                                                                     &vehicle_type.mode);
                let time_bfr = self.path_network.get_travel_time(route[si-1], route[si], 
                                                                 &vehicle_type.mode);
                let time_aft = self.path_network.get_travel_time(route[si], route[si+1], 
                                                                 &vehicle_type.mode);
                let time_with = time_bfr + time_aft + self.cfg.mean_stop_time_s;
                stop_times[si] = time_with - time_without;
            }
            let total_time = leg_times[ri].sum();
            let remaining = total_time - stop_times.sum();
            stop_times[0] = remaining / 2.;
            stop_times[route_len - 1] = remaining / 2.;            
            let stop_fractions = stop_times / total_time;
            let stop_power_costs = stop_fractions * route_powers[ri];
            per_stop_powers.push(stop_power_costs);
        }

        log::debug!("return results");
        let total_power = route_powers.iter().sum();

        return SimResults {
            satisfied_demand: total_satisfied_demand,
            power_used_kW: total_power,
            total_saved_time,
            per_stop_satisfied_demand,
            stop_n_boarders,
            stop_n_disembarkers,
            per_stop_power_used_kW: per_stop_powers,
        };
    }

    pub fn get_route_leg_times(&self, route: &Vec<usize>, mode: &str) -> Array<f64, Ix1> {
        // compute the cost of each leg on the route
        let mut route_leg_times = Array::zeros(route.len() - 1);
        for ii in 1..route.len() {
            let pi = route[ii - 1];
            let ci = route[ii];
            let i_leg_cost = self.path_network.get_travel_time(pi, ci, mode);
            // if this happens, the route goes between disconnected segments of the path 
            // network!  That's not allowed!  (probably there shouldn't be disconnected
            // segments at all...)
            assert!(i_leg_cost != f64::INFINITY);
            route_leg_times[ii - 1] = i_leg_cost;
        }
        // add the stop time to each leg cost
        route_leg_times += self.cfg.mean_stop_time_s;
        return route_leg_times;
    }

    fn get_best_board_dmbrk(&self, src: NodeIndex, dst: NodeIndex, 
                            hyperpaths: &HashMap<usize, HashMap<usize, Path>>)
                            -> Option<(usize, usize, f64)> 
    {
        let src_conns = match self.basin_connections.get(&src) {
            Some(conns) => conns,
            None => return None,
        };
        let dst_conns = match self.basin_connections.get(&dst) {
            Some(conns) => conns,
            None => return None,
        };

        let mut best_conn = None;
        let mut best_time = f64::INFINITY;
        for (src_stop, src_walk_time) in src_conns {
            for (dst_stop, dst_walk_time) in dst_conns {
                let path_time = match hyperpaths.get(&dst_stop.index()) {
                    Some(dst_hyperpaths) => match dst_hyperpaths.get(&src_stop.index()) {
                        Some(path) => match path {
                            // if the best path from the first stop starts by walking, 
                            // the whole thing must be a walk; that's not a valid path.
                            Path::Walkingpath(_) => f64::INFINITY,
                            _ => path.mean_journey_time(),
                        }
                        None => f64::INFINITY,
                    },
                    None => f64::INFINITY,
                };
                let total_time = src_walk_time + dst_walk_time + path_time;
                if total_time < best_time {
                    best_time = total_time;
                    best_conn = Some((src_stop, dst_stop));
                }
            }
        }

        return match best_conn {
            Some((src_stop, dst_stop)) => Some((src_stop.index(), dst_stop.index(), best_time)),
            None => None,
        };
    }

    fn get_best_car_time(&self, src_ni: NodeIndex, dst_ni: NodeIndex) -> f64 {
        // assume the basin radius also applies to how far people will walk to and from their car
        let mut times = vec![];
        if let Some(src_conns) = self.basin_connections.get(&src_ni) {
            for (near_src_node, nsd) in src_conns {
                let src_walk_time = self.cfg.dist_to_walk_time(*nsd);
                if let Some(dst_conns) = self.basin_connections.get(&dst_ni) {
                    for (near_dst_node, ndd) in dst_conns {
                        let in_car_time = self.path_network.get_travel_time(
                            near_src_node.index(), near_dst_node.index(), "road");
                        let dst_walk_time = self.cfg.dist_to_walk_time(*ndd);
                        times.push(src_walk_time + dst_walk_time + in_car_time);
                    }    
                }
            }
        }

        return match times.iter().min_by_key(|tt| **tt as u32) {
            Some(val) => *val as f64,
            // no path exists, but this will probably never happen
            None => f64::INFINITY,
        };
    }

    pub fn get_node_idx_by_id(&self, node_id: &str) -> Option<usize> {
        return self.path_network.get_node_idx_by_id(node_id);
    }

    pub fn get_node_id_by_idx(&self, node_idx: usize) -> Option<&str> {
        return self.path_network.get_node_id_by_idx(node_idx);
    }

    pub fn get_stop_node_positions(&self) -> &Vec<Point2d> {
        return self.path_network.get_node_positions();
    }

    pub fn get_travel_times_matrix(&self, mode: &str) -> &Array<f64, Ix2> {
        return self.path_network.get_travel_times_matrix(mode);
    }

    pub fn get_drive_dists_matrix(&self, mode: &str) -> &Array<f64, Ix2> {
        return self.path_network.get_drive_dists_matrix(mode);
    }

    pub fn get_demand_edges(&self) -> Vec<(usize, usize, f64)> {
        // this is needed by the python interface.
        let mut edges = vec![];
        for edge_ref in self.demand_graph.edge_references() {
            edges.push((edge_ref.source().index(), edge_ref.target().index(), *edge_ref.weight()));
        }
        return edges;        
    }

    pub fn get_demand_graph(&self) -> &DiGraph<Point2d, f64> {
        return &self.demand_graph;
    }

    pub fn get_street_edges(&self) -> Vec<(usize, usize)> {
        return self.path_network.get_adjacencies();
    }

    pub fn get_basin_connections(&self) -> &BasinConnections {
        return &self.basin_connections;
    }
}


fn build_transfer_graph(path_network: &PathNetwork<StaticSimConfig>) 
                        -> DiGraph<Node, RoutingGraphEdge> {
    let mut transfer_graph = DiGraph::new();
    for _ in 0..path_network.get_num_nodes() {
        transfer_graph.add_node(Node);
    }

    for ii in transfer_graph.node_indices() {
        for jj in transfer_graph.node_indices() {
            if ii == jj {
                continue;
            }
            if path_network.can_transfer(ii.index(), jj.index()){
                let time = path_network.get_walk_time(ii.index(), jj.index());
                let edge = RoutingGraphEdge::new("transfer", time);
                transfer_graph.add_edge(NodeIndex::from(ii), NodeIndex::from(jj), edge);
            }
        }
    }

    return transfer_graph;
}


fn build_basin_conns(path_network: &PathNetwork<StaticSimConfig>, 
                     demand_graph: &DiGraph<Point2d, f64>, cfg: &StaticSimConfig)
                     -> BasinConnections {
    // build the basin graph
    // compute distances between each point in the demand graph and the path network
    let stop_locs = path_network.get_node_positions();
    let demand_locs = demand_graph.node_weights().map(|pp| pp.clone()).collect();
    let demand_to_stop_nodes_dists = euclidean_distances(&stop_locs, Some(&demand_locs));
    // for each distance that is <= the basin radius, add an edge with the walk time
    let mut basin_conns = HashMap::new();
    for ((si, di), dist) in demand_to_stop_nodes_dists.indexed_iter() {
        let sni = NodeIndex::new(si);
        let dni = NodeIndex::new(di);
        if *dist <= cfg.basin_radius_m {
            // edges point *from* demand nodes *to* stop nodes.
            let dconns = basin_conns.entry(dni).or_insert(HashMap::new());
            dconns.insert(sni, cfg.dist_to_walk_time(*dist));
        }
    }
    return basin_conns;
}

fn generate_grid_city<'a, RR>(num_x_nodes: usize, num_y_nodes: usize, city_size: f64, 
                              num_travellers: usize, num_demand_loci: usize, 
                              num_demand_edges: usize, drive_speed_mps: f64,
                              cfg: &'a StaticSimConfig, rng: &mut RR)
                      -> (PathNetwork<'a, StaticSimConfig>, DiGraph<Point2d, f64>) 
                      where RR: Rng {
    // num_x_nodes, num_y_nodes: the number of "intersections" in each direction
    //     in the city
    // city_size: the side length of the square city (in meters)
    // num_travellers: the total transit demand in the city
    
    // generate a street network
    // generate the nodes at grid points
    let inter_x_dist = city_size / num_x_nodes as f64;
    let inter_y_dist = city_size / num_y_nodes as f64;

    let get_id_and_pos = |x_idx, y_idx| {
        let pos = Point2d::new(inter_x_dist * x_idx as f64, inter_y_dist * y_idx as f64);
        let id = y_idx * num_x_nodes + x_idx;
        return (id, pos);
    };

    let mut street_graph = DiGraphMap::new();
    let mut node_positions = vec![];
    for (y_idx, x_idx) in iproduct!(0..num_y_nodes, 0..num_x_nodes) {
        let (id, pos) = get_id_and_pos(x_idx, y_idx);
        street_graph.add_node(id);
        node_positions.push(pos);
    }

    // now that all nodes are added, iterate over them again and add edges
    let modes = vec![String::from("road")];
    for (y_idx, x_idx) in iproduct!(0..num_y_nodes, 0..num_x_nodes) {
        let (this_id, _) = get_id_and_pos(x_idx, y_idx);
        let mut others = vec![];
        if 0 < x_idx {
            let (other_id, _) = get_id_and_pos(x_idx-1, y_idx);
            others.push((other_id, inter_x_dist));
        }
        if x_idx < num_x_nodes - 1 {
            let (other_id, _) = get_id_and_pos(x_idx+1, y_idx);
            others.push((other_id, inter_x_dist));
        }
        if 0 < y_idx {
            let (other_id, _) = get_id_and_pos(x_idx, y_idx-1);
            others.push((other_id, inter_y_dist));
        }
        if y_idx < num_y_nodes - 1 {
            let (other_id, _) = get_id_and_pos(x_idx, y_idx+1);
            others.push((other_id, inter_y_dist));
        }

        for (other_id, dist) in others {
            let id = String::from(format!("{}-{}", this_id, other_id));
            let seg = PathSegment::new(id, modes.clone(), 1, 1000, drive_speed_mps, dist);
            street_graph.add_edge(this_id, other_id, seg);
        }
    }

    // now build the demands!
    // randomly pick nodes as demand loci.
    let num_nodes = num_y_nodes * num_x_nodes;
    let mut nodes: Vec<usize> = (0..num_nodes).collect();
    nodes.shuffle(rng);
    // let num_demand_loci = street_graph.node_count();
    let demand_loci_stops = &nodes[0..num_demand_loci];
    let mut demand_graph = DiGraph::new();

    // for each stop near a locus:
    let mut demand_poss = vec![];
    for stop_locus_idx in demand_loci_stops {
        // apply a random transform to its x, y coordinates
        // take the min of two random dists to bias it towards being close to the stop node
        let dist = rng.gen::<f64>().min(rng.gen()) * cfg.basin_radius_m;
        let angle = rng.gen::<f64>() * 2. * std::f64::consts::PI;
        // add the transformed point to the graph's nodes
        let delta = Point2d::new(dist * angle.cos(), dist * angle.sin());
        let new_point = node_positions[*stop_locus_idx].plus(&delta);
        demand_poss.push(new_point);
    }

    let path_network = PathNetwork::from_graph(street_graph, node_positions, cfg);
    
    // add demand between half of the loci pairs.
    let mut loci_pairs: Vec<Vec<usize>> = (0..(demand_poss.len())).
        permutations(2).collect();
    let num_demand_edges = num_demand_edges.min(loci_pairs.len());
    let rands: Vec<f64> = (0..num_demand_edges).map(|_| rng.gen()).collect();
    let total: f64 = rands.iter().sum();
    // scale the randomly generated numbers to get demands between each loci
    let demands = rands.iter().map(|rr| rr * num_travellers as f64 / total);
    loci_pairs.shuffle(rng);
    let mut poss_set = HashSet::new();
    for point_pair in &loci_pairs {
        for point_idx in point_pair {
            poss_set.insert(point_idx);
        }
    }

    // add every node in poss_set to the demand graph
    // store its node index in a mapping from indexes in demand_poss to node indexes
    let mut vec_idxs_to_node_idxs = HashMap::new();
    for point_idx in poss_set {
        let node_idx = demand_graph.add_node(demand_poss[*point_idx].clone());
        vec_idxs_to_node_idxs.insert(*point_idx, node_idx);
    }

    // for each demand, sample a loci pair without replacement
    for demand in demands {
        let loci_pair = loci_pairs.pop().unwrap();
        let from_node_idx = vec_idxs_to_node_idxs[&loci_pair[0]];
        let to_node_idx = vec_idxs_to_node_idxs[&loci_pair[1]];
        demand_graph.add_edge(from_node_idx, to_node_idx, demand);
    }

    // TODO implement spreading the demand around stochastically
    return (path_network, demand_graph);
}


fn od_to_demand_graph<'a>(trips: Vec<PassengerTrip>) -> DiGraph<Point2d, f64> {
    // add a node for each unique source and dest in the trips
    let mut demand_nodes = HashMap::new();
    for trip in &trips {
        for point in &[&trip.origin, &trip.destination] {
            // point is an && by default, so dereference it once for convenience
            let point = *point;
            // we don't care about sub-meter precision here, and we need a hashable version of the
            // point, so we cast its components to integers.
            let hashable_point = point2d_to_ints(point);
            if ! demand_nodes.contains_key(&hashable_point) {
                demand_nodes.insert(hashable_point, point.clone());
            }
        }
    }
    let mut demand_graph = DiGraph::new();
    let mut nodeidxs_by_pos = HashMap::new();
    for (int_point, point) in demand_nodes.into_iter() {
        let nodeidx = demand_graph.add_node(point);
        nodeidxs_by_pos.insert(int_point, nodeidx);
    }

    let mut edges = HashMap::new();
    for trip in trips {
        // iterate over trips and add associated demand as edges to the demand graph
        // find nodes corresponding to end-points
        let orig_nodeidx = nodeidxs_by_pos.get(&point2d_to_ints(&trip.origin)).unwrap();
        let dest_nodeidx = nodeidxs_by_pos.get(&point2d_to_ints(&trip.destination)).unwrap();
        *edges.entry((*orig_nodeidx, *dest_nodeidx)).or_insert(0.) += trip.expansion_factor;
    }

    for ((oo, dd), ef) in edges {
        demand_graph.add_edge(oo, dd, ef);
    }

    return demand_graph;
}


fn point2d_to_ints(point: &Point2d) -> (i64, i64) {
    return (point.x_coord as i64, point.y_coord as i64);
}


fn allocate_demand_rewards(demand: f64, dst_hyperpaths: &HashMap<usize, Path>, src: usize,
                           route_segment_loads: &mut HashMap<String, RouteLoad>,
                           route_segment_rwds: &mut HashMap<String, RouteLoad>) -> f64 {
    let mut paths_and_amts = vec![];                               
    let satisfied_demand = allocate_demand_helper(demand, dst_hyperpaths, src,
                                                  route_segment_loads, vec![], 
                                                  &mut paths_and_amts);
        
    // allocate route segment rewards, which spread the demand out over each transfer
    for (path, demand_on_path) in paths_and_amts {
        let demand_reward = demand_on_path / path.len() as f64;
    
        for (route_id, board_node_id, dmbrk_node_id) in path {
            let segrwd = route_segment_rwds.get_mut(&route_id).
                expect("route not in segment rewards map!");
            segrwd.add_load(board_node_id, dmbrk_node_id, demand_reward, false);
        }
    
    }

    return satisfied_demand;
}

fn allocate_demand_helper(rmng_demand: f64, 
                          dst_hyperpaths: &HashMap<usize, Path>, 
                          src: usize,
                          route_segment_loads: &mut HashMap<String, RouteLoad>,
                          path_so_far: Vec<(String, usize, usize)>,
                          paths_and_amts: &mut Vec<(Vec<(String, usize, usize)>, f64)>)
                          -> f64 {
    if rmng_demand == 0. {
        // we've reached the goal, or demand has vanished.
        return rmng_demand;
    }

    let path_from_src = match dst_hyperpaths.get(&src) {
        Some(pfs) => pfs,
        None => {return rmng_demand}  // panic!("no src hyperpath for {}", src),
    };
    let next_nodes = path_from_src.next_nodes();
    let probs = path_from_src.route_probabilities();

    if next_nodes.len() == 0 {
        // there are no "next nodes", meaning we're at the destination!
        paths_and_amts.push((path_so_far, rmng_demand));
        return rmng_demand;
    }
    let mut demand_at_goal = 0.;
    // for each path from src in the hyperpath:
    for (route_id, prob) in probs {
        // add the appropriate amount of demand to the route segment load
        let demand_on_segment = prob * rmng_demand;
        let next_node = next_nodes.get(&route_id).expect("no next nodes!");
        let demand_that_fits = match &route_id[..] {
            key if key == WALK_KEY => demand_on_segment,
            _ => {
                let segment_loads = route_segment_loads.get_mut(&route_id).expect("no route seg!");
                segment_loads.add_load(src, *next_node, demand_on_segment, true)
            }
        };
        // recurse at the end-point of the segment
        let mut path_so_far = path_so_far.clone();
        if &route_id[..] != WALK_KEY {
            path_so_far.push((route_id.clone(), src, *next_node));
        }
        let satisfied = allocate_demand_helper(demand_that_fits, dst_hyperpaths, *next_node, 
                                               route_segment_loads, path_so_far, paths_and_amts);

        demand_at_goal += satisfied;

        if route_id != WALK_KEY {
            // only add load that actually made it to the end
            let segment_loads = route_segment_loads.get_mut(&route_id).expect("no final seg!");
            segment_loads.add_load(src, *next_node, satisfied, false);
        }
    }

    return demand_at_goal;
                            
}


fn float_cumsum(array: &Array<f64, Ix1>) -> Array<f64, Ix1> {
    let mut cumsum = Array::zeros(array.dim());
    let mut sum = 0.;
    for (ii, elem) in array.indexed_iter() {
        sum += *elem;
        cumsum[ii] = sum;
    }
    return cumsum;
}


#[cfg(test)]
mod tests {
    use petgraph::graph::DiGraph;
    use petgraph::graphmap::DiGraphMap;
    use approx::assert_ulps_eq;
    
    use super::super::Point2d;
    use super::super::path_network::PathSegment;
    use super::*;

    static DEFAULT_CFG: StaticSimConfig = StaticSimConfig {
        basin_radius_m: 500.,
        transfer_radius_m: 100.,
        transfer_penalty_m: 13.5,
        beeline_dist_factor: 1.3,
        walk_speed_mps: 1.35,
        mean_intersection_time_s: 10.,
        mean_stop_time_s: 60.,
        adjacency_threshold_s: 300.,
        penalize_indirection: false,
        ignore_small_components: true,
    };

    #[test]
    fn test_hyperpath_dijkstra_minimal() {
        // build a simple graph with only two nodes and one path between them
        let mut routing_graph: DiGraph<Node, RoutingGraphEdge> = DiGraph::new();
        let aa = routing_graph.add_node(Node);
        let bb = routing_graph.add_node(Node);
        let edge_weight = 5.;
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new("101", edge_weight));
        let mut freqs = HashMap::new();
        let freq = 1. / (15. * 60.);
        freqs.insert(String::from("101"), freq);

        let mut origins = HashSet::new();
        origins.insert(aa);
        let hyperpaths = hyperpath_dijkstra(&routing_graph, None, &freqs, Some(origins), bb, 0., 
                                            None);
        assert!(hyperpaths.contains_key(&0));
    
        let correct_mean_journey_time = (1. + freq * edge_weight) / freq;
        let hp = hyperpaths.get(&0).unwrap();
        match hp {
            Path::Hyperpath(hp) => {
                assert_eq!(correct_mean_journey_time, hp.mean_journey_time);
                assert_eq!(freq, hp.effective_frequency);
                let probs = hp.route_probabilities();
                assert!(probs.contains_key("101"));
                assert_eq!(probs.len(), 1);
                assert_eq!(*probs.get("101").unwrap(), 1.);
            }
            _ => panic!("Path is the wrong type!"),
        }
    }

    #[test]
    fn test_hyperpath_dijkstra_nopath() {
        // build a simple graph with only two nodes and one path between them
        let mut routing_graph: DiGraph<Node, RoutingGraphEdge> = DiGraph::new();
        let aa = routing_graph.add_node(Node);
        let bb = routing_graph.add_node(Node);
        routing_graph.add_node(Node);
        let edge_weight = 5.;
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new("101", edge_weight));
        let mut freqs = HashMap::new();
        let freq = 1. / (15. * 60.);
        freqs.insert(String::from("101"), freq);

        let hyperpaths = hyperpath_dijkstra(&routing_graph, None, &freqs, None, bb, 0., None);

        assert!(hyperpaths.contains_key(&0));
        assert!(!hyperpaths.contains_key(&2));
    }
   
    #[test]
    fn test_hyperpath_dijkstra_multiroutes() {
        // build a simple graph with only two nodes and one path between them
        let mut routing_graph: DiGraph<Node, RoutingGraphEdge> = DiGraph::new();
        let aa = routing_graph.add_node(Node);
        let bb = routing_graph.add_node(Node);
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new("101", 5.));
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new("102", 6.));
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new("103", 7.));
        let mut freqs = HashMap::new();
        freqs.insert(String::from("101"), 0.5);
        freqs.insert(String::from("102"), 0.25);
        freqs.insert(String::from("103"), 1.0);

        let hyperpaths = hyperpath_dijkstra(&routing_graph, None, &freqs, None, bb, 0., None);

        assert_eq!(hyperpaths.len(), 2);
        assert!(hyperpaths.contains_key(&0));
        match hyperpaths.get(&0).unwrap() {
            Path::Hyperpath(hp) => {
                assert_eq!(hp.mean_journey_time, 20. / 3.);
                let probs = hp.route_probabilities();
                assert_eq!(probs.len(), 2);
                assert_eq!(*probs.get("101").unwrap(), 2. / 3.);
                assert_eq!(*probs.get("102").unwrap(), 1. / 3.);
                assert_eq!(hp.effective_frequency, 0.75);
            },
            _ => panic!("This should not be a walking path!"),
        }
    }
   
    #[test]
    fn test_hyperpath_dijkstra_multinodes() {
        let mut graph = DiGraph::new();
        let aa = graph.add_node(Node);
        let xx = graph.add_node(Node);
        let yy = graph.add_node(Node);
        let bb = graph.add_node(Node);
        // route 1 is directly from a to b
        graph.add_edge(aa, bb, RoutingGraphEdge::new("1", 25.));
        // route 2 is from a to x, then x to y
        graph.add_edge(aa, xx, RoutingGraphEdge::new("2", 7.));
        graph.add_edge(xx, yy, RoutingGraphEdge::new("2", 6.));
        graph.add_edge(aa, yy, RoutingGraphEdge::new("2", 13.));
        // route 3 is from x to y, then from y to b
        graph.add_edge(xx, yy, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(yy, bb, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(xx, bb, RoutingGraphEdge::new("3", 8.));
        // route 4 is from y to b
        graph.add_edge(yy, bb, RoutingGraphEdge::new("4", 10.));

        let mut freqs = HashMap::new();
        let freq1 = 1./6.;
        freqs.insert(String::from("1"), freq1);
        let freq2 = freq1;
        freqs.insert(String::from("2"), freq2);
        let freq3 = 1./15.;
        freqs.insert(String::from("3"), freq3);
        let freq4 = 1./3.;
        freqs.insert(String::from("4"), freq4);

        let hyperpaths = hyperpath_dijkstra(&graph, None, &freqs, None, bb, 0., None);
        assert!(hyperpaths.contains_key(&0));
        assert!(hyperpaths.contains_key(&1));
        assert!(hyperpaths.contains_key(&2));
        assert!(hyperpaths.contains_key(&3));

        // according to Spiess and Florian, the optimal strategies are as follows:
        match hyperpaths.get(&0).unwrap() {
            Path::Hyperpath(hpa) => {
                let probs = hpa.route_probabilities();
                assert_eq!(probs.len(), 2);
                assert_eq!(*probs.get("1").unwrap(), 0.5);        
                assert_eq!(*probs.get("2").unwrap(), 0.5);
                let nn = hpa.next_nodes();
                let nnset: HashSet<String> = nn.keys().map(|kk| kk.clone()).collect();
                let probset: HashSet<String> = probs.keys().map(|kk| kk.clone()).collect();
                assert_eq!(nnset, probset);
                assert_eq!(*nn.get("1").unwrap(), bb.index());
                assert_eq!(*nn.get("2").unwrap(), yy.index());
            },
            _ => panic!("This should not be a walking path!"),
        }

        match hyperpaths.get(&1).unwrap() {
            Path::Hyperpath(hpx) => {
                let correct_ef = freqs.get("2").unwrap() + freqs.get("3").unwrap();
                assert_eq!(hpx.effective_frequency, correct_ef);
                let correct_mjt = (1. + (6. + 11.5) * freq2 + 8. * freq3) / correct_ef;
                // check this is approximately correct, floating-point might mean it's not exact
                assert_ulps_eq!(hpx.mean_journey_time, correct_mjt);
                let probs = hpx.route_probabilities();
                assert_eq!(probs.len(), 2);
                assert_eq!(*probs.get("2").unwrap(), freq2 / correct_ef);
                assert_eq!(*probs.get("3").unwrap(), freq3 / correct_ef);
                let nn = hpx.next_nodes();
                let nnset: HashSet<String> = nn.keys().map(|kk| kk.clone()).collect();
                let probset: HashSet<String> = probs.keys().map(|kk| kk.clone()).collect();
                assert_eq!(nnset, probset);
                assert_eq!(*nn.get("2").unwrap(), yy.index());
                assert_eq!(*nn.get("3").unwrap(), bb.index());
            }
            _ => panic!("This should not be a walking path!"),            
        }

        match hyperpaths.get(&2).unwrap() {
            Path::Hyperpath(hpy) => {
                let probs = hpy.route_probabilities();
                assert_eq!(probs.len(), 2);
                assert_ulps_eq!(*probs.get("3").unwrap(), 1./6.);
                assert_ulps_eq!(*probs.get("4").unwrap(), 5./6.);
                let nn = hpy.next_nodes();
                let nnset: HashSet<String> = nn.keys().map(|kk| kk.clone()).collect();
                let probset: HashSet<String> = probs.keys().map(|kk| kk.clone()).collect();
                assert_eq!(nnset, probset);
                assert_eq!(*nn.get("3").unwrap(), bb.index());
                assert_eq!(*nn.get("4").unwrap(), bb.index());
                assert_ulps_eq!(hpy.mean_journey_time, 11.5);
                assert_ulps_eq!(hpy.effective_frequency, 1./3. + 1./15.);
            }
            _ => panic!("This should not be a walking path!"),            
        }

        match hyperpaths.get(&3).unwrap() {
            Path::Hyperpath(hpb) => {
                assert_eq!(hpb.mean_journey_time, 0.);
                assert_eq!(hpb.route_probabilities().len(), 0);
                assert_eq!(hpb.next_nodes().len(), 0);
            }
            _ => panic!("This should not be a walking path!"),            
        }
    }

    #[test]
    fn test_hyperpath_dijkstra_multinodes_onlysomenodes() {
        let mut graph = DiGraph::new();
        let aa = graph.add_node(Node);
        let xx = graph.add_node(Node);
        let yy = graph.add_node(Node);
        let bb = graph.add_node(Node);
        // route 1 is directly from a to b
        graph.add_edge(aa, bb, RoutingGraphEdge::new("1", 25.));
        // route 2 is from a to x, then x to y
        graph.add_edge(aa, xx, RoutingGraphEdge::new("2", 7.));
        graph.add_edge(xx, yy, RoutingGraphEdge::new("2", 6.));
        graph.add_edge(aa, yy, RoutingGraphEdge::new("2", 13.));
        // route 3 is from x to y, then from y to b
        graph.add_edge(xx, yy, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(yy, bb, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(xx, bb, RoutingGraphEdge::new("3", 8.));
        // route 4 is from y to b
        graph.add_edge(yy, bb, RoutingGraphEdge::new("4", 10.));

        let mut freqs = HashMap::new();
        let freq1 = 1./6.;
        freqs.insert(String::from("1"), freq1);
        let freq2 = freq1;
        freqs.insert(String::from("2"), freq2);
        let freq3 = 1./15.;
        freqs.insert(String::from("3"), freq3);
        let freq4 = 1./3.;
        freqs.insert(String::from("4"), freq4);

        let mut used_stops = HashSet::new();
        used_stops.insert(aa);
        used_stops.insert(yy);
        used_stops.insert(bb);

        let hyperpaths = hyperpath_dijkstra(&graph, Some(&used_stops), &freqs, None, bb, 0., None);
        assert!(hyperpaths.contains_key(&0));
        assert!(! hyperpaths.contains_key(&1));
        assert!(hyperpaths.contains_key(&2));
        assert!(hyperpaths.contains_key(&3));

        // according to Spiess and Florian, the optimal strategies are as follows:
        match hyperpaths.get(&0).unwrap() {
            Path::Hyperpath(hpa) => {
                let probs = hpa.route_probabilities();
                assert_eq!(probs.len(), 2);
                assert_eq!(*probs.get("1").unwrap(), 0.5);        
                assert_eq!(*probs.get("2").unwrap(), 0.5);
                let nn = hpa.next_nodes();
                let nnset: HashSet<String> = nn.keys().map(|kk| kk.clone()).collect();
                let probset: HashSet<String> = probs.keys().map(|kk| kk.clone()).collect();
                assert_eq!(nnset, probset);
                assert_eq!(*nn.get("1").unwrap(), bb.index());
                assert_eq!(*nn.get("2").unwrap(), yy.index());
            },
            _ => panic!("This should not be a walking path!"),
        }

        match hyperpaths.get(&2).unwrap() {
            Path::Hyperpath(hpy) => {
                let probs = hpy.route_probabilities();
                assert_eq!(probs.len(), 2);
                assert_ulps_eq!(*probs.get("3").unwrap(), 1./6.);
                assert_ulps_eq!(*probs.get("4").unwrap(), 5./6.);
                let nn = hpy.next_nodes();
                let nnset: HashSet<String> = nn.keys().map(|kk| kk.clone()).collect();
                let probset: HashSet<String> = probs.keys().map(|kk| kk.clone()).collect();
                assert_eq!(nnset, probset);
                assert_eq!(*nn.get("3").unwrap(), bb.index());
                assert_eq!(*nn.get("4").unwrap(), bb.index());
                assert_ulps_eq!(hpy.mean_journey_time, 11.5);
                assert_ulps_eq!(hpy.effective_frequency, 1./3. + 1./15.);
            }
            _ => panic!("This should not be a walking path!"),            
        }

        match hyperpaths.get(&3).unwrap() {
            Path::Hyperpath(hpb) => {
                assert_eq!(hpb.mean_journey_time, 0.);
                assert_eq!(hpb.route_probabilities().len(), 0);
                assert_eq!(hpb.next_nodes().len(), 0);
            }
            _ => panic!("This should not be a walking path!"),            
        }
    }

    #[test]
    fn test_hyperpath_dijkstra_walkonly() {
        let mut routing_graph: DiGraph<Node, RoutingGraphEdge> = DiGraph::new();
        let aa = routing_graph.add_node(Node);
        let bb = routing_graph.add_node(Node);
        let edge_weight = 5.;
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new(WALK_KEY, edge_weight));
        let mut freqs = HashMap::new();
        let freq = 1. / (15. * 60.);
        freqs.insert(String::from("101"), freq);

        let mut origins = HashSet::new();
        origins.insert(aa);
        let hyperpaths = hyperpath_dijkstra(&routing_graph, None, &freqs, Some(origins), bb, 0., 
                                            None);
        assert!(hyperpaths.contains_key(&0));
        assert!(hyperpaths.contains_key(&1));
        match hyperpaths.get(&0).unwrap() {
            Path::Walkingpath(wp) => {
                assert_eq!(wp.mean_journey_time(), 5.);
                assert_eq!(wp.next_node_id, Some(1));
                assert_eq!(wp.walk_time, 5.);
                assert_eq!(wp.postwalk_time, 0.);
            }
            _ => panic!("This should be a walking path!"),
        }
    }
   
    #[test]
    fn test_hyperpath_dijkstra_walkorride() {
        let mut routing_graph: DiGraph<Node, RoutingGraphEdge> = DiGraph::new();
        let aa = routing_graph.add_node(Node);
        let bb = routing_graph.add_node(Node);
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new("101", 5.));
        routing_graph.add_edge(aa, bb, RoutingGraphEdge::new(WALK_KEY, 7.));
        let mut freqs = HashMap::new();
        freqs.insert(String::from("101"), 1.);

        // at this frequency, riding should dominate
        let mut origins = HashSet::new();
        origins.insert(aa);
        let hyperpaths = hyperpath_dijkstra(&routing_graph, None, &freqs, Some(origins.clone()),
                                            bb, 0., None);
        match hyperpaths.get(&0).unwrap() {
            Path::Hyperpath(hpa) => {
                let nn = hpa.next_nodes();
                assert_eq!(nn.len(), 1);
                assert!(!nn.contains_key(WALK_KEY));
            }
            _ => panic!("This should not be a walking path!"),            
        }

        // at this frequency, walking should dominate
        freqs.insert(String::from("101"), 0.4);
        let hyperpaths = hyperpath_dijkstra(&routing_graph, None, &freqs, Some(origins.clone()),
                                            bb, 0., None);
        match hyperpaths.get(&0).unwrap() {
            Path::Walkingpath(_) => (),
            _ => panic!("This should be a walking path!"),
        }
    }
   
    #[test]
    fn test_hyperpath_dijkstra_multinodes_walk() {
        let mut graph = DiGraph::new();
        let aa = graph.add_node(Node);
        let xx = graph.add_node(Node);
        let yy = graph.add_node(Node);
        let bb = graph.add_node(Node);
        // route 1 is directly from a to b
        graph.add_edge(aa, bb, RoutingGraphEdge::new("1", 25.));
        // route 2 is from a to x, then x to y
        graph.add_edge(aa, xx, RoutingGraphEdge::new("2", 7.));
        graph.add_edge(xx, yy, RoutingGraphEdge::new("2", 6.));
        graph.add_edge(aa, yy, RoutingGraphEdge::new("2", 13.));
        // route 3 is from x to y, then from y to b
        graph.add_edge(xx, yy, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(yy, bb, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(xx, bb, RoutingGraphEdge::new("3", 8.));
        // route 4 is from y to b
        graph.add_edge(yy, bb, RoutingGraphEdge::new("4", 10.));
        // add a walking link
        graph.add_edge(xx, yy, RoutingGraphEdge::new(WALK_KEY, 0.1));

        let mut freqs = HashMap::new();
        let freq1 = 1./6.;
        freqs.insert(String::from("1"), freq1);
        let freq2 = freq1;
        freqs.insert(String::from("2"), freq2);
        let freq3 = 1./15.;
        freqs.insert(String::from("3"), freq3);
        let freq4 = 1./3.;
        freqs.insert(String::from("4"), freq4);

        let hyperpaths = hyperpath_dijkstra(&graph, None, &freqs, None, bb, 0., None);
        match hyperpaths.get(&1).unwrap() {
            Path::Walkingpath(wp) => {
                assert_eq!(wp.next_node_id, Some(2));
                match hyperpaths.get(&0).unwrap() {
                    Path::Hyperpath(hpa) => {
                        let a_probs = hpa.route_probabilities();
                        assert_eq!(a_probs.len(), 1);
                        assert_eq!(*a_probs.get("2").unwrap(), 1.);

                        let ann = hpa.next_nodes();
                        assert_eq!(ann.len(), 1);
                        assert_eq!(*ann.get("2").unwrap(), 1);
                    }
                    _ => panic!("This should be a hyperpath!"),            
                }
            }
            _ => panic!("This should be a walking path!"),
        }
    }
   
    #[test]
    fn test_hyperpath_dijkstra_transferpenalty() {
        let mut graph = DiGraph::new();
        let aa = graph.add_node(Node);
        let xx = graph.add_node(Node);
        let yy = graph.add_node(Node);
        let bb = graph.add_node(Node);
        let cc = graph.add_node(Node);
        // route 1 is directly from a to b
        graph.add_edge(aa, bb, RoutingGraphEdge::new("1", 25.));
        // route 2 is from a to x, then x to y
        graph.add_edge(aa, xx, RoutingGraphEdge::new("2", 7.));
        graph.add_edge(xx, yy, RoutingGraphEdge::new("2", 6.));
        graph.add_edge(aa, yy, RoutingGraphEdge::new("2", 13.));
        // route 3 is from x to y, then from y to b
        graph.add_edge(xx, yy, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(yy, bb, RoutingGraphEdge::new("3", 4.));
        graph.add_edge(xx, bb, RoutingGraphEdge::new("3", 8.));
        // route 4 is from y to b
        graph.add_edge(yy, bb, RoutingGraphEdge::new("4", 10.));
        // add a walking link
        graph.add_edge(bb, cc, RoutingGraphEdge::new(WALK_KEY, 0.1));

        let mut freqs = HashMap::new();
        let freq1 = 1./6.;
        freqs.insert(String::from("1"), freq1);
        let freq2 = freq1;
        freqs.insert(String::from("2"), freq2);
        let freq3 = 1./15.;
        freqs.insert(String::from("3"), freq3);
        let freq4 = 1./3.;
        freqs.insert(String::from("4"), freq4);

        // run with transfer penalty of 10
        let hyperpaths = hyperpath_dijkstra(&graph, None, &freqs, None, cc, 10., None);
        match hyperpaths.get(&0).unwrap() {
            Path::Hyperpath(hpa) => {
                // the path from a on line 2, transferring to 3 at y, should be dominated
                let a_probs = hpa.route_probabilities();
                assert_eq!(a_probs.len(), 1);
                assert!(a_probs.contains_key("1"));
                assert_eq!(hpa.mean_journey_time, 25. + 1. / freq1 + 0.1);
            }
            _ => panic!("This should be a hyperpath!"),            
        }
        match hyperpaths.get(&1).unwrap() {
            Path::Hyperpath(hpx) => {
                assert_eq!(hpx.route_probabilities().len(), 1);
            }
            _ => panic!("This should be a hyperpath!"),            
        }

        // try with a much lower transfer penalty
        let hyperpaths = hyperpath_dijkstra(&graph, None, &freqs, None, cc, 1., None);
        match hyperpaths.get(&0).unwrap() {
            Path::Hyperpath(hpa) => {
                let a_probs = hpa.route_probabilities();
                assert_eq!(a_probs.len(), 2);
                assert!(a_probs.contains_key("1"));
                assert!(a_probs.contains_key("2"));
                assert_ulps_eq!(hpa.mean_journey_time, 28.35);
            }
            _ => panic!("This should be a hyperpath!"),            
        }
        match hyperpaths.get(&1).unwrap() {
            Path::Hyperpath(hpx) => {
                assert_eq!(hpx.route_probabilities().len(), 2);
            }
            _ => panic!("This should be a hyperpath!"),            
        }
    }

    #[test]
    fn test_static_sim_oneroad() {
        let (mut sim, veh_type) = make_simple_simenv(15., &DEFAULT_CFG).unwrap();
        // let veh_power = sim.vehicle_type.avg_power_kW;

        // test with no transit
        let result = sim.run(&vec![], &vec![], &vec![], false);
        assert_eq!(result.satisfied_demand, 0.);
        assert_eq!(result.power_used_kW, 0.);

        // with a loop route
        let routes = vec![vec![0, 1, 0]];
        let vehicles = vec![veh_type.clone()];
        // run every 10m20s, so that only one vehicle is needed
        let result = sim.run(&routes, &vec![1. / 620.], &vehicles, false);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);
        // run every 5m10s, so that two vehicles are needed
        let result = sim.run(&routes, &vec![1. / 310.], &vehicles, false);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW * 2.);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);
        // run every 7m45s, so that 1.5 vehicles are needed
        let result = sim.run(&routes, &vec![1. / 465.], &vehicles, false);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW * 4. / 3.);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);
        // run every 3m26.67s, so that three vehicles are needed
        let result = sim.run(&routes, &vec![3. / 620.], &vehicles, false);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW * 3.);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);

        // with a non-looping route
        let routes = vec![vec![0, 1]];
        // run every 10m20s, so that only one vehicle is needed, half the time
        let result = sim.run(&routes, &vec![1. / 620.], &vehicles, false);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW * 0.5);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);
        // run every 5m10s, so that one vehicle is needed
        let result = sim.run(&routes, &vec![1. / 310.], &vehicles, false);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);
        // run every 3m26.67s, so that 1.5 vehicles are needed
        let result = sim.run(&routes, &vec![3. / 620.], &vehicles, false);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW * 1.5);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);
    }

    #[test]
    fn test_static_sim_oneroad_offsetdemands() {
        let src_offset = Point2d::new(100., -30.);
        let dst_offset = Point2d::new(20., 51.3);
        let (mut sim, veh_type) = make_simple_simenv_with_offsets(
            15., &DEFAULT_CFG, Some(src_offset.clone()), Some(dst_offset.clone())).unwrap();

        // test with no transit
        let result = sim.run(&vec![], &vec![], &vec![], false);
        assert_eq!(result.satisfied_demand, 0.);
        assert_eq!(result.power_used_kW, 0.);
        check_per_stop_sums(&result);

        let routes = vec![vec![0, 1, 0]];
        let vehicles = vec![veh_type.clone()];
        // run every 10:20, so that only one vehicle is needed
        // with penalize_indirection = false, this should have the same results as without offsets.
        let result = sim.run(&routes, &vec![1. / 620.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW);
        check_per_stop_sums(&result);

        let cfg = Box::new(DEFAULT_CFG.clone());
        let cfg: &'static mut StaticSimConfig = Box::leak(cfg);
        cfg.basin_radius_m = 100.;
        let (mut sim, _) = make_simple_simenv_with_offsets(15., cfg, Some(src_offset.clone()), 
                                                           Some(dst_offset.clone())).unwrap();
        let result = sim.run(&routes, &vec![1. / 620.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 0.);
        check_per_stop_sums(&result);

        // add penalize indirection test
        let cfg = Box::new(DEFAULT_CFG.clone());
        let cfg: &'static mut StaticSimConfig = Box::leak(cfg);
        cfg.penalize_indirection = true;
        let (mut sim, _) = make_simple_simenv_with_offsets(15., cfg, Some(src_offset.clone()), 
                                                           Some(dst_offset.clone())).
                        unwrap();
        let result = sim.run(&routes, &vec![1. / 620.], &vehicles, false);
        assert!(result.satisfied_demand > 0.);
        assert!(result.satisfied_demand < 15.);
        check_per_stop_sums(&result);
    }

    #[test]
    fn test_static_sim_oneroad_demandoverloop() {
        let (mut sim, veh_type) = make_simple_simenv(15., &DEFAULT_CFG).unwrap();

        // test with no transit
        let result = sim.run(&vec![], &vec![], &vec![], false);
        assert_eq!(result.satisfied_demand, 0.);
        assert_eq!(result.power_used_kW, 0.);

        let vehicles = vec![veh_type];
        let routes = vec![vec![1, 0]];
        let result = sim.run(&routes, &vec![1. / 620.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 0.);
        check_per_stop_sums(&result);

        let routes = vec![vec![1, 0, 1]];
        let result = sim.run(&routes, &vec![1. / 620.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        check_per_stop_sums(&result);
    }

    #[test]
    fn test_static_sim_oneroad_threshold() {
        let (mut sim, veh_type) = make_simple_simenv(600., &DEFAULT_CFG).unwrap();

        // non-looping route
        let routes = vec![vec![0, 1]];
        let veh_power = veh_type.avg_power_kW;
        let vehicles = vec![veh_type];

        // run every 10m.  Hourly capacity is then 6 vehicles, just enough to carry 600 people.
        let result = sim.run(&routes, &vec![1. / 600.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 600.);
        assert_ulps_eq!(result.power_used_kW, veh_power * 310. / 600.);
        check_per_stop_sums(&result);

        // run every 15:30.  Hourly capacity is then 4 vehicles, which only meets 400 demand
        let result = sim.run(&routes, &vec![1. / 900.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 400.);
        assert_ulps_eq!(result.power_used_kW, veh_power * 310. / 900.);
        check_per_stop_sums(&result);

        // looping route
        let routes = vec![vec![0, 1, 0]];
        // run every 10m.  Hourly capacity is then 6 vehicles, just enough to carry 600 people.
        let result = sim.run(&routes, &vec![1. / 600.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 600.);
        assert_ulps_eq!(result.power_used_kW, veh_power * 620. / 600.);
        check_per_stop_sums(&result);

        // run every 15:30.  Hourly capacity is then 4 vehicles, which only meets 400 demand
        let result = sim.run(&routes, &vec![1. / 900.], &vehicles, false);
        assert_ulps_eq!(result.satisfied_demand, 400.);
        assert_ulps_eq!(result.power_used_kW, veh_power * 620. / 900.);
        check_per_stop_sums(&result);
    }

    #[test]
    fn test_static_sim_multiroutes() {
        let (mut sim, veh_type) = make_simple_simenv(15., &DEFAULT_CFG).unwrap();

        let routes = vec![vec![0, 1, 0], vec![0, 1, 0]];
        let freqs = vec![1. / 620., 1. / 620.];

        let result = sim.run(&routes, &freqs, &vec![veh_type.clone(), veh_type.clone()], false);
        assert_ulps_eq!(result.satisfied_demand, 15.);
        assert_ulps_eq!(result.power_used_kW, veh_type.avg_power_kW * 2.);
        check_per_stop_sums(&result);  
    }

    #[test]
    fn test_static_sim_multiroutes_different_vehicles() {
        let (mut sim, _) = make_simple_simenv(1000., &DEFAULT_CFG).unwrap();

        let routes = vec![vec![0, 1, 0], vec![0, 1, 0]];
        let freqs = vec![1. / 620., 1. / 620.];
        let veh_type1 = PtVehicleType::new("0", "road", "high capacity", 50, 50, 11.2, 26.7);
        let veh_type2 = PtVehicleType::new("1", "road", "low capacity", 7, 3, 11.2, 26.7);
        let veh_types = vec![veh_type1, veh_type2];

        let result = sim.run(&routes, &freqs, &veh_types, false);
        // check that rewards are higher on first route
        let route_1_satdem = result.per_stop_satisfied_demand[0].sum();
        let route_2_satdem = result.per_stop_satisfied_demand[1].sum();
        assert!(route_1_satdem > route_2_satdem);
        check_per_stop_sums(&result);

        let veh_type1 = PtVehicleType::new("0", "road", "high cost", 50, 50, 11.2, 10000.);
        let veh_type2 = PtVehicleType::new("1", "road", "low cost", 50, 50, 11.2, 1.0);
        let veh_types = vec![veh_type1, veh_type2];

        let result = sim.run(&routes, &freqs, &veh_types, false);
        // check that power is larger on first route with very high energy cost
        let route_1_power = result.per_stop_power_used_kW[0].sum();
        println!("route 1 power: {}", route_1_power);
        // check that power is less on second route with very low energy cost
        let route_2_power = result.per_stop_power_used_kW[1].sum();
        println!("route 2 power: {}", route_2_power);
        assert!(route_1_power > route_2_power);
        check_per_stop_sums(&result);
    }

    #[test]
    fn test_static_sim_multiroutes_threshold() {
        let (mut sim, veh_type) = make_simple_simenv(1200., &DEFAULT_CFG).unwrap();

        // looping routes
        let routes = vec![vec![0, 1, 0], vec![0, 1, 0]];

        // run every 10 minutes, so all demand is satisfied
        let freqs = vec![1. / 600., 1. / 600.];
        let vehicle_types = vec![veh_type.clone(), veh_type];
        let result = sim.run(&routes, &freqs, &vehicle_types, false);
        assert_ulps_eq!(result.satisfied_demand, 1200.);
        check_per_stop_sums(&result);

        // run every 15 minutes, so some demand is unsatisfied
        let freqs = vec![1. / 900., 1. / 900.];
        let result = sim.run(&routes, &freqs, &vehicle_types, false);
        assert_ulps_eq!(result.satisfied_demand, 800.);
        check_per_stop_sums(&result);

        // non-looping routes
        let routes = vec![vec![0, 1], vec![0, 1]];

        // run every 10 minutes, so all demand is satisfied
        let freqs = vec![1. / 600., 1. / 600.];
        let result = sim.run(&routes, &freqs, &vehicle_types, false);
        assert_ulps_eq!(result.satisfied_demand, 1200.);
        check_per_stop_sums(&result);
        // run every 15 minutes, so some demand is unsatisfied
        let freqs = vec![1. / 900., 1. / 900.];
        let result = sim.run(&routes, &freqs, &vehicle_types, false);
        assert_ulps_eq!(result.satisfied_demand, 800.);
        check_per_stop_sums(&result);
    }

    #[test]
    fn test_per_demand_power() {
        let mut street_graph = DiGraphMap::new();
        let short_time = 5. * 60.;
        street_graph.add_edge(0, 1, make_pathsegment(0, short_time));
        street_graph.add_edge(1, 3, make_pathsegment(0, short_time));
        let long_time = 10. * 60.;
        street_graph.add_edge(0, 2, make_pathsegment(0, long_time));
        street_graph.add_edge(2, 3, make_pathsegment(0, long_time));
        let node_positions = vec![
            Point2d::new(0., 0.),
            Point2d::new(5000., 0.),
            Point2d::new(5000., 5000.),
            Point2d::new(10000., 0.),
        ];
        let pn = PathNetwork::from_graph(street_graph, node_positions, &DEFAULT_CFG);
        let veh_power = 26.7;
        let veh_type = PtVehicleType::new("1", "road", "a normal-sized bus", 50, 0, 18., veh_power);
        let transfer_graph = build_transfer_graph(&pn);
        validate_transfer_graph(&pn, &transfer_graph, &DEFAULT_CFG);
        let demand_graph = DiGraph::new();
        let basin_connections = build_basin_conns(&pn, &demand_graph, &DEFAULT_CFG);
        validate_basin_conns(&pn, &demand_graph, &basin_connections, &DEFAULT_CFG);
        let mut sim = StaticSimulator {
            path_network: pn,
            demand_graph: demand_graph,
            transfer_graph,
            basin_connections,
            cfg: &DEFAULT_CFG,
            rng: Isaac64Rng::seed_from_u64(RAND_SEED),
        };

        // two paths through the graph, one short and one long
        let routes = vec![vec![0, 1, 3], vec![0, 2, 3]];
        let stop_time = &DEFAULT_CFG.mean_stop_time_s;
        let isctn_time = &DEFAULT_CFG.mean_intersection_time_s;
        let total_short_time = (short_time + isctn_time + stop_time) * 2.;
        let total_long_time = (long_time + isctn_time + stop_time) * 2.;
        let freqs = vec![1./total_short_time, 1./total_long_time];
        let vehicles = vec![veh_type.clone(), veh_type.clone()];
        let result = sim.run(&routes, &freqs, &vehicles, false);

        let first_route_psp = &result.per_stop_power_used_kW[0];
        let tsp_middle = (stop_time / total_short_time) * veh_power;
        assert_ulps_eq!(first_route_psp[1], tsp_middle);
        let tsp_first = (veh_power - tsp_middle) / 2.;
        assert_ulps_eq!(first_route_psp[0], tsp_first);
        let tsp_last = tsp_first;
        assert_ulps_eq!(first_route_psp[2], tsp_last);

        let second_route_psp = &result.per_stop_power_used_kW[1];
        let tlt_middle = (stop_time + long_time * 2.) - short_time * 2.;
        let tlp_middle = tlt_middle * veh_power / total_long_time;
        assert_ulps_eq!(second_route_psp[1], tlp_middle);
        let tlp_last = (veh_power - tlp_middle)  / 2.;
        assert_ulps_eq!(second_route_psp[2], tlp_last);
        let tlp_first = tlp_last;
        assert_ulps_eq!(second_route_psp[0], tlp_first);
    }

    #[test]
    fn test_static_sim_multinodes_linear() -> Result<(), std::io::Error> {
        let mut street_graph = DiGraphMap::new();
        street_graph.add_edge(0, 1, make_pathsegment(0, 5. * 60.));
        street_graph.add_edge(1, 2, make_pathsegment(0, 5. * 60.));
        street_graph.add_edge(2, 3, make_pathsegment(0, 5. * 60.));
        street_graph.add_edge(3, 4, make_pathsegment(0, 5. * 60.));
        street_graph.add_edge(4, 5, make_pathsegment(0, 5. * 60.));
        street_graph.add_edge(5, 6, make_pathsegment(0, 5. * 60.));
        street_graph.add_edge(6, 0, make_pathsegment(0, 20. * 60.));
        let node_positions = vec![
            Point2d::new(0., 0.),
            Point2d::new(5000., 0.),
            Point2d::new(10000., 0.),
            Point2d::new(15000., 0.),
            Point2d::new(20000., 0.),
            Point2d::new(25000., 0.),
            Point2d::new(30000., 0.),
        ];
        let mut demand_graph = DiGraph::new();
        let aa = demand_graph.add_node(node_positions[0].clone());
        let bb = demand_graph.add_node(node_positions[1].clone());
        let cc = demand_graph.add_node(node_positions[2].clone());
        let dd = demand_graph.add_node(node_positions[3].clone());
        let ee = demand_graph.add_node(node_positions[4].clone());
        let ff = demand_graph.add_node(node_positions[5].clone());
        let gg = demand_graph.add_node(node_positions[6].clone());

        let pn = PathNetwork::from_graph(street_graph, node_positions, &DEFAULT_CFG);

        // total demand: 210
        //         #  0  10  15  30  20  50  85
        //           [0, 10, 5 , 0 , 5 , 5 , 0 ],  # 25   0->25
        //           [0, 0 , 10, 30, 0 , 5 , 10],  # 55  15->70
        //           [0, 0 , 0 , 0 , 10, 0 , 40],  # 50  55->105
        //           [0, 0 , 0 , 0 , 5 , 10, 5 ],  # 20  75->95
        //           [0, 0 , 0 , 0 , 0 , 30, 10],  # 40  75->115
        //           [0, 0 , 0 , 0 , 0 , 0 , 20],  # 20  65->85
        //           [0, 0 , 0 , 0 , 0 , 0 , 0 ]   # 0   
        // # loads:  25, 70,105, 95,115, 85, 0        
        demand_graph.add_edge(aa, bb, 10.); 
        demand_graph.add_edge(aa, cc, 5.);
        demand_graph.add_edge(aa, ee, 5.); 
        demand_graph.add_edge(aa, ff, 5.); 
        demand_graph.add_edge(bb, cc, 10.);
        demand_graph.add_edge(bb, dd, 30.);
        demand_graph.add_edge(bb, ff, 5.);
        demand_graph.add_edge(bb, gg, 10.);
        demand_graph.add_edge(cc, ee, 10.);
        demand_graph.add_edge(cc, gg, 40.);
        demand_graph.add_edge(dd, ee, 5.);
        demand_graph.add_edge(dd, ff, 10.);
        demand_graph.add_edge(dd, gg, 5.);
        demand_graph.add_edge(ee, ff, 30.);
        demand_graph.add_edge(ee, gg, 10.);
        demand_graph.add_edge(ff, gg, 20.);

        let total_demand = get_total_demand(&demand_graph);

        let veh_power = 26.7;
        let veh_type = PtVehicleType::new("1", "road", "a normal-sized bus", 50, 0, 18., veh_power);
        let vehicles = vec![veh_type];
        let transfer_graph = build_transfer_graph(&pn);
        validate_transfer_graph(&pn, &transfer_graph, &DEFAULT_CFG);
        let basin_connections = build_basin_conns(&pn, &demand_graph, &DEFAULT_CFG);
        validate_basin_conns(&pn, &demand_graph, &basin_connections, &DEFAULT_CFG);
        let mut sim = StaticSimulator {
            path_network: pn,
            demand_graph: demand_graph,
            transfer_graph,
            basin_connections,
            cfg: &DEFAULT_CFG,
            rng: Isaac64Rng::seed_from_u64(RAND_SEED),
        };
        
        let routes = vec![vec![0, 1, 2, 3, 4, 5, 6, 0]];
        let n_vehicles = 5.;
        // drive time 3000 + 7 * stop time + 7 * intersection time
        let route_time = 3490.;
        let result = sim.run(&routes, &vec![n_vehicles / route_time], &vehicles, false);
        // let vehicle_penalty = veh_power / sim.cfg.person_value_in_kW;
        assert_ulps_eq!(result.satisfied_demand, 210.);
        assert_ulps_eq!(result.power_used_kW, veh_power * n_vehicles);
        check_per_stop_sums(&result);

        // reduced loads
        let n_vehicles = 2.;
        let freqs = vec![n_vehicles / 3600.];
        for _ in 0..100 {
            // with just two vehicles, max capacity is limited.  Actual satisfied demand will be
            // less than available demand, but depends on the random order in which the demands are
            // dealt with; not more than 20 will be dropped.
            let result = sim.run(&routes, &freqs, &vehicles, false);
            // print values here, since assert! doesn't show the arguments if it fails
            println!("{}, {}", result.satisfied_demand, total_demand);
            assert!(result.satisfied_demand < total_demand);
            assert!(result.satisfied_demand >= total_demand - 20.);
        }
        return Ok(());
    }

    #[test]
    fn test_static_sim_walkneeded() -> Result<(), std::io::Error> {
        let veh_power = 26.7;
        let veh_type = PtVehicleType::new("1", "road", "a normal-sized bus", 50, 0, 18., veh_power);
        let cfg = Box::new(DEFAULT_CFG.clone());
        let cfg: &'static mut StaticSimConfig = Box::leak(cfg);
        cfg.basin_radius_m = 500.;
        cfg.penalize_indirection = true;
        let path_network = walkneeded_helper(cfg);
        let transfer_graph = build_transfer_graph(&path_network);

        // people want to go a->d and d->a...
        let mut demand_graph = DiGraph::new();
        let aa = demand_graph.add_node(path_network.get_node_positions()[0].clone());
        // demand_graph.add_node(path_network.get_node_positions()[1].clone());
        // demand_graph.add_node(path_network.get_node_positions()[2].clone());
        let dd = demand_graph.add_node(path_network.get_node_positions()[3].clone());
        demand_graph.add_edge(aa, dd, 10.);
        demand_graph.add_edge(dd, aa, 15.);

        validate_transfer_graph(&path_network, &transfer_graph, &cfg);
        let basin_connections = build_basin_conns(&path_network, &demand_graph, &cfg);
        validate_basin_conns(&path_network, &demand_graph, &basin_connections, &cfg);
        println!("basin conns: {:?}", basin_connections);

        let mut sim = StaticSimulator {
            path_network,
            demand_graph: demand_graph.clone(),
            transfer_graph,
            basin_connections,
            cfg,
            rng: Isaac64Rng::seed_from_u64(RAND_SEED),
        };

        // ...but route only goes between b and c!
        let routes = vec![vec![1, 2]];
        let freqs = vec![12. / 3600.];

        // with radius 500m and penalized indirection, some demand will be satisfied, but not all
        let result = sim.run(&routes, &freqs, &vec![veh_type], false);
        // print this because plain asserts won't
        println!("satisfied demand: {}", result.satisfied_demand);
        assert!(result.satisfied_demand > 0.);
        assert!(result.satisfied_demand < 25.);
        check_per_stop_sums(&result);

        // with radius 100m, basins are too small for people to get to transit, so no demand is
        // satisfied.
        let veh_type = PtVehicleType::new("1", "road", "a normal-sized bus", 50, 0, 18., veh_power);
        let cfg = Box::new(DEFAULT_CFG.clone());
        let cfg: &'static mut StaticSimConfig = Box::leak(cfg);
        cfg.basin_radius_m = 100.;
        cfg.penalize_indirection = true;

        let path_network = walkneeded_helper(cfg);
        let transfer_graph = build_transfer_graph(&path_network);
        validate_transfer_graph(&path_network, &transfer_graph, &cfg);
        let basin_connections = build_basin_conns(&path_network, &demand_graph, &cfg);
        validate_basin_conns(&path_network, &demand_graph, &basin_connections, &cfg);
        println!("basin conns: {:?}", basin_connections);

        let mut sim = StaticSimulator {
            path_network,
            demand_graph: demand_graph.clone(),
            transfer_graph,
            basin_connections,
            cfg,
            rng: Isaac64Rng::seed_from_u64(RAND_SEED),
        };

        let result = sim.run(&routes, &freqs, &vec![veh_type], false);
        assert_eq!(result.satisfied_demand, 0.);
        check_per_stop_sums(&result);

        return Ok(());
    }

    fn walkneeded_helper<'a>(cfg: &'a StaticSimConfig) -> PathNetwork<'a, StaticSimConfig> {
        let mut street_graph = DiGraphMap::new();
        street_graph.add_edge(0, 1, make_pathsegment(0, 100.));
        street_graph.add_edge(1, 0, make_pathsegment(1, 100.));
        street_graph.add_edge(1, 2, make_pathsegment(2, 600.));
        street_graph.add_edge(2, 1, make_pathsegment(3, 600.));
        street_graph.add_edge(2, 3, make_pathsegment(4, 100.));
        street_graph.add_edge(3, 2, make_pathsegment(5, 100.));
        let node_positions = vec![
            Point2d::new(0., 0.),
            Point2d::new(0., 200.),
            Point2d::new(0., 10000.),
            Point2d::new(0., 10200.),
        ];
        return PathNetwork::from_graph(street_graph, node_positions, cfg);
    }

    #[test]
    fn test_fast_paths() {
        // a scenario where multiple routes are on the hyperpath, while others are not
        let mut street_graph = DiGraphMap::new();
        let aa = 0;
        let xx = 1;
        let yy = 2;
        let zz = 3;
        let bb = 4;
        street_graph.add_edge(aa, xx, make_pathsegment(0, 10. * 60.));
        street_graph.add_edge(xx, bb, make_pathsegment(1, 10. * 60.));
        street_graph.add_edge(aa, yy, make_pathsegment(2, 10. * 60.));
        street_graph.add_edge(yy, bb, make_pathsegment(3, 10. * 60.));
        street_graph.add_edge(aa, zz, make_pathsegment(4, 15. * 60.));
        street_graph.add_edge(zz, bb, make_pathsegment(5, 15. * 60.));
        street_graph.add_edge(bb, aa, make_pathsegment(6, 60.));

        let node_positions = vec![
            Point2d::new(0., 0.),          // a
            Point2d::new(5000., 50000.),   // x
            Point2d::new(0., 50000.),      // y
            Point2d::new(-5000., 50000.),  // z
            Point2d::new(0., 100000.),     // b
        ];

        let mut demand_graph = DiGraph::new();
        let demand_a = demand_graph.add_node(node_positions[0].clone());
        let demand_b = demand_graph.add_node(node_positions[4].clone());
        // all demand goes from a to b
        demand_graph.add_edge(demand_a, demand_b, 100.);

        let vehicle_type = PtVehicleType::new("1", "road", "a magic bus that needs no power", 100,
                                              0, 18., 0.);
        let routes = vec![
            vec![aa, xx, bb], 
            vec![aa, yy, bb],
            vec![aa, zz, bb], 
        ];
        let freqs = vec![
            1./ (6. * 60.),
            1./ (6. * 60.),
            1./ (6. * 60.),
        ];

        let cfg = Box::new(DEFAULT_CFG.clone());
        let cfg: &'static mut StaticSimConfig = Box::leak(cfg);
        cfg.transfer_penalty_m = 0.;
        cfg.mean_stop_time_s = 0.;

        let path_network = PathNetwork::from_graph(street_graph, node_positions, cfg);
        let transfer_graph = build_transfer_graph(&path_network);
        let basin_connections = build_basin_conns(&path_network, &demand_graph, cfg);
        let mut sim = StaticSimulator {
            path_network,
            demand_graph,
            transfer_graph,
            basin_connections,
            cfg,
            rng: Isaac64Rng::seed_from_u64(RAND_SEED),
        };

        // iterate and check results
        let mut used_routes = HashSet::new();
        let veh_types = vec![vehicle_type.clone(), vehicle_type.clone(), vehicle_type];
        for _ in 0..100 {
            println!("running...");
            let result = sim.run(&routes, &freqs, &veh_types, true);
            for (ii, stop_satdems) in result.per_stop_satisfied_demand.iter().enumerate() {
                if stop_satdems.sum() > 0. {
                    used_routes.insert(ii);
                }
            }
        }
        // routes 0 and 1
        let expected_used_routes: HashSet<usize> = [0, 1].iter().cloned().collect();
        assert_eq!(used_routes, expected_used_routes);
    }

    #[test]
    fn test_static_sim_complexnetwork() -> Result<(), std::io::Error> {
        let mut street_graph = DiGraphMap::new();
        let north = 0;
        let south = 1;
        let east = 2;
        let west = 3;
        let center = 4;
        let edge_time = 9. * 60. - 10.;
        let center_time = 6. * 60. - 10.;
        street_graph.add_edge(east, north, make_pathsegment(0, edge_time));
        street_graph.add_edge(north, east, make_pathsegment(1, edge_time));
        street_graph.add_edge(east, south, make_pathsegment(2, edge_time));
        street_graph.add_edge(south, east, make_pathsegment(3, edge_time));
        street_graph.add_edge(west, south, make_pathsegment(4, edge_time));
        street_graph.add_edge(south, west, make_pathsegment(5, edge_time));
        street_graph.add_edge(west, north, make_pathsegment(6, edge_time));
        street_graph.add_edge(north, west, make_pathsegment(7, edge_time));
        street_graph.add_edge(center, north, make_pathsegment(8, center_time));
        street_graph.add_edge(north, center, make_pathsegment(9, center_time));
        street_graph.add_edge(center, south, make_pathsegment(10, center_time));
        street_graph.add_edge(south, center, make_pathsegment(11, center_time));
        let node_positions = vec![
            Point2d::new(0., 5000.),
            Point2d::new(0., -5000.),
            Point2d::new(5000., 0.),
            Point2d::new(-5000., 0.),
            Point2d::new(0., 0.),
        ];

        //  # n,  s,  e,  w,  c
        // # 190,270,180,180, 75
        //  [[0, 100, 50, 40, 40], # n  230   260->300
        //   [80,  0, 40, 30, 10], # s  160    30->190
        //   [40, 50,  0, 70, 10], # e  170    10->180
        //   [20, 70, 60,  0, 15], # w  165     0->165
        //   [50, 50, 30, 40,  0], # c  170    90->260
        let mut demand_graph = DiGraph::new();
        let north_dn = demand_graph.add_node(node_positions[north].clone());
        let south_dn = demand_graph.add_node(node_positions[south].clone());
        let east_dn = demand_graph.add_node(node_positions[east].clone());
        let west_dn = demand_graph.add_node(node_positions[west].clone());
        let center_dn = demand_graph.add_node(node_positions[center].clone());

        demand_graph.add_edge(north_dn, south_dn, 100.);
        demand_graph.add_edge(north_dn, east_dn, 50.);
        demand_graph.add_edge(north_dn, west_dn, 40.);
        demand_graph.add_edge(north_dn, center_dn, 40.);

        demand_graph.add_edge(south_dn, north_dn, 80.);
        demand_graph.add_edge(south_dn, east_dn, 40.);
        demand_graph.add_edge(south_dn, west_dn, 30.);
        demand_graph.add_edge(south_dn, center_dn, 10.);

        demand_graph.add_edge(east_dn, north_dn, 40.);
        demand_graph.add_edge(east_dn, south_dn, 50.);
        demand_graph.add_edge(east_dn, west_dn, 70.);
        demand_graph.add_edge(east_dn, center_dn, 10.);

        demand_graph.add_edge(west_dn, north_dn, 20.);
        demand_graph.add_edge(west_dn, south_dn, 70.);
        demand_graph.add_edge(west_dn, east_dn, 60.);
        demand_graph.add_edge(west_dn, center_dn, 15.);

        demand_graph.add_edge(center_dn, north_dn, 50.);
        demand_graph.add_edge(center_dn, south_dn, 50.);
        demand_graph.add_edge(center_dn, east_dn, 30.);
        demand_graph.add_edge(center_dn, west_dn, 40.);
        let total_demand = get_total_demand(&demand_graph);

        let path_network = PathNetwork::from_graph(street_graph, node_positions, &DEFAULT_CFG);

        let veh_power = 26.7;
        let veh_type = PtVehicleType::new("1", "road", "a normal-sized bus", 50, 0, 18., veh_power);

        let transfer_graph = build_transfer_graph(&path_network);
        validate_transfer_graph(&path_network, &transfer_graph, &DEFAULT_CFG);
        let basin_connections = build_basin_conns(&path_network, &demand_graph, &DEFAULT_CFG);
        validate_basin_conns(&path_network, &demand_graph, &basin_connections, &DEFAULT_CFG);

        let mut sim = StaticSimulator {
            path_network,
            demand_graph,
            transfer_graph,
            basin_connections,
            cfg: &DEFAULT_CFG,
            rng: Isaac64Rng::seed_from_u64(RAND_SEED),
        };

        let routes = vec![
            // runs along the northern side: west, north, east, north, west
            vec![3, 0, 2, 0, 3],
            // runs along the southern side: west, south, east, south, west
            vec![3, 1, 2, 1, 3],
            // runs up and down the middle: north, center, south, center, north
            vec![0, 4, 1, 4, 0],
            ];
        let n_vehicles = 10.;
        let freqs = vec![n_vehicles * 0.4 / 2400., n_vehicles * 0.4 / 2400., 
                         n_vehicles * 0.2 / 1680.];
        let veh_types = vec![veh_type.clone(), veh_type.clone(), veh_type];
        let result = sim.run(&routes, &freqs, &veh_types, false);
        assert_ulps_eq!(result.satisfied_demand, total_demand);
        assert_ulps_eq!(result.power_used_kW, n_vehicles * veh_power);
        // check that the per-stop values add up to the total value
        check_per_stop_sums(&result);

        let n_vehicles = 4.;
        let freqs = vec![n_vehicles * 0.4 / 2400., n_vehicles * 0.4 / 2400., 
                         n_vehicles * 0.2 / 1680.];
        // let freqs = vec![n_vehicles * 0.4 / 3600., n_vehicles * 0.4 / 3600., 
        //                  n_vehicles * 0.2 / 2520.];
        for _ in 0..100 {
            let result = sim.run(&routes, &freqs, &veh_types, false);
            assert_ulps_eq!(result.power_used_kW, n_vehicles * veh_power);
            // print values here, since assert! doesn't show the arguments if it fails
            println!("satisfied demand is {} but should be < {} and > than that / 2", 
                     result.satisfied_demand, total_demand);
            assert!(result.satisfied_demand < total_demand);
            assert!(result.satisfied_demand > total_demand / 2.);
        }

        // test fast paths
        for _ in 0..100 {
            let result = sim.run(&routes, &freqs, &veh_types, true);
            assert_ulps_eq!(result.power_used_kW, n_vehicles * veh_power);
            // print values here, since assert! doesn't show the arguments if it fails
            println!("satisfied demand is {} but should be < {} and > than that / 2", 
                     result.satisfied_demand, total_demand);
            assert!(result.satisfied_demand <= total_demand);
            assert!(result.satisfied_demand > total_demand / 2.);
        }

        return Ok(());
    }

    fn make_pathsegment(idx: usize, time_s: f64) -> PathSegment {
        let drive_speed = 15.;
        return PathSegment::new(format!("{}", idx), vec![String::from("road")], 1, 1000, 
                                drive_speed, time_s * drive_speed);
    }

    fn make_simple_simenv(demand: f64, cfg: &'static StaticSimConfig)
                          -> Result<(StaticSimulator, PtVehicleType), std::io::Error> {
        return make_simple_simenv_with_offsets(demand, cfg, None, None);
    }

    fn make_simple_simenv_with_offsets(demand: f64, cfg: &'static StaticSimConfig, 
                                       src_offset: Option<Point2d>, dst_offset: Option<Point2d>)
                                       -> Result<(StaticSimulator, PtVehicleType), std::io::Error> {
        let mut street_graph = DiGraphMap::new();
        let aa = street_graph.add_node(0);
        let bb = street_graph.add_node(1);
        street_graph.add_edge(aa, bb, make_pathsegment(0, 240.));
        street_graph.add_edge(bb, aa, make_pathsegment(1, 240.));
        let node_positions = vec![Point2d::new(0., 0.), Point2d::new(10000., 10000.)];

        let mut demand_graph: DiGraph<Point2d, f64> = DiGraph::new();
        let node0pos = match src_offset {
            Some(offset) => node_positions[0].plus(&offset),
            None => node_positions[0].clone(),
        };
        let node1pos = match dst_offset {
            Some(offset) => node_positions[1].plus(&offset),
            None => node_positions[1].clone(),
        };
        let node0 = demand_graph.add_node(node0pos);
        let node1 = demand_graph.add_node(node1pos);
        demand_graph.add_edge(node0, node1, demand);

        let path_network = PathNetwork::from_graph(street_graph, node_positions, cfg);
        let transfer_graph = build_transfer_graph(&path_network);
        validate_transfer_graph(&path_network, &transfer_graph, cfg);
        let basin_connections = build_basin_conns(&path_network, &demand_graph, cfg);
        println!("basin graph: {:?}", basin_connections);
        validate_basin_conns(&path_network, &demand_graph, &basin_connections, cfg);

        let veh_power = 26.7;
        let vehicle_type = PtVehicleType::new("1", "road", "a normal-sized bus", 70, 30, 18., 
                                              veh_power);

        let sim = StaticSimulator {
            path_network,
            demand_graph,
            transfer_graph,
            basin_connections,
            cfg,
            rng: Isaac64Rng::seed_from_u64(RAND_SEED),
        };
        return Ok((sim, vehicle_type));
    }
    
    #[test]
    fn test_generate_grid_city() {
        let mut rng = Isaac64Rng::seed_from_u64(0);
        let num_travellers = 1000;
        let num_demand_edges = 11;
        let num_demand_loci = 5;
        let (path_network, demand_graph) = generate_grid_city(4, 5, 1000., num_travellers, 
                                                              num_demand_loci, num_demand_edges,
                                                              15.0, &DEFAULT_CFG, &mut rng);
        assert_eq!(path_network.get_num_nodes(), 20);
        assert_eq!(demand_graph.node_count(), 5);
        assert_eq!(demand_graph.edge_count(), num_demand_edges);
        let total_demand = get_total_demand(&demand_graph);
        assert_ulps_eq!(total_demand, num_travellers as f64, epsilon=0.00001);
    }

    fn validate_transfer_graph(path_network: &PathNetwork<StaticSimConfig>, 
                               transfer_graph: &DiGraph<Node, RoutingGraphEdge>,
                               cfg: &StaticSimConfig) {
        let node_positions = path_network.get_node_positions();
        for (ii, npi) in node_positions.iter().enumerate() {
            for (jj, npj) in node_positions.iter().enumerate() {
                let has_transfer_edge = transfer_graph.contains_edge(NodeIndex::new(ii), 
                NodeIndex::new(jj));
                let dist = npi.euclidean_distance(&npj);
                assert_eq!(has_transfer_edge, ii != jj && dist < cfg.transfer_radius_m);    
            }
        }
    }

    fn validate_basin_conns(path_network: &PathNetwork<StaticSimConfig>,
                            demand_graph: &DiGraph<Point2d, f64>,
                            basin_conns: &BasinConnections, cfg: &StaticSimConfig) {
        for (si, sp) in path_network.get_node_positions().iter().enumerate() {
            for (di, dp) in demand_graph.node_weights().enumerate() {
                let sni = NodeIndex::new(si);
                let dni = NodeIndex::new(di);
                let has_basin_edge = if basin_conns.contains_key(&dni) {
                    let dconns = basin_conns.get(&dni).unwrap();
                    dconns.contains_key(&sni)
                } else {
                    false
                };
                let dist = sp.euclidean_distance(dp);
                assert_eq!(has_basin_edge, dist <= cfg.basin_radius_m);
            }
        }
    }

    fn check_per_stop_sums(result: &SimResults) {
        // check satisfied demands sum correctly
        let stop_demand_sum: f64 = result.per_stop_satisfied_demand.iter().
            fold(0., |sum, psq| sum + psq.sum());
        assert_ulps_eq!(stop_demand_sum, result.satisfied_demand);
        // check boarders sum correctly
        let boarders_sum: f64 = result.stop_n_boarders.iter().
            fold(0., |sum, psq| sum + psq.sum());
        assert!(boarders_sum >= result.satisfied_demand);
        // check boarders and disembarkers sum to the same for each route
        for (route_stop_boarders, route_stop_disembarkers) in result.stop_n_boarders.iter().zip(
                                                              &result.stop_n_disembarkers) {
            assert_ulps_eq!(route_stop_boarders.sum(), route_stop_disembarkers.sum());
        }
        // check per-stop powers sum correctly
        let stop_power_sum: f64 = result.per_stop_power_used_kW.iter().
            fold(0., |sum, psq| sum + psq.sum());
        assert_ulps_eq!(stop_power_sum, result.power_used_kW);
    }

    fn get_total_demand(demand_graph: &DiGraph<Point2d, f64>) -> f64 {
        return demand_graph.edge_references().map(|edge_ref| *edge_ref.weight()).sum();
    }

    #[test]
    fn test_od_to_demand_graph_simple() {
        // test a collection with no overlaps
        let origins = vec![Point2d::new(0.0, 0.0), 
                           Point2d::new(300., 30138.3), 
                           Point2d::new(35000., 8.),
                           Point2d::new(0.0, 50.3)]; // last one collides on just one dimension
        let dests = vec![Point2d::new(1000., 1563.),
                         Point2d::new(8924., 9977.),
                         Point2d::new(1238., 4489.),
                         Point2d::new(832., 4489.)]; // last one collides on just one dimension
        let expfacs = vec![12., 5.4, 2.6, 99.8];
        let mut trips = vec![];
        for (ii, op) in origins.iter().enumerate() {
            let id = format!("{}", ii);
            let dp = &dests[ii];
            let ef = expfacs[ii];
            trips.push(PassengerTrip::new(&id, ii as u32, 0, op.clone(), dp.clone(), 0, ef));
        }
        let demand_graph = od_to_demand_graph(trips);

        // check number and poses of nodes
        assert_eq!(demand_graph.node_count(), origins.len() * 2);
        let nodes: Vec<&Point2d> = demand_graph.node_weights().collect();
        for node in &origins {
            assert!(nodes.contains(&node));
        }
        for node in &dests {
            assert!(nodes.contains(&node));
        }
        // check number of weights of edges
        assert_eq!(demand_graph.edge_count(), expfacs.len());
        for edge_ref in demand_graph.edge_references() {
            let orig_pos = demand_graph.node_weight(edge_ref.source()).unwrap();
            let orig_idx = origins.iter().position(|pp| *orig_pos == *pp).unwrap();

            // check destination is right
            let dest_pos = demand_graph.node_weight(edge_ref.target()).unwrap();
            assert_eq!(dests[orig_idx], *dest_pos);

            // check expansion factor is right
            assert_eq!(expfacs[orig_idx], *edge_ref.weight());
        }
    }

    #[test]
    fn test_od_to_demand_graph_empty() {
        let demand_graph = od_to_demand_graph(vec![]);
        assert_eq!(demand_graph.node_count(), 0);
        assert_eq!(demand_graph.edge_count(), 0);
    }

    #[test]
    fn test_od_to_demand_graph_overlaps() {
        // sources overlap exactly
        // sources round to the same
        // dests overlap exactly
        // dests round to the same
        // source overlaps dest
        let origins = vec![Point2d::new(0.0, 0.0), // overlaps with 2nd and last origins
                           Point2d::new(0.0, 0.0), 
                           Point2d::new(35000.7, 8.3), // overlaps with 4th origin
                           Point2d::new(35000.4, 8.8),
                           Point2d::new(-5020.5, 5020.6), // overlaps with 1st dest
                           Point2d::new(-100., -100.), // overlaps with last trip
                           Point2d::new(-100., -100.),
                          ];
        let dests = vec![Point2d::new(-5020.4, 5020.5),
                         Point2d::new(1000.1, -1563.25), // overlaps with 4th and last dests
                         Point2d::new(8924., 9977.), // overlaps with 5th dest
                         Point2d::new(1000.99, -1563.76),
                         Point2d::new(8924., 9977.),
                         Point2d::new(100., 5000.), // overlaps with last trip
                         Point2d::new(100., 5000.),
                         ];
        let expfacs = vec![8.0, 12., 5.4, 2.6, 100., 3.33, 7.8];
        let mut trips = vec![];
        for (ii, op) in origins.iter().enumerate() {
            let id = format!("{}", ii);
            let dp = &dests[ii];
            let ef = expfacs[ii];
            trips.push(PassengerTrip::new(&id, ii as u32, 0, op.clone(), dp.clone(), 0, ef));
        }
        let demand_graph = od_to_demand_graph(trips);

        // check number and poses of nodes
        // all origins overlap with some dest and vice versa, so count should be number of trips
        assert_eq!(demand_graph.node_count(), 7);
        let nodes: Vec<&Point2d> = demand_graph.node_weights().collect();
        let true_nodes = [&origins[0], &dests[0], &dests[1], &origins[2], &dests[2], &origins[5],
                          &dests[5]];
        for true_node in &true_nodes {
            if ! nodes.contains(true_node) {
                panic!("True node {:?} not in {:?}", true_node, nodes);
            }
        }

        // check number of weights of edges
        assert_eq!(demand_graph.edge_count(), 6);
        let zipped_locs: Vec<((i64, i64), (i64, i64))> = origins.iter().zip(dests).
            map(|(op, dp)| (point2d_to_ints(op), point2d_to_ints(&dp))).collect();
        for edge_ref in demand_graph.edge_references() {
            let orig_pos = demand_graph.node_weight(edge_ref.source()).unwrap();
            let dest_pos = demand_graph.node_weight(edge_ref.target()).unwrap();

            let matchable = (point2d_to_ints(orig_pos), point2d_to_ints(dest_pos));
            let orig_idx = zipped_locs.iter().position(|pp| matchable == *pp).unwrap();

            if orig_idx < 5 {
                // check expansion factor is right
                assert_eq!(expfacs[orig_idx], *edge_ref.weight());
            } else {
                // the last two trips should get added together into one edge
                assert_eq!(expfacs[5] + expfacs[6], *edge_ref.weight());
            }
        }
    }
}