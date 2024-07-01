// this file defines a struct to represent a network of streets, railways, and other paths for an area.
// It's going to be a wrapper around a petgraph graph.
use std::error::Error;
use std::path::Path;
use std::fmt::Debug;
use std::collections::HashMap;
use std::collections::HashSet;

use ndarray::prelude::*;
use petgraph::graphmap::DiGraphMap;
use petgraph::algo::dijkstra;
use petgraph::algo::kosaraju_scc;
use petgraph::visit::IntoNodeIdentifiers;
use xml::reader::XmlEvent;
use rayon::prelude::*;

use super::config_utils;
use super::geometry::Point2d;
use super::geometry::LineSegment;
use super::geometry::euclidean_distances;
use super::pt_system::StopFacility;
use super::SimConfig;


static ALLOWED_HWY_TYPES: [&str; 11] = [
    "primary", 
    "primary_link", 
    "secondary", 
    "secondary_link", 
    "tertiary",
    "tertiary_link", 
    "trunk", 
    "trunk_link", 
    "motorway", 
    "motorway_link",
    // "residential", 
    "service"
    ];

static TRACKED_MODES: [&str; 3] = ["road", "metro", "rail"];


fn relative_eq(aa: f64, bb: f64) -> bool {
    let absdiff = (aa - bb).abs();
    return absdiff < 0.0001;
}

#[derive(PartialEq, Debug)]
pub struct PathSegment {
    id: String,
    modes: Vec<String>,
    permlanes: u8,
    capacity: u32,
    freespeed: f64,
    length: f64,
}

impl PathSegment {
    pub fn new(id: String, modes: Vec<String>, permlanes: u8, capacity: u32, freespeed: f64,
               length: f64) -> PathSegment {
        return PathSegment {id, modes, permlanes, capacity, freespeed, length};
    }
}

pub struct PathNetwork<'a, CC> {
    // a directed graph to hold the actual road network.  Node type is a 
    // String id, locations 
    node_positions: Vec<Point2d>,
    node_idxs_by_id: HashMap<String, usize>,
    node_ids_by_idx: Vec<String>,
    network: DiGraphMap<usize, PathSegment>,
    edges: HashMap<String, (usize, usize)>,
    // estimated times, in seconds, it takes to drive between every two nodes.
    drive_times: HashMap<String, Array<f64, Ix2>>,
    drive_dists: HashMap<String, Array<f64, Ix2>>,
    // estimated times, in seconds, it takes to walk between every two nodes.
    walk_times: Array<f64, Ix2>,
    // whether each pair of nodes are within the basin radius of each other.
    walk_dists: Array<f64, Ix2>,
    cfg: &'a CC,
}

impl<'a, CC> PathNetwork<'a, CC> where CC: SimConfig {
    pub fn from_xml(xml_path: &Path, cfg: &'a CC) 
                    -> Result<PathNetwork<'a, CC>, Box<dyn Error>> {
        let mut edges_vec = vec![];
        // assemble the DiGraphMap from the xml
        
        let mut parser = config_utils::xml_parser_from_path(xml_path);
        let mut connected_nodes = HashSet::new();
        let mut pos_by_id = HashMap::new();
        let pt_modes = vec!["pt", "bus"];
        let road_mode_string = String::from("road");
        while let ee = parser.next() {
            if let Err(err) = ee {
                log::error!("Error: {}", err);
                break;
            } else if let Ok(XmlEvent::EndDocument) = ee {
                log::info!("Reached end of network xml");
                break;
            }
            
            let ee = ee.unwrap();
            if let XmlEvent::StartElement{ name, attributes, .. } = &ee {
                if name.local_name == "node" {
                    let id = config_utils::get_xml_attribute_value(&attributes, "id").unwrap();
                    let xpos = config_utils::get_xml_attribute_value(&attributes, "x").
                        unwrap().parse()?;
                    let ypos = config_utils::get_xml_attribute_value(&attributes, "y").
                        unwrap().parse()?;
                    let pos = Point2d::new(xpos, ypos);
                    pos_by_id.insert(id, pos);
                } else if name.local_name == "link" {
                    let to_id = config_utils::get_xml_attribute_value(&attributes, "to").unwrap();
                    let from_id = config_utils::get_xml_attribute_value(&attributes, "from").
                        unwrap();
                    if to_id == from_id {
                        // ignore self-connections
                        continue;
                    }
                    let id = config_utils::get_xml_attribute_value(&attributes, "id").unwrap();
                    let modes = config_utils::get_xml_attribute_value(&attributes, "modes").
                        unwrap();
                    let mut modes: Vec<String> = modes.split(',').map(|ss| String::from(ss)).
                                                        collect();
                    let permlanes = config_utils::get_xml_attribute_value(&attributes, 
                                                                          "permlanes").
                                                                          unwrap().parse::<f64>()?;
                    let permlanes = permlanes as u8;
                    let capacity = config_utils::get_xml_attribute_value(&attributes,
                                                                         "capacity")
                                                                         .unwrap().parse::<f64>()?;
                    let capacity = capacity as u32;
                    let freespeed = config_utils::get_xml_attribute_value(&attributes,
                                                                          "freespeed").unwrap();
                    let freespeed = if freespeed == "Infinity" {
                        100.0
                    } else {
                        freespeed.parse()?
                    };

                    let length = config_utils::get_xml_attribute_value(&attributes, "length").
                        unwrap().parse()?;

                    // parse attributes
                    let mut cur_attr_name: Option<String> = None;
                    let mut link_attrs = HashMap::new();
                    while let subelem = parser.next() {
                        let subelem = subelem.unwrap();
                        match &subelem {
                            // we're done parsing the link's sub-elements
                            XmlEvent::EndElement{ name } if name.local_name == "link" => break,
                            XmlEvent::StartElement{ name, attributes, .. } 
                            if name.local_name == "attribute" => {
                                // record the name of the attribute
                                let can = config_utils::get_xml_attribute_value(&attributes, "name")
                                                        .unwrap();
                                cur_attr_name = Some(can.clone());
                            }
                            XmlEvent::Characters(content) => {
                                if let Some(can) = cur_attr_name {                                
                                    // add the attribute's name and value to the link attribute dict
                                    link_attrs.insert(can, content.clone());
                                    // we're done with this one, so reset the attribute name to None
                                    cur_attr_name = None;
                                }
                            }
                            _ => (),
                        }
                    }

                    // if the highway tag is one of the allowed types, include it
                    // if it's "unclassified" and has either 'bus' in its modes or a 'bus' 
                    // relation, include it
                    let include_link = match link_attrs.get("osm:way:highway") {
                        Some(hwytype) => {
                            let include = if *hwytype == "unclassified" || 
                                             *hwytype == "residential" {
                                // check if link has a bus relation
                                let has_bus_relation = match link_attrs.get("osm:relation:route") {
                                    Some(relations) => {
                                        match relations.split(',').find(|&xx| xx == "bus") {
                                            Some(_) => true,
                                            None => false,
                                        }
                                    }
                                    None => false,
                                };
                                if *hwytype == "unclassified" {
                                    let has_bus_mode = modes.iter().
                                                            any(|m1| 
                                                                pt_modes.iter().any(|m2| m1 == m2));
                                    has_bus_relation || has_bus_mode
                                } else {
                                    has_bus_relation
                                }
                            } else {
                                // check if it's in allowed types
                                match ALLOWED_HWY_TYPES.iter().find(|&xx| xx == hwytype) {
                                    Some(_) => true,
                                    None => false,
                                }
                            };
                            if include && ! modes.contains(&road_mode_string) {
                                // we're adding this as a bus link, so make sure it has bus mode
                                modes.push(road_mode_string.clone());
                            }
                            include
                        }

                        None => {
                            // no highway tag was specified...wierd, but include it anyway.
                            log::warn!("No highway tag specified for link ({}, {})", 
                                       from_id, to_id);
                            true
                        }
                    };

                    if include_link {
                        let path_seg = PathSegment{id, modes, permlanes, capacity, freespeed, 
                                                   length};
                        connected_nodes.insert(from_id.clone());
                        connected_nodes.insert(to_id.clone());
                        edges_vec.push((from_id, to_id, path_seg));    
                    }
                }
            }
        }
        
        // first, build a dummy graph that we'll use to check for components to discard.
        let mut graph = DiGraphMap::new();
        for (from_id, to_id, _) in &edges_vec {
            graph.add_edge(from_id, to_id, 0);
        }

        // check for components to discard.
        let comps = kosaraju_scc(&graph);
        log::warn!("there are {} connected components", comps.len());  
        for comp in comps {
            if cfg.get_ignore_small_components() && comp.len() == 1 {
                // remove single nodes that couldn't be connected
                log::info!("removing {}", comp[0]);
                graph.remove_node(comp[0]);
            }
            else if comp.len() < 100 {
                // print some helpful diagnostic information about these components
                for elem in comp {
                    log::warn!("component has id: {:?}", elem);
                }
            }
        }

        // rebuild the graph without the ignored components
        let mut node_idxs_by_id = HashMap::new();
        let mut node_ids_by_idx = vec![];
        let mut node_positions = vec![];
        let old_node_ids: Vec<&String> = graph.node_identifiers().collect();
        let mut graph = DiGraphMap::new();
        for node_id in old_node_ids {
            let node_pos = &pos_by_id[node_id];
            node_positions.push(node_pos.clone());
            let new_idx = node_idxs_by_id.len();
            graph.add_node(new_idx);
            log::debug!("inserting {}, {}", node_id, new_idx);
            node_idxs_by_id.insert(node_id.clone(), new_idx);
            node_ids_by_idx.push(node_id.clone());
        }
        // then add all edges
        let mut edges = HashMap::new();
        for (from_id, to_id, path_seg) in edges_vec {
            if let Some(from_idx) = node_idxs_by_id.get(&from_id) {
                if let Some(to_idx) = node_idxs_by_id.get(&to_id) {
                    edges.insert(path_seg.id.clone(), (*from_idx, *to_idx));
                    log::debug!("adding edge {} ({}), {} ({})", from_id, *from_idx, to_id, *to_idx);
                    graph.add_edge(*from_idx, *to_idx, path_seg);
                } else {
                    log::info!("No idx for {}", to_id);
                }
            } else {
                log::info!("No idx for {}", from_id);                
            }
        }

        let (drive_times, drive_dists, walk_dists, walk_times) = 
            compute_matrices(&graph, &node_positions, cfg);

        Ok(PathNetwork {
            node_positions,
            node_idxs_by_id,
            node_ids_by_idx,
            network: graph,
            edges,
            drive_times,
            drive_dists,
            walk_times,
            walk_dists,
            cfg,
        })
    }

    pub fn from_graph(graph: DiGraphMap<usize, PathSegment>, node_positions: Vec<Point2d>, 
                      cfg: &'a CC) -> PathNetwork<CC> {
        // assume ids are just indexes
        let mut node_idxs_by_id = HashMap::new();
        let mut node_ids_by_idx = vec![];
        for node in graph.nodes() {
            let node_str = format!("{}", node).to_string();
            node_idxs_by_id.insert(node_str.clone(), node);
            node_ids_by_idx.push(node_str)
        }
        let mut edges = HashMap::new();
        for (from, to, path_seg) in graph.all_edges() {
            edges.insert(path_seg.id.clone(), (from, to));
        }

        let (drive_times, drive_dists, walk_dists, walk_times) = 
            compute_matrices(&graph, &node_positions, cfg);
        PathNetwork{
            node_positions,
            node_idxs_by_id,
            node_ids_by_idx,
            network: graph,
            edges,
            drive_times,
            drive_dists,
            walk_times,
            walk_dists,
            cfg,
        }
    }

    pub fn estimate_time_between_stops(&self, stopfac1: &StopFacility, 
                                       stopfac2: &StopFacility, mode: &str) -> Option<u32> {
        // get the pre-computed time between the node after the first stop and the
        // node before the second stop.
        if ! self.drive_times.contains_key(mode) {
            return None;
        }
        let drive_times = &self.drive_times[mode];
        let (from1, to1) = self.edges.get(&stopfac1.link).unwrap();
        if stopfac1.link == stopfac2.link {
            // they're on the same link!
            let end_from = &self.node_positions[*from1];
            let end_to = &self.node_positions[*to1];
            let seg = LineSegment::from_refs(end_from, end_to);
            let point1 = stopfac1.pos.nearest_point_on_line(&seg);
            let point2 = stopfac2.pos.nearest_point_on_line(&seg);
            if point1.euclidean_distance(end_from) <= point2.euclidean_distance(end_from) {
                // 1 is before (or at) 2 on the one-way street, so we can go right there.
                let pathseg = self.network.edge_weight(*from1, *to1).unwrap();
                let time = point1.euclidean_distance(&point2) / pathseg.freespeed;
                return Some(time.round() as u32);
            } // else, we'll have to find a path back around, which we handle in the usual way.
        }

        let (from2, _) = self.edges.get(&stopfac2.link).unwrap();
        let mut inter_node_time = 0.0;
        if to1 != from2 {
            inter_node_time = drive_times[[*to1, *from2]];
        }
        if inter_node_time == f64::INFINITY {
            return None;
        }

        // a utility function to get the time between a stop on a link and one of its ends.
        let time_on_stop_seg = |stopfac: &StopFacility, starting_at_stop: bool| {

            let (mut end_i, mut end_j) = self.edges.get(&stopfac.link).unwrap();
            let pathseg = self.network.edge_weight(end_i, end_j).unwrap();
            if ! starting_at_stop {
                // we're going from one end to the stop.  Handle this by reversing the ends.
                let tmp = end_i;
                end_i = end_j;
                end_j = tmp;
            }
            let seg_end = &self.node_positions[end_j];
            let seg = LineSegment::from_refs(&self.node_positions[end_i], seg_end);
            let segpoint = stopfac.pos.nearest_point_on_line(&seg);
            let dist_from_node = segpoint.euclidean_distance(&seg_end);
            dist_from_node / pathseg.freespeed
        };

        // the inter-node time will not count the time in the first node.
        let mits = self.cfg.get_mean_intersection_time_s() as f64;
        let seg1time = time_on_stop_seg(stopfac1, true) + mits;
        let seg2time = time_on_stop_seg(stopfac2, false);

        Some((inter_node_time + seg1time + seg2time).round() as u32)
    }

    pub fn get_node_idx_by_id(&self, id: &str) -> Option<usize> {
        match self.node_idxs_by_id.get(id) {
            Some(idx_ref) => Some(*idx_ref),
            None => None,
        }
    }

    pub fn get_node_id_by_idx(&self, idx: usize) -> Option<&str> {
        if idx >= self.node_ids_by_idx.len() {
            return None;
        } else {
            return Some(&self.node_ids_by_idx[idx]);
        }
    }

    pub fn get_node_positions(&self) -> &Vec<Point2d> {
        &self.node_positions
    }

    pub fn get_num_nodes(&self) -> usize {
        return self.network.node_count();
    }

    pub fn get_travel_time(&self, from_idx: usize, to_idx: usize, mode: &str) -> f64 {
        return self.drive_times[mode][[from_idx, to_idx]];
    }

    pub fn get_travel_times_matrix(&self, mode: &str) -> &Array<f64, Ix2> {
        return self.drive_times.get(mode).unwrap();
    }

    pub fn get_drive_dists_matrix(&self, mode: &str) -> &Array<f64, Ix2> {
        return self.drive_dists.get(mode).unwrap();
    }

    pub fn get_adjacencies(&self) -> Vec<(usize, usize)> {
        return self.network.all_edges().map(|(ff, tt, _)| (ff, tt)).collect();
    }

    pub fn get_walk_time(&self, from_idx: usize, to_idx: usize) -> f64 {
        return self.walk_times[[from_idx, to_idx]];
    }

    pub fn are_within_basin(&self, from_idx: usize, to_idx: usize) -> bool {
        return self.walk_dists[[from_idx, to_idx]] <= self.cfg.get_basin_radius_m();
    }

    pub fn can_transfer(&self, from_idx: usize, to_idx: usize) -> bool {
        return self.walk_dists[[from_idx, to_idx]] <= self.cfg.get_transfer_radius_m();
    }
}


/// returns drive_times, walk_times, board_dmbrks
fn compute_matrices<CC>(graph: &DiGraphMap<usize, PathSegment>, node_positions: &Vec<Point2d>,
                        cfg: &CC) 
                        -> (HashMap<String, Array<f64, Ix2>>, HashMap<String, Array<f64, Ix2>>,
                            Array<f64, Ix2>, Array<f64, Ix2>)
                        where CC: SimConfig {
    // run Dijsktra from all nodes to get drive times for each node pair
    let size = graph.node_count();
    let mut transit_times = HashMap::new();
    let mut transit_distances = HashMap::new();
    let node_list: Vec<usize> = graph.nodes().collect();

    for mode in TRACKED_MODES {
        // by default, all times are infinite; we assume no path exists
        let mode = String::from(mode);
        let mut mode_transit_times = Array::ones((size, size)) * f64::INFINITY;
        let mits = cfg.get_mean_intersection_time_s() as f64;
        let edge_cost = |(_, _, edge): (usize, usize, &PathSegment)| {
            if edge.modes.contains(&mode) {
                return edge.length / edge.freespeed + mits;
            } else {
                return f64::INFINITY;
            }
        };

        // run dijkstra in parallel
        let path_results: Vec<(usize, HashMap<usize, f64>)> = node_list.par_iter().
            map(|nn| (*nn, dijkstra(&graph, *nn, None, edge_cost))).collect();
        for (node, paths) in path_results {
            for (dest_node, time) in paths {
                if time == f64::INFINITY {
                    log::debug!("Time from {} to {} is infinite!", node, dest_node);
                }
                mode_transit_times[[node, dest_node]] = time;
            }
        }

        transit_times.insert(mode.clone(), mode_transit_times);

        // compute the inter-node on-street distances the same way
        let mut mode_street_distances = Array::ones((size, size)) * f64::INFINITY;
        let edge_cost = |(_, _, edge): (usize, usize, &PathSegment)| {
            return edge.length;
        };
        let path_results: Vec<(usize, HashMap<usize, f64>)> = node_list.par_iter().
            map(|nn| (*nn, dijkstra(&graph, *nn, None, edge_cost))).collect();
        for (node, paths) in path_results {
            for (dest_node, dist) in paths {
                if dist == f64::INFINITY {
                    println!("Dist from {} to {} is infinite!", node, dest_node);
                }
                mode_street_distances[[node, dest_node]] = dist;
            }
        }
        transit_distances.insert(mode, mode_street_distances);
    }

    // compute matrices related to walking
    let direct_distances = euclidean_distances(&node_positions, None);
    let walk_times = direct_distances.mapv(|xx| cfg.dist_to_walk_time(xx));
    return (transit_times, transit_distances, direct_distances, walk_times);
}


#[cfg(test)]
mod tests {

    use std::fs::File;
    use std::io::Write;
    use approx::assert_relative_eq;

    use tempfile::tempdir;
    
    use super::*;
    use super::super::pt_system::StopFacility;

    // create the test xml
    static TEST_NETWORK_XML: &str = r###"
    <?xml version="1.0" encoding="utf-8"?>
    <!DOCTYPE network SYSTEM "http://matsim.org/files/dtd/network_v1.dtd">
    <network>
        <nodes>
            <node x="0.0" y="0.0" id="1" />
            <node x="1000.0" y="0.0" id="2" />
            <node x="2000.0" y="0.0" id="3" />
            <node x="20000.0" y="0.0" id="4" />
            <node x="21000.0" y="0.0" id="5" />
            <node x="22000.0" y="0.0" id="6" />
        </nodes>
    
        <links capperiod="10:00:00">
            <link id="1-2" modes="road" permlanes="1" capacity="2000" freespeed="22" length="1000" to="2" from="1"/>
            <link id="2-3" modes="road,testmode" permlanes="1" capacity="5000" freespeed="22" length="1000" to="3" from="2"/>
            <link id="3-4" modes="road" permlanes="1" capacity="2000" freespeed="29" length="18000" to="4" from="3"/>
            <link id="4-5" modes="road" permlanes="1" capacity="2000" freespeed="22" length="1000" to="5" from="4"/>
            <link id="5-6" modes="road" permlanes="1" capacity="1" freespeed="22" length="1000" to="6" from="5"/>
            <link id="6-5" modes="road" permlanes="1" capacity="1" freespeed="22" length="1000" to="5" from="6"/>
        </links>
    </network>        
    "###;

    #[test]
    fn test_network_parsing() -> Result<(), std::io::Error> {
        let network = get_network(TEST_NETWORK_XML).unwrap();

        // compare the returned network to what the results should be
        // check the node positions
        let true_node_positions = vec![
            Point2d::new(0., 0.),
            Point2d::new(1000., 0.),
            Point2d::new(2000., 0.),
            Point2d::new(20000., 0.),
            Point2d::new(21000., 0.),
            Point2d::new(22000., 0.),
        ];
        assert_eq!(true_node_positions.len(), network.node_positions.len());

        for (pos, true_pos) in network.node_positions.iter().zip(true_node_positions) {
            assert_eq!(true_pos, *pos);
        }

        // check the graph edges
        let mut true_edges = HashMap::new();
        true_edges.insert((0, 1), PathSegment {
            id: String::from("1-2"),
            modes: vec![String::from("road")],
            permlanes: 1,
            capacity: 2000,
            freespeed: 22.0,
            length: 1000.0,
        });
        true_edges.insert((1, 2), PathSegment {
            id: String::from("2-3"),
            modes: vec![String::from("road"), String::from("testmode")],
            permlanes: 1,
            capacity: 5000,
            freespeed: 22.0,
            length: 1000.0,
        });
        true_edges.insert((2, 3), PathSegment {
            id: String::from("3-4"),
            modes: vec![String::from("road")],
            permlanes: 1,
            capacity: 2000,
            freespeed: 29.0,
            length: 18000.0,
        });
        true_edges.insert((3, 4), PathSegment {
            id: String::from("4-5"),
            modes: vec![String::from("road")],
            permlanes: 1,
            capacity: 2000,
            freespeed: 22.0,
            length: 1000.0,
        });
        true_edges.insert((4, 5), PathSegment {
            id: String::from("5-6"),
            modes: vec![String::from("road")],
            permlanes: 1,
            capacity: 1,
            freespeed: 22.0,
            length: 1000.0,
        });
        true_edges.insert((5, 4), PathSegment {
            id: String::from("6-5"),
            modes: vec![String::from("road")],
            permlanes: 1,
            capacity: 1,
            freespeed: 22.0,
            length: 1000.0,
        });

        assert_eq!(network.network.edge_count(), true_edges.len());
        for (from, to, edge) in network.network.all_edges() {
            let true_edge = true_edges.get(&(from, to)).unwrap();
            assert_eq!(edge, true_edge);
            let (mapfrom, mapto) = network.edges.get(&true_edge.id).unwrap();
            assert_eq!(*mapfrom, from);
            assert_eq!(*mapto, to);
        }

        // check the node times
        let mut true_times = Array::ones((6, 6)) * f64::INFINITY;
        true_times[[0, 1]] = 50.454545;
        true_times[[0, 2]] = 100.90909;
        true_times[[0, 3]] = 726.59875;
        true_times[[0, 4]] = 777.0533;
        true_times[[0, 5]] = 827.5078;

        true_times[[1, 2]] = 50.454545;
        true_times[[1, 3]] = 676.14417;
        true_times[[1, 4]] = 726.5987;
        true_times[[1, 5]] = 777.0532;

        true_times[[2, 3]] = 625.68964;
        true_times[[2, 4]] = 676.14417;
        true_times[[2, 5]] = 726.5987;

        true_times[[3, 4]] = 50.454545;
        true_times[[3, 5]] = 100.90909;

        true_times[[4, 5]] = 50.454545;

        true_times[[5, 4]] = 50.454545;

        true_times[[0, 0]] = 0.;
        true_times[[1, 1]] = 0.;
        true_times[[2, 2]] = 0.;
        true_times[[3, 3]] = 0.;
        true_times[[4, 4]] = 0.;
        true_times[[5, 5]] = 0.;
    
        for ((ii, jj), val) in network.drive_times["road"].indexed_iter() {
            assert_relative_eq!(*val, true_times[[ii, jj]], epsilon=0.0001);
        } 
        Ok(())
    }

    #[test]
    fn test_est_time_between_stops() {
        let network = get_network(TEST_NETWORK_XML).unwrap();
        // define some stop facilities whose inter-stop distance we want to test
        let sf12_1 = StopFacility {
            id: String::from("1-2_1"), 
            link: String::from("1-2"), 
            pos: Point2d::new(10.0, -10.0),
        };
        // time from sf12_1 should be 980 / 22
        let sf12_2 = StopFacility {
            id: String::from("1-2_2"),
            link: String::from("1-2"), 
            pos: Point2d::new(990.0, 10.0),
        };
        // test stops on the same link
        let time = network.estimate_time_between_stops(&sf12_1, &sf12_2, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        let true_time: f64 = 980.0 / 22.0;
        assert_eq!(time, true_time.round() as u32);
        // stops on the same link with no path between them
        let time = network.estimate_time_between_stops(&sf12_2, &sf12_1, "road");
        assert!(time.is_none());

        // this one should have the same distance from any other as sf12_2
        let sf12_3 = StopFacility {
            id: String::from("1-2_3"), 
            link: String::from("1-2"), 
            pos: Point2d::new(990.0, -100.0),
        };
        let time = network.estimate_time_between_stops(&sf12_1, &sf12_3, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        let true_time: f64 = 980.0 / 22.0;
        assert_eq!(time, true_time.round() as u32);

        let time = network.estimate_time_between_stops(&sf12_2, &sf12_3, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        assert_eq!(time, 0);

        let sf23_1 = StopFacility {
            id: String::from("2-3_1"), 
            link: String::from("2-3"), 
            pos: Point2d::new(1010.0, 100.0),
        };
        // test stops on adjacent links
        let time = network.estimate_time_between_stops(&sf12_1, &sf23_1, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        let true_time = network.cfg.mean_intersection_time_s as f64 + (1000.0 / 22.0);
        assert_eq!(time, true_time.round() as u32);

        let sf34_1 = StopFacility {
            id: String::from("3-4_1"), 
            link: String::from("3-4"), 
            pos: Point2d::new(2010.0, 50.0),
        };
        // stops on non-adjacent links
        let time = network.estimate_time_between_stops(&sf12_1, &sf34_1, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        let true_time: f64 = network.cfg.mean_intersection_time_s as f64 * 2.0 + 
                         (1990.0 / 22.0) + (10.0 / 29.0);
        assert_eq!(time, true_time.round() as u32);
        // stops on different links with no path between them
        assert!(network.estimate_time_between_stops(&sf34_1, &sf12_1, "road").is_none());

        // stops on the same link, B before A, but with another path from A to B
        let sf56_1 = StopFacility {
            id: String::from("5-6_1"),
            link: String::from("5-6"), 
            pos: Point2d::new(21010.0, 30.0),
        };
        let sf56_2 = StopFacility {
            id: String::from("5-6_2"),
            link: String::from("5-6"), 
            pos: Point2d::new(21990.0, 30.0),
        };

        // stops separated by many links

        let time = network.estimate_time_between_stops(&sf12_1, &sf56_2, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        let true_time: f64 = (3980.0 / 22.0) + (18000.0 / 29.0) + 20.0;
        assert_eq!(time, true_time.round() as u32);
 
        // stops on the same link
        let time = network.estimate_time_between_stops(&sf56_1, &sf56_2, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        let true_time: f64 = 980.0 / 22.0;
        assert_eq!(time, true_time.round() as u32);

        let time = network.estimate_time_between_stops(&sf56_2, &sf56_1, "road");
        assert!(time.is_some());
        let time = time.unwrap();
        let true_time = network.cfg.mean_intersection_time_s as f64 * 2.0 + (1020.0 / 22.0);
        assert_eq!(time, true_time.round() as u32);
    }

    struct DummyConfig {
        basin_radius_m: f64,
        delay_tolerance_s: u32,
        initial_wait_tolerance_s: u32,
        transfer_time_s: u32,
        transfer_radius_m: f64,
        beeline_dist_factor: f64,
        walk_speed_mps: f64,
        mean_intersection_time_s: u32,
    }

    impl SimConfig for DummyConfig {
        fn get_basin_radius_m(&self) -> f64 {
            return self.basin_radius_m;
        }

        fn get_transfer_time_s(&self) -> u32 {
            return self.transfer_time_s;
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
            return self.mean_intersection_time_s;
        }

        fn get_ignore_small_components(&self) -> bool {
            return false;
        }
    }

    fn get_network(test_xml: &str) 
                   -> Result<PathNetwork<DummyConfig>, std::io::Error> {
        let dir = tempdir()?;
        let file_path = dir.path().join("network.xml");
        let mut file = File::create(&file_path)?;
        file.write_all(test_xml.as_bytes())?;

        let cfg = Box::new(DummyConfig{
            basin_radius_m: 500.,
            delay_tolerance_s: 1800,
            initial_wait_tolerance_s: 300,
            transfer_time_s: 600,
            transfer_radius_m: 100.,
            beeline_dist_factor: 1.3,
            walk_speed_mps: 1.35,
            mean_intersection_time_s: 5,
        });

        return Ok(PathNetwork::from_xml(&file_path, Box::leak(cfg)).unwrap());
    }

    #[test]
    fn test_constructor_basic() {
        let network = get_network(TEST_NETWORK_XML).unwrap();
        test_path_network_constructor(&network);
    }

    // TODO other constructor tests...

    fn test_path_network_constructor<CC>(pn: &PathNetwork<CC>) where CC: SimConfig {
        // test the walk times, transfer graph, and board-disembark validity are set correctly
        let dist_to_time_const = pn.cfg.get_beeline_dist_factor() / pn.cfg.get_walk_speed_mps();
        let node_positions = pn.get_node_positions();
        for (ii, npi) in node_positions.iter().enumerate() {
            for (jj, npj) in node_positions.iter().enumerate() {
                let dist = npi.euclidean_distance(&npj);
                let true_walk_time = dist * dist_to_time_const;
                assert_relative_eq!(pn.get_walk_time(ii, jj), true_walk_time);

                assert_eq!(pn.are_within_basin(ii, jj), dist < pn.cfg.get_basin_radius_m());
            }
        }
    }
}