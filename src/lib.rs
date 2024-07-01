// standard library imports
use itertools::iproduct;
use rand::seq::SliceRandom;
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::path::{Path, PathBuf};

// non-standard crate imports
use kdtree::distance::squared_euclidean;
use kdtree::KdTree;
use rayon::prelude::*;
use yaml_rust::YamlLoader;
use ndarray::prelude::*;

extern crate blas_src;

// imports of other modules from this crate
mod pt_system;
pub use pt_system::{PtSystem, PtVehicle, PtVehicleType, VehicleState};

mod passengers;
pub use passengers::{JourneyLeg, PassengerTrip, PtLeg, WalkingLeg};

mod geometry;
pub use geometry::Point2d;

mod path_network;
use path_network::PathNetwork;

mod config_utils;

mod static_sim;
pub use static_sim::StaticSimulator;

mod dynamic_sim;
use dynamic_sim::Connection;
pub use dynamic_sim::DynamicSimulator;

mod my_dijkstra;
use my_dijkstra::bkwds_dijkstra_with_paths;

#[cfg(test)]
mod test_utils;


/// Defines common elements for all simulator config.
pub trait SimConfig {
    fn get_basin_radius_m(&self) -> f64;
    fn get_transfer_time_s(&self) -> u32;
    fn get_transfer_radius_m(&self) -> f64;
    fn get_beeline_dist_factor(&self) -> f64;
    fn get_walk_speed_mps(&self) -> f64;
    fn get_mean_intersection_time_s(&self) -> u32;
    fn get_ignore_small_components(&self) -> bool;
    fn dist_to_walk_time(&self, distance: f64) -> f64 {
        return distance * self.get_beeline_dist_factor() / self.get_walk_speed_mps();
    }

    fn get_walk_time(&self, pos1: &Point2d, pos2: &Point2d) -> f64 {
        let distance = pos1.euclidean_distance(pos2);
        return self.dist_to_walk_time(distance);
    }
}


